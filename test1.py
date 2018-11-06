#!/usr/bin/env python3

import asyncio
import logging
import signal

LOG = logging.getLogger()

# Simulate project IDs we might get from rabbit
FAKE_EVENTS = [
    '5e6e2d10-4c8e-4a78-8433-48754407dcd0',
    '5d65eb04-5661-4e8d-9b95-e195505eca67',
    '5edcc66b-131d-47ad-a53a-b7071b1c0eb5',
    '5d65eb04-5661-4e8d-9b95-e195505eca67',
    '5e6e2d10-4c8e-4a78-8433-48754407dcd0',
    'f1e716dc-9c47-4607-99fd-6aad3c597b69',
    'b2ac5681-95df-413e-af1c-74c0d70c3f12',
    'f1e716dc-9c47-4607-99fd-6aad3c597b69',
    'b2ac5681-95df-413e-af1c-74c0d70c3f12',
]

# This should come from a configuration option
NUM_CONCURRENT = 2


async def error_handler(coroutine, name):
    log = logging.getLogger('error_handler.{}'.format(name))
    try:
        await coroutine
    except Exception as err:
        log.traceback(err)


class Producer:

    _log = logging.getLogger('Producer')

    def __init__(self, q):
        self._q = q
        event_loop = asyncio.get_event_loop()
        event_loop.add_signal_handler(signal.SIGINT, self._stop)
        self._run = True

    def _sigint(self):
        self._log.info('sigint')
        self._stop()

    def _stop(self):
        self._log.info('stopping')
        self._run = False

    async def produce(self):
        self._log.info('starting')
        while self._run:
            self._log.info('%d events', len(FAKE_EVENTS))
            for event in FAKE_EVENTS:
                await self._q.put({'project_id': event})
                self._log.info('NEW EVENT %s', event)
            self._log.info('pausing')
            await asyncio.sleep(1)
        await self._q.put(None)


class ProjectHandler:

    def __init__(self, project_id):
        self._log = logging.getLogger('ProjectHandler.{}'.format(project_id))
        self._log.info('new handler')
        self._project_id = project_id
        self._q = asyncio.Queue()
        loop = asyncio.get_event_loop()
        self.task = loop.create_task(error_handler(self._consumer(), project_id))

    async def _consumer(self):
        while True:
            self._log.debug('consumer waiting')
            event = await self._q.get()
            if event is None:
                self._log.info('consumer stopping')
                break
            self._log.info('consumer received %s', event)
            await asyncio.sleep(0.1)

    async def send(self, event):
        self._log.info('sending %s', event or '<stop signal>')
        await self._q.put(event)

    async def stop(self):
        await self.send(None)


class Dispatcher:

    _log = logging.getLogger('Dispatcher')

    def __init__(self):
        self._keep_running = True
        self._lock = asyncio.Lock()
        self._handlers = {}

    async def dispatch(self, project_id, event):
        if not self._keep_running:
            self._log.info('ignoring %s', event)
            return
        self._log.info('got %s', event)
        await self._lock.acquire()
        try:
            if project_id not in self._handlers:
                self._handlers[project_id] = ProjectHandler(project_id)
            handler = self._handlers[project_id]
        finally:
            self._lock.release()
        await handler.send(event)

    async def stop(self):
        await self._lock.acquire()
        try:
            self._log.info('stopping all %s handlers', len(self._handlers))
            self._keep_running = False
            for project_id, handler in self._handlers.items():
                await handler.stop()
        finally:
            self._lock.release()

    async def wait(self):
        await self._lock.acquire()
        try:
            handler_tasks = [
                h.task
                for h in self._handlers.values()
            ]
        finally:
            self._lock.release()
        if handler_tasks:
            self._log.info('waiting for handler tasks to complete')
            await asyncio.wait(handler_tasks)


async def handle_notification(dispatcher, event):
    # pretend to get the project_id from the event
    project_id = event.get('project_id')
    await dispatcher.dispatch(project_id, event)


async def notification_consumer(dispatcher, q):
    log = logging.getLogger('notification_consumer')
    log.info('starting')
    while True:
        log.debug('waiting')
        event = await q.get()
        if event is None:
            await dispatcher.stop()
            q.task_done()
            break
        await handle_notification(dispatcher, event)
        q.task_done()


async def main():

    q = asyncio.Queue(maxsize=NUM_CONCURRENT)
    dispatcher = Dispatcher()
    loop = asyncio.get_event_loop()
    consumer_task = loop.create_task(
        error_handler(notification_consumer(dispatcher, q), 'consumer')
    )
    producer = Producer(q)
    producer_task = loop.create_task(
        error_handler(producer.produce(), 'producer'),
    )

    await asyncio.wait([producer_task])
    LOG.info('producer finished')
    LOG.info('waiting for queue')
    await q.join()
    LOG.info('queue done')
    LOG.info('waiting for dispatcher to finish')
    await asyncio.wait([dispatcher.wait()])
    LOG.info('dispatcher finished')
    LOG.info('waiting for consumer to finish')
    await asyncio.wait([consumer_task])
    LOG.info('done')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
    )

    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(main())
    finally:
        event_loop.close()
