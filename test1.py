#!/usr/bin/env python3

import asyncio
import functools
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
NUM_CONCURRENT = 100


async def error_handler(coroutine, name):
    log = logging.getLogger('error_handler.{}'.format(name))
    try:
        await coroutine
    except Exception as err:
        log.traceback(err)


async def produce_events(q):
    log = logging.getLogger('produce')
    log.info('starting')
    while True:
        # This would really read from rabbitmq
        for event in FAKE_EVENTS:
            try:
                await q.put({'project_id': event})
                log.info('NEW EVENT %s', event)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                return
        # End fake data production


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
            self._log.debug('waiting for event')
            event = await self._q.get()
            if event is None:
                self._log.info('received <stop signal>')
                break
            self._log.info('received %s', event)
            await asyncio.sleep(0.2)

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
        #await handler.send(event)
        if event is None:
            await self.stop()
        else:
            loop = asyncio.get_event_loop()
            loop.create_task(handler.send(event))

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
            self._log.info('waiting for %d handler tasks to complete',
                           len(handler_tasks))
            await asyncio.wait(handler_tasks)


async def handle_notification(dispatcher, event):
    # pretend to get the project_id from the event
    project_id = event.get('project_id')
    await dispatcher.dispatch(project_id, event)


async def notification_consumer(dispatcher, q):
    log = logging.getLogger('notification_consumer')
    log.info('starting')
    loop = asyncio.get_event_loop()
    while True:
        log.info('waiting')
        event = await q.get()
        if event is None:
            log.info('STOPPING')
            q.task_done()
            break
        #await handle_notification(dispatcher, event)
        loop.create_task(handle_notification(dispatcher, event))
        q.task_done()


async def _stop(signame, producer, dispatcher, q):
    LOG.info('STOPPING on %s', signame)
    #await producer.stop()
    producer.cancel()
    # Stop the consumer
    await q.put(None)
    await dispatcher.stop()


def signal_handler(signame, producer, dispatcher, q):
    LOG.info('scheduling shutdown')
    loop = asyncio.get_event_loop()
    loop.create_task(
        error_handler(_stop(signame, producer, dispatcher, q),
                      '_stop'),
    )


async def main():

    loop = asyncio.get_event_loop()
    q = asyncio.Queue(maxsize=NUM_CONCURRENT)
    dispatcher = Dispatcher()
    producer_task = loop.create_task(
        error_handler(produce_events(q), 'produce_events'),
    )

    loop.add_signal_handler(
        signal.SIGINT,
        functools.partial(
            signal_handler,
            signame='SIGINT',
            producer=producer_task,
            dispatcher=dispatcher,
            q=q,
        ),
    )
    consumer_task = loop.create_task(
        error_handler(notification_consumer(dispatcher, q), 'consumer')
    )

    LOG.info('running')
    await asyncio.wait([producer_task])
    LOG.info('producer finished')

    LOG.info('waiting for consumer to finish')
    await asyncio.wait([consumer_task])
    LOG.info('consumer finished')

    LOG.info('waiting for queue')
    await q.join()
    LOG.info('queue empty')

    LOG.info('waiting for dispatcher to finish')
    await asyncio.wait([dispatcher.wait()])
    LOG.info('dispatcher finished')

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
