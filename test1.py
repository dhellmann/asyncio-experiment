#!/usr/bin/env python3

import asyncio
import logging

LOG = logging.getLogger()

# Simulate project IDs we might get from rabbit
FAKE_EVENTS = [
    '5e6e2d10-4c8e-4a78-8433-48754407dcd0',
    '5d65eb04-5661-4e8d-9b95-e195505eca67',
    '5edcc66b-131d-47ad-a53a-b7071b1c0eb5',
    '5d65eb04-5661-4e8d-9b95-e195505eca67',
    '5e6e2d10-4c8e-4a78-8433-48754407dcd0',
]

# This should come from a configuration option
NUM_CONCURRENT = 3


async def notification_producer(q):
    LOG.info('starting notification_producer')
    for event in FAKE_EVENTS:
        LOG.info('sending %s', event)
        await q.put(event)
    await q.put(None)


_keep_running = True
_queue_lock = asyncio.Lock()
_project_queues = {}


class ProjectHandler:
    def __init__(self, project_id, event_loop):
        LOG.info('creating handler for %s', project_id)
        self._project_id = project_id
        self._q = asyncio.Queue()
        self.task = event_loop.create_task(self._consumer(event_loop))

    async def _consumer(self, event_loop):
        while True:
            LOG.info('%s consumer waiting', self._project_id)
            event = await self._q.get()
            if event is None:
                LOG.info('%s consumer stopping', self._project_id)
                break
            LOG.info('%s consumer received %s', self._project_id, event)

    async def send(self, event):
        LOG.info('sending %s to %s', event, self._project_id)
        await self._q.put(event)

    async def stop(self):
        LOG.info('sending stop to %s consumer', self._project_id)
        await self.send(None)


async def handle_notification(event, event_loop):
    if not _keep_running:
        LOG.info('ignoring %s', event)
        return
    LOG.info('got %s', event)
    project_id = event
    await _queue_lock.acquire()
    try:
        if project_id not in _project_queues:
            _project_queues[project_id] = ProjectHandler(project_id, event_loop)
        handler = _project_queues[project_id]
    finally:
        _queue_lock.release()
    await handler.send(event)


async def stop_handlers():
    await _queue_lock.acquire()
    try:
        LOG.info('stopping all %s handlers', len(_project_queues))
        _keep_running = False
        for project_id, handler in _project_queues.items():
            await handler.stop()
    finally:
        _queue_lock.release()


async def notification_consumer(event_loop, q):
    LOG.info('starting notification_consumer')
    while True:
        LOG.info('notification_consumer waiting')
        event = await q.get()
        if event is None:
            await stop_handlers()
            break
        await handle_notification(event, event_loop)


async def main(event_loop):

    q = asyncio.Queue(maxsize=NUM_CONCURRENT)
    consumer_task = event_loop.create_task(notification_consumer(event_loop, q))
    producer_task = event_loop.create_task(notification_producer(q))

    await asyncio.wait([producer_task])
    await _queue_lock.acquire()
    try:
        handler_tasks = [
            h.task
            for h in _project_queues.values()
        ]
    finally:
        _queue_lock.release()
    LOG.info('waiting for handler tasks')
    await asyncio.wait(handler_tasks)
    await asyncio.wait([consumer_task])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
    )

    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(main(event_loop))
    finally:
        event_loop.close()
