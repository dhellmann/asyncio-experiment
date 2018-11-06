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


async def handle_notification(event):
    LOG.info('got %s', event)


async def notification_consumer(event_loop, q):
    LOG.info('starting notification_consumer')
    while True:
        LOG.info('notification_consumer waiting')
        event = await q.get()
        if event is None:
            break
        event_loop.create_task(handle_notification(event))


async def main(event_loop):

    q = asyncio.Queue(maxsize=NUM_CONCURRENT)
    consumer_task = event_loop.create_task(notification_consumer(event_loop, q))
    producer_task = event_loop.create_task(notification_producer(q))

    await asyncio.wait([consumer_task, producer_task])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
    )

    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(main(event_loop))
    finally:
        event_loop.close()
