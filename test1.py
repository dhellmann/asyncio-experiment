#!/usr/bin/env python3

import asyncio
import fnmatch
import functools
import itertools
import logging
import random
import signal

LOG = logging.getLogger()

# Simulate project IDs we might get from rabbit
FAKE_EVENTS = [
    {'project_id': '5d65eb04-5661-4e8d-9b95-e195505eca67',
     'resource_id': 'R1',
     'event': 'a.b.c',
    },

    {'project_id': '5e6e2d10-4c8e-4a78-8433-48754407dcd0',
     'resource_id': 'R2',
     'event': 'a.b.c',  # ignored
    },
    {'project_id': '5e6e2d10-4c8e-4a78-8433-48754407dcd0',
     'resource_id': 'R3',
     'event': 'd.e',
    },

    {'project_id': '5edcc66b-131d-47ad-a53a-b7071b1c0eb5',
     'resource_id': 'R4',
     'event': 'a.f',
    },
    {'project_id': '5edcc66b-131d-47ad-a53a-b7071b1c0eb5',
     'resource_id': 'R5',
     'event': 'g.h',  # ignored
    },
    {'project_id': '5edcc66b-131d-47ad-a53a-b7071b1c0eb5',
     'resource_id': 'R6',
     'event': 'g.h.i',
    },

    {'project_id': 'b2ac5681-95df-413e-af1c-74c0d70c3f12',
     'resource_id': 'R7',
     'event': 'a.b.c',
    },
]

# This should come from a configuration option
READ_AHEAD_MAX = 1

SUBSCRIPTIONS = [
    {'project_id': '5d65eb04-5661-4e8d-9b95-e195505eca67',
     'event': 'subscription.create',
     'parameters': {
         'event': ['a.*'],
     }
    },
    {'project_id': '5e6e2d10-4c8e-4a78-8433-48754407dcd0',
     'event': 'subscription.create',
     'parameters': {
         'event': ['d.e'],
     },
    },
    {'project_id': '5edcc66b-131d-47ad-a53a-b7071b1c0eb5',
     'event': 'subscription.create',
     'parameters': {
         'event': ['a.f'],
     },
    },
    {'project_id': '5edcc66b-131d-47ad-a53a-b7071b1c0eb5',
     'event': 'subscription.create',
     'parameters': {
         'event': ['g.h.i'],
     },
    },
    {'project_id': 'b2ac5681-95df-413e-af1c-74c0d70c3f12',
     'event': 'subscription.create',
     'parameters': {
         'event': ['a.b.*'],
     },
    },
]


async def error_handler(coroutine, name):
    log = logging.getLogger('error_handler.{}'.format(name))
    try:
        await coroutine
    except Exception as err:
        log.exception(err)


async def produce_events(q):
    log = logging.getLogger('produce')
    log.info('starting')
    to_send = FAKE_EVENTS[:]
    i = 0
    while True:
        # This would really read from rabbitmq
        random.shuffle(to_send)
        for event in to_send:
            try:
                i += 1
                event['num'] = i
                await q.put(event)
                log.info('NEW EVENT %s', event)
            except asyncio.CancelledError:
                # If we were canceled we didn't successfully enqueue
                # the current value of i.
                log.info('last num %d', i - 1)
                return


class SubscriptionHandler:

    def __init__(self, subscription):
        self._data = subscription
        self._project_id = subscription['project_id']
        self._patterns = subscription['parameters']['event']
        self._log = logging.getLogger(str(self))
        # Limit the queue size so we don't read more events than we
        # can send back out.
        self._q = asyncio.Queue(maxsize=1)
        loop = asyncio.get_event_loop()
        self.task = loop.create_task(
            error_handler(self._consumer(), self._project_id)
        )
        self._log.info('new subscription %s', self._patterns)

    def __str__(self):
        return 'SubscriptionHandler.{}.{}'.format(
            self._project_id, self._patterns)

    def match(self, event_id):
        return any(
            fnmatch.fnmatch(event_id, pat)
            for pat in self._patterns
        )

    async def _consumer(self):
        # TODO: Set up client to copy the events to zaqar.
        while True:
            self._log.debug('waiting for event')
            event = await self._q.get()
            if event is None:
                self._log.info('received <stop signal>')
                break
            self._log.info('received %s', event)
            await asyncio.sleep(0.2)
            # FIXME: Pretending to do something with the event.

    async def send(self, event):
        self._log.info('sending %s', event or '<stop signal>')
        await self._q.put(event)

    async def stop(self):
        await self.send(None)


class Dispatcher:

    _log = logging.getLogger('Dispatcher')

    def __init__(self):
        self._lock = asyncio.Lock()
        self._handlers = {}

    async def dispatch(self, project_id, event):
        if event is None:
            await self._stop()
            return

        self._log.info('got %s', event)
        event_id = event.get('event')

        await self._lock.acquire()
        try:
            if event_id == 'subscription.create':
                # Handle our own subscription events by creating new
                # handlers.
                # TODO: delete and update events for subscriptions
                if project_id not in self._handlers:
                    self._handlers[project_id] = []
                self._handlers[project_id].append(SubscriptionHandler(event))
            handlers = self._handlers.get(project_id, [])
        finally:
            self._lock.release()

        self._log.info('testing %d handlers for event id %s',
                       len(handlers), event_id)
        matched = 0
        for handler in handlers:
            if handler.match(event_id):
                await handler.send(event)
                matched += 1
        self._log.info('matched %d times', matched)

    @property
    def _all_handlers(self):
        return itertools.chain.from_iterable(self._handlers.values())

    async def _stop(self):
        await self._lock.acquire()
        try:
            self._log.info('stopping all handlers')
            for handler in self._all_handlers:
                await handler.stop()
        finally:
            self._lock.release()

    async def wait(self):
        await self._lock.acquire()
        try:
            handler_tasks = [
                h.task
                for h in self._all_handlers
            ]
        finally:
            self._lock.release()
        if handler_tasks:
            self._log.info('waiting for %d handler tasks to complete',
                           len(handler_tasks))
            await asyncio.wait(handler_tasks)


async def notification_consumer(dispatcher, q):
    log = logging.getLogger('notification_consumer')
    log.info('starting')
    loop = asyncio.get_event_loop()
    while True:
        log.debug('waiting')
        event = await q.get()

        if event:
            # pretend to get the project_id from the event
            project_id = event.get('project_id')
        else:
            project_id = None

        await dispatcher.dispatch(project_id, event)
        q.task_done()

        if event is None:
            log.info('STOPPING')
            break


async def _stop(signame, producer, q):
    LOG.info('STOPPING on %s', signame)
    producer.cancel()
    # Stop the consumer and dispatcher by sending a poison pill
    await q.put(None)


def signal_handler(signame, producer, q):
    LOG.info('scheduling shutdown')
    loop = asyncio.get_event_loop()
    loop.create_task(
        error_handler(_stop(signame, producer, q),
                      '_stop'),
    )


async def initialize_subscriptions(q):
    for subscription in SUBSCRIPTIONS:
        await q.put(subscription)


async def main():

    loop = asyncio.get_event_loop()
    q = asyncio.Queue(maxsize=READ_AHEAD_MAX)
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
            q=q,
        ),
    )
    consumer_task = loop.create_task(
        error_handler(notification_consumer(dispatcher, q), 'consumer')
    )

    await initialize_subscriptions(q)

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
