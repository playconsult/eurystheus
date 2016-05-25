import asyncio
import json
from os import environ
from sys import stderr

import boto3


class QueueProcessor:
    """

    """
    queue_name = environ.get('QUEUE_NAME', None)
    _tasks = dict()

    def get_task(self, name):
        return self._tasks.get(name)

    def process(self, message):
        task = self.get_task(message.get('task'))
        if task is not None:
            task(self, *message.get('parameters', []))

    @asyncio.coroutine
    def poll(self):
        for message in self.queue.receive_messages():
            message_body = json.loads(message.body)
            self.process(message_body)
            message.delete()
        yield from asyncio.sleep(1)

    @asyncio.coroutine
    def loop_executer(self, loop):
        # you could use even while True here
        while loop.is_running():
            yield from asyncio.wait([self.poll()])

    def __init__(self):
        if self.queue_name is None:
            print('QUEUE_NAME environment variable is not set.', file=stderr)
            exit(1)
        # Get the service resource
        sqs = boto3.resource('sqs')
        # Get the queue. This returns an SQS.Queue instance
        self.queue = sqs.get_queue_by_name(QueueName=self.queue_name)
        print('step: asyncio.get_event_loop()')
        loop = asyncio.get_event_loop()
        try:
            print('step: loop.run_until_complete()')
            loop.create_task(self.loop_executer(loop))
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            print('step: loop.close()')
            loop.close()

    @classmethod
    def task(cls, name):
        def decorator(func):
            cls._tasks[name] = func
            return func
        return decorator
