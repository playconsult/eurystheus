import json

from nose.tools import assert_raises, assert_equal

import os
from mock import patch
from mock import MagicMock
from nose import with_setup
from faker import Factory
from queue_processor import QueueProcessor
import logging

from faker.providers import BaseProvider

class MockProvider(BaseProvider):

    __provider__ = "magicmock"
    __lang__     = "en_GB"

    @classmethod
    def magicmock(cls):
        return MagicMock()


fake = Factory.create()
fake.add_provider(MockProvider)

def setup_func():
    os.environ['QUEUE_NAME'] = fake.pystr()
    logging.getLogger('queue_processor').setLevel(logging.DEBUG)

def teardown_func():
    "tear down test fixtures"

@with_setup(setup_func, teardown_func)
def test_task_decorator_registers_task():
    "test ..."
    # Arrange
    mock_task = MagicMock()
    mock_task_name = fake.pystr()

    # Act
    QueueProcessor.task(mock_task_name)(mock_task)

    # Assert
    assert_equal(QueueProcessor._tasks[mock_task_name], mock_task)

@with_setup(setup_func, teardown_func)
@patch('queue_processor.queue_processor.boto3.resource')
@patch('queue_processor.queue_processor.asyncio.get_event_loop')
def test_get_task_returns_task(mock_loop, mock_resource):
    "test ..."
    # Arrange
    mock_task = MagicMock()
    mock_task_name = fake.pystr()
    QueueProcessor._tasks[mock_task_name] = mock_task
    Q = QueueProcessor(run=False)

    # Act
    actual_task = Q._get_task(mock_task_name)

    # Assert
    assert_equal(actual_task, mock_task)


@with_setup(setup_func, teardown_func)
@patch('queue_processor.QueueProcessor._get_task')
@patch('queue_processor.queue_processor.boto3.resource')
@patch('queue_processor.queue_processor.asyncio.get_event_loop')
def test_check_environment_raises(mock_loop, mock_resource, mock_get_task):
    "test ..."
    # Arrange
    del os.environ['QUEUE_NAME']

    # Act/Assert
    with assert_raises(SystemExit):
        QueueProcessor(run=False)



@with_setup(setup_func, teardown_func)
@patch('queue_processor.QueueProcessor._get_task')
@patch('queue_processor.queue_processor.boto3.resource')
@patch('queue_processor.queue_processor.asyncio.get_event_loop')
def test_process_invokes_function(mock_loop, mock_resource, mock_get_task):
    "test ..."
    # Arrange
    Q = QueueProcessor(run=False)

    fask_task_name = fake.pystr()
    fake_params = fake.pylist()

    fake_message = {
        'task': fask_task_name,
        'parameters': fake_params
    }
    mock_task = MagicMock()
    mock_get_task.return_value = mock_task

    # Act
    Q.process(fake_message)

    # Assert
    mock_get_task.assert_called_once_with(fask_task_name)
    mock_task.assert_called_once_with(Q, *fake_params)


@with_setup(setup_func, teardown_func)
@patch('queue_processor.QueueProcessor._get_task')
@patch('queue_processor.QueueProcessor.log')
@patch('queue_processor.queue_processor.boto3.resource')
@patch('queue_processor.queue_processor.asyncio.get_event_loop')
def test_process_function_logs_error(mock_loop, mock_resource, mock_log, mock_get_task):
    "test ..."
    # Arrange
    Q = QueueProcessor(run=False)

    fask_task_name = fake.pystr()
    fake_params = fake.pylist()

    fake_message = {
        'task': fask_task_name,
        'parameters': fake_params
    }
    mock_task = MagicMock()
    def raise_exception(*args, **kwargs):
        raise Exception()
    mock_task.side_effect = raise_exception
    mock_get_task.return_value = mock_task

    # Act
    Q.process(fake_message)

    # Assert
    mock_get_task.assert_called_once_with(fask_task_name)
    mock_log.exception.assert_called()
    mock_task.assert_called_once_with(Q, *fake_params)

@with_setup(setup_func, teardown_func)
@patch('queue_processor.queue_processor.boto3.resource')
@patch('queue_processor.queue_processor.asyncio.get_event_loop')
def test_poll_polls_sqs_no_messages_does_nothing(mock_loop, mock_resource):
    "test ..."
    # Arrange
    mock_sqs = MagicMock()
    mock_queue = MagicMock()
    mock_sqs.get_queue_by_name.return_value = mock_queue
    mock_resource.return_value = mock_sqs

    Q = QueueProcessor(run=False)
    Q.process = MagicMock()

    # Act
    Q.poll().__next__()

    # Assert
    mock_queue.receive_messages.assert_called()

@with_setup(setup_func, teardown_func)
@patch('queue_processor.queue_processor.boto3.resource')
@patch('queue_processor.queue_processor.asyncio.get_event_loop')
def test_poll_polls_sqs_messages_deletes_messages(mock_loop, mock_resource):
    "test ..."
    # Arrange
    mock_sqs = MagicMock()
    mock_queue = MagicMock()
    mock_sqs.get_queue_by_name.return_value = mock_queue
    mock_resource.return_value = mock_sqs

    mock_process = MagicMock()

    def generate_message():
        message = {
            'task': fake.pystr(),
            'params': fake.pylist(10, False, int, str)
        }
        return message

    fake_bodies = []
    fake_messages = fake.pylist(10, False, MagicMock)
    for f in fake_messages:
        # TODO: need to make this have a task name and params that are mocks
        body = generate_message()
        f.body = json.dumps(body)
        fake_bodies.append(body)
    mock_queue.receive_messages.return_value = fake_messages

    Q = QueueProcessor(run=False)
    Q.process = mock_process

    # Act
    Q.poll().__next__()

    # Assert
    mock_queue.receive_messages.assert_called()
    for f in fake_bodies:
        mock_process.assert_any_call(f)
