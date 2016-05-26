import os
from mock import patch
from mock import MagicMock
from nose import with_setup
from faker import Factory
from queue_processor import QueueProcessor

fake = Factory.create()

def setup_func():
    os.environ['QUEUE_NAME'] = fake.pystr()

def teardown_func():
    "tear down test fixtures"

@with_setup(setup_func, teardown_func)
@patch('queue_processor.QueueProcessor._get_task')
@patch('queue_processor.queue_processor.boto3.resource')
def test_process_invokes_function(mock_resource, mock_get_task):
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

