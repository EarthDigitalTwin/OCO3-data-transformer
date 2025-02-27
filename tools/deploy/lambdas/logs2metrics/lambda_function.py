# Copyright 2025 California Institute of Technology (Caltech)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import base64
import gzip
import json
import os
from datetime import datetime

import boto3


def handle_progress_metric(message: str, timestamp, client):
    print('Handling progress report')

    json_start = message.index('{')
    json_end = message.rindex('}') + 1

    message_data = message[json_start:json_end]

    decoded_message = json.loads(message_data)

    metric_data = []

    if decoded_message['task'] == 'data generation':
        for ds in decoded_message['subtask_progress']:
            metric_data.append({
                'MetricName': 'Process Counts',
                'Dimensions': [
                    {
                        'Name': 'Dataset',
                        'Value': ds.upper()
                    },
                    {
                        'Name': 'Count',
                        'Value': 'Total'
                    }
                ],
                'Value': decoded_message['subtask_progress'][ds]['total'],
                'Timestamp': datetime.fromtimestamp(timestamp / 1000),
                'Unit': 'Count',
                'StorageResolution': 1,
            })
            metric_data.append({
                'MetricName': 'Process Counts',
                'Dimensions': [
                    {
                        'Name': 'Dataset',
                        'Value': ds.upper()
                    },
                    {
                        'Name': 'Count',
                        'Value': 'Completed'
                    }
                ],
                'Value': decoded_message['subtask_progress'][ds]['complete'],
                'Timestamp': datetime.fromtimestamp(timestamp / 1000),
                'Unit': 'Count',
                'StorageResolution': 1,
            })

        metric_data.append({
            'MetricName': 'Total Process Counts',
            'Dimensions': [
                {
                    'Name': 'Count',
                    'Value': 'Total'
                }
            ],
            'Value': decoded_message['total_subtasks'],
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Count',
            'StorageResolution': 1,
        })

        metric_data.append({
            'MetricName': 'Total Process Counts',
            'Dimensions': [
                {
                    'Name': 'Count',
                    'Value': 'Completed'
                }
            ],
            'Value': decoded_message['total_complete'],
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Count',
            'StorageResolution': 1,
        })

        metric_data.append({
            'MetricName': 'Process Completion',
            'Value': decoded_message['percent_total_complete'] * 100,
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Percent',
            'StorageResolution': 1,
        })
    elif decoded_message['task'] == 'data write':
        for ds in decoded_message['subtask_progress']:
            metric_data.append({
                'MetricName': 'Write Counts',
                'Dimensions': [
                    {
                        'Name': 'Dataset',
                        'Value': ds.upper()
                    },
                    {
                        'Name': 'Count',
                        'Value': 'Total'
                    }
                ],
                'Value': decoded_message['subtask_progress'][ds]['total'],
                'Timestamp': datetime.fromtimestamp(timestamp / 1000),
                'Unit': 'Count',
                'StorageResolution': 1,
            })
            metric_data.append({
                'MetricName': 'Write Counts',
                'Dimensions': [
                    {
                        'Name': 'Dataset',
                        'Value': ds.upper()
                    },
                    {
                        'Name': 'Count',
                        'Value': 'Completed'
                    }
                ],
                'Value': decoded_message['subtask_progress'][ds]['complete'],
                'Timestamp': datetime.fromtimestamp(timestamp / 1000),
                'Unit': 'Count',
                'StorageResolution': 1,
            })

        metric_data.append({
            'MetricName': 'Total Write Counts',
            'Dimensions': [
                {
                    'Name': 'Count',
                    'Value': 'Total'
                }
            ],
            'Value': decoded_message['total_subtasks'],
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Count',
            'StorageResolution': 1,
        })

        metric_data.append({
            'MetricName': 'Total Write Counts',
            'Dimensions': [
                {
                    'Name': 'Count',
                    'Value': 'Completed'
                }
            ],
            'Value': decoded_message['total_complete'],
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Count',
            'StorageResolution': 1,
        })

        metric_data.append({
            'MetricName': 'Write Completion',
            'Value': decoded_message['percent_total_complete'] * 100,
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Percent',
            'StorageResolution': 1,
        })

    client.put_metric_data(
        Namespace=os.getenv('NAMESPACE'),
        MetricData=metric_data
    )


def handle_cmr_report(message: str, timestamp, client):
    print('Handling CMR report')

    json_start = message.index('{')
    json_end = message.rindex('}') + 1

    message_data = message[json_start:json_end]

    decoded_message = json.loads(message_data)

    metric_data = [
        {
            'MetricName': 'CMR Last Feature Count',
            'Dimensions': [
                {
                    'Name': 'Count',
                    'Value': 'Features'
                }
            ],
            'Value': decoded_message['features'],
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Count',
            'StorageResolution': 60,
        },
        {
            'MetricName': 'Last New Granule Count',
            'Dimensions': [
                {
                    'Name': 'Count',
                    'Value': 'Granules'
                }
            ],
            'Value': decoded_message['new'],
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Count',
            'StorageResolution': 60,
        },
        {
            'MetricName': 'Last Remaining Data Day Count',
            'Dimensions': [
                {
                    'Name': 'Count',
                    'Value': 'Days'
                }
            ],
            'Value': decoded_message['days'],
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Count',
            'StorageResolution': 60,
        },
    ]

    client.put_metric_data(
        Namespace=os.getenv('NAMESPACE'),
        MetricData=metric_data
    )


def handle_finished_execution(message: str, timestamp, client):
    print('Handling finished execution')

    message_data = json.loads(message)

    sfn = boto3.client('stepfunctions')
    arn = message_data['execution_arn']

    response = sfn.describe_execution(executionArn=arn)
    execution_start: datetime = response['startDate']
    execution_end: datetime = response['stopDate']
    execution_duration = execution_end - execution_start

    metric_data = {
        'MetricName': 'Execution time',
        'Value': execution_duration.total_seconds(),
        'Timestamp': datetime.fromtimestamp(timestamp / 1000),
        'Unit': 'Seconds',
        'StorageResolution': 60,
    }

    client.put_metric_data(
        Namespace=os.getenv('NAMESPACE'),
        MetricData=[metric_data]
    )


def handle_profiling_report(message: str, timestamp, client):
    print('Handling profiling report')

    parts = message.split('|')

    parts[0] = parts[0].split(']')[-1].strip()

    metric_data = [
        {
            'MetricName': 'Process CPU Utilization',
            'Value': float(parts[0].split('=')[-1]),
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Percent',
            'StorageResolution': 1,
        },
        {
            'MetricName': 'Process Memory Utilization',
            'Dimensions': [
                {
                    'Name': 'Memory',
                    'Value': 'Physical Memory'
                }
            ],
            'Value': int(parts[1].split()[1].replace(',', '')),
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Megabytes',
            'StorageResolution': 1,
        },
        {
            'MetricName': 'Process Memory Utilization',
            'Dimensions': [
                {
                    'Name': 'Memory',
                    'Value': 'Virtual Memory'
                }
            ],
            'Value': int(parts[2].split()[1].replace(',', '')),
            'Timestamp': datetime.fromtimestamp(timestamp / 1000),
            'Unit': 'Megabytes',
            'StorageResolution': 1,
        },
    ]

    client.put_metric_data(
        Namespace=os.getenv('NAMESPACE'),
        MetricData=metric_data
    )


def decode_event(event):
    print('Decoding event')

    event_data = event['awslogs']['data']

    compressed_data = base64.b64decode(event_data)
    uncompressed_data = gzip.decompress(compressed_data)

    event_dict = json.loads(uncompressed_data)

    return event_dict


def lambda_handler(event, context):
    client = boto3.client('cloudwatch')

    if os.getenv('NAMESPACE') is None:
        return {'success': False, 'error': 'NAMESPACE not set'}

    try:
        event = decode_event(event)

        for log_event in event['logEvents']:
            event_message = log_event['message']

            if 'progress_json::' in event_message:
                handle_progress_metric(event_message, log_event['timestamp'], client)
            elif 'cmr_granule_report:' in event_message:
                handle_cmr_report(event_message, log_event['timestamp'], client)
            elif '"ExecutionSucceeded"' in event_message or '"ExecutionFailed"' in event_message:
                handle_finished_execution(event_message, log_event['timestamp'], client)
            elif 'metrics::' in event_message:
                handle_profiling_report(event_message, log_event['timestamp'], client)
    except Exception as e:
        print(e)
        return {'success': False, 'error': str(e)}
