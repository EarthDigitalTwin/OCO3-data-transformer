import argparse
import os
from datetime import timedelta, datetime, timezone
from urllib.parse import urlparse

import boto3

TD_KWARGS = [
    'days',
    'seconds',
    'milliseconds',
    'microseconds',
    'minutes',
    'hours',
    'weeks'
]


def td(s: str):
    kwargs = {}

    for p in s.split(';'):
        fields = p.split('=')

        if len(fields) != 2:
            raise ValueError(f'Incorrect timedelta field {p}')

        k, v = tuple(fields)

        if k not in TD_KWARGS:
            raise ValueError(f'{k} is not a valid kwarg for timedelta')

        kwargs[k] = float(v)

    return timedelta(**kwargs)


parser = argparse.ArgumentParser()

rule = parser.add_mutually_exclusive_group(required=True)

rule.add_argument(
    '-n', '--number',
    type=int,
    help='Max number of logs to retain',
    dest='count'
)

rule.add_argument(
    '-a', '--age',
    type=td,
    help='Max age of logs to retain. String of form unit=value[(;unit=value)+] where unit is one of '
         'datetime.timedelta\'s kwargs. Eg: --age "weeks=2;days=2.5"',
    dest='age'
)

args = parser.parse_args()

now = datetime.now(timezone.utc)

session = boto3.Session(profile_name=os.getenv('AWS_PROFILE'))
s3 = session.client('s3')

try:
    s3_url = urlparse(os.environ['S3_PATH'])
except KeyError:
    print('Need to define \'S3_PATH\' environment var. Should be the S3 URI of the directory in which the logs are '
          'stored')
    exit(1)

bucket = s3_url.netloc
prefix = s3_url.path[1:]

paginator = s3.get_paginator('list_objects_v2')

bucket_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

keys = []

for page in bucket_iterator:
    for obj in page.get('Contents', []):
        keys.append(obj)

keys.sort(key=lambda x: x['LastModified'])

if args.count is not None:
    if args.count > 0:
        to_delete = keys[:-args.count]
    else:
        to_delete = keys[:]
else:
    to_delete = list(filter(
        lambda x: now - x['LastModified'] >= args.age,
        keys
    ))

to_delete = [dict(Key=o['Key']) for o in to_delete]

for k in to_delete:
    print(f'delete: {k}')

if len(to_delete) > 0:
    s3.delete_objects(Bucket=bucket, Delete=dict(Objects=to_delete))

print(f'total: {len(keys)}, to delete: {len(to_delete)}, keeping: {len(keys) - len(to_delete)}')

