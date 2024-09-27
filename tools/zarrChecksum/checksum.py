# Copyright 2024 California Institute of Technology (Caltech)
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

import argparse
import hashlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Tuple
from urllib.parse import urlparse, urlunparse

import boto3

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)

logger = logging.getLogger('zarr_checksum')

SUPPRESS = [
    'botocore',
    'urllib3',
]

for logger_name in SUPPRESS:
    logging.getLogger(logger_name).setLevel(logging.WARNING)


def format_long_bytes(b: bytes):
    data_hex = b.hex()

    if len(data_hex) > 35:
        data_hex = f'{data_hex[:16]}...{data_hex[-16:]}'

    return data_hex


def open_local(path: str) -> Tuple[str, bytes]:
    data = open(path, 'rb').read()

    logger.debug(f'local file {path} -> 0x{format_long_bytes(data)}')

    return str(path), data


def open_s3(key, bucket, s3) -> Tuple[str, bytes]:
    data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()

    url = urlunparse(('s3', bucket, key, None, None, None))

    logger.debug(f'S3 object {url} -> 0x{format_long_bytes(data)}')

    return str(key), data


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'path',
        help='Path or URL to zarr root'
    )

    parser.add_argument(
        '--type',
        dest='store_type',
        choices=['local', 's3'],
        default='local'
    )

    parser.add_argument(
        '-p',
        dest='profile',
        help='AWS credentials profile'
    )

    parser.add_argument(
        '-v',
        dest='verbose',
        action='store_true',
        help='Verbose output'
    )

    args = parser.parse_args()

    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    logger.info('Getting list of files to process')

    if args.store_type == 'local':
        zarr_files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(args.path) for f in filenames]
        f_open = open_local
    else:
        zarr_files = []

        session = boto3.Session(profile_name=args.profile)
        s3 = session.client('s3')

        url_parsed = urlparse(args.path)

        bucket = url_parsed.netloc
        prefix = url_parsed.path.strip('/')

        paginator = s3.get_paginator('list_objects_v2')
        list_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in list_iterator:
            zarr_files.extend(page.get('Contents', []))

        zarr_files = [k['Key'] for k in zarr_files]

        f_open = partial(open_s3, s3=s3, bucket=bucket)

    with ThreadPoolExecutor(thread_name_prefix='s3_worker') as pool:
        parts = []

        logger.info(f'Computing initial hashes for {len(zarr_files):,} files')

        for p in pool.map(f_open, zarr_files):
            parts.append(p)

    parts = [p[1] for p in sorted(parts, key=lambda x: x[0])]

    while len(parts) > 1:
        next_parts = []

        for p in [parts[i:i+2] for i in range(0, len(parts), 2)]:
            if len(p) == 1:
                a = p[0]

                if isinstance(a, str):
                    next_parts.append(f_open(a))
                else:
                    next_parts.append(a)
            else:
                a, b = tuple(p)

                if isinstance(a, str):
                    a = f_open(a)
                if isinstance(b, str):
                    b = f_open(b)

                # m = hashlib.sha256()
                m = hashlib.md5()
                m.update(a)
                m.update(b)

                digest = m.digest()

                logger.debug(f'md5(0x{format_long_bytes(a)} + 0x{format_long_bytes(b)}) -> 0x{digest.hex()}')

                next_parts.append(digest)

        logger.info(f'Combined {len(parts):,} hashes into {len(next_parts):,}')

        parts = next_parts

    print(f'Checksum: 0x{parts[0].hex()}')


if __name__ == '__main__':
    main()

