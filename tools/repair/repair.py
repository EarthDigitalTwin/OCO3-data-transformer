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
import json
import logging
import os
import sys
import tarfile
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from shutil import copytree, rmtree
from typing import Literal
from urllib.parse import urlparse

import boto3
from sam_extract.writers import ZarrWriter
from sam_extract.utils import ZARR_REPAIR_FILE, delete_zarr_backup, AWSConfig
from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader as Loader

log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s')
log_level = logging.DEBUG
log_handlers = []

stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(log_level)
log_handlers.append(stream_handler)

logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger.addHandler(stream_handler)

SUPPRESS = [
    'botocore',
    's3transfer',
    'urllib3',
]

for logger_name in SUPPRESS:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

root_logger = logging.getLogger()
for each in root_logger.handlers:
    root_logger.removeHandler(each)


def delete_group(
        path: str,
        store_type: Literal['local', 's3'],
        store_params: AWSConfig = None
):
    path = path.rstrip('/')

    logger.info(f'Deleting potentially corrupted data at {path}')

    try:
        # Check that the path we're deleting is actually Zarr data
        if not path.endswith('.tar'):
            ZarrWriter.open_zarr_group(path, store_type, store_params, root=True)
        else:
            likely_zarr = False
            with tarfile.open(path, 'r') as tar:
                for f in tar.getnames():
                    if os.path.basename(f) == '.zgroup':
                        likely_zarr = True
                        break
            if not likely_zarr:
                logger.error(f'Backup archive file at {path} does not appear to be a Zarr group')
                return False
    except Exception as e:
        logger.warning(f'Zarr group at path {path} could not be opened. Maybe it does not exist. Error: {repr(e)}')
        return False

    if store_type == 'local':
        try:
            path = urlparse(path).path.rstrip('/')

            if not path.endswith('.tar'):
                logger.info(f'Recursively deleting {path}')
                rmtree(path)
            else:
                logger.info(f'Deleting {path}')
                os.remove(path)
        except Exception as e:
            logger.exception(e)
            return False
    elif store_type == 's3':
        try:
            src_parsed = urlparse(path)

            bucket = src_parsed.netloc
            src_key_prefix = src_parsed.path.strip('/')

            s3 = boto3.client('s3', **store_params.to_client_kwargs())
            paginator = s3.get_paginator('list_objects_v2')

            list_iterator = paginator.paginate(Bucket=bucket, Prefix=src_key_prefix)

            to_delete = []

            for page in list_iterator:
                to_delete.extend(page.get('Contents', []))

            to_delete = [dict(Key=k['Key']) for k in to_delete]

            batches = [to_delete[i:i + 1000] for i in range(0, len(to_delete), 1000)]

            for batch in batches:
                logger.debug(f'Deleting {len(batch):,} keys')
                s3.delete_objects(Bucket=bucket, Delete=dict(Objects=batch))
        except Exception as e:
            logger.exception(e)
            return False
    else:
        raise ValueError(f'Invalid store type: {store_type}')

    return True


def restore_backup(
        src_path: str,
        dst_path: str,
        store_type: Literal['local', 's3'],
        store_params: AWSConfig = None
) -> bool:
    logger.info(f'Restoring backup from {src_path} to {dst_path}')

    src_path = src_path.rstrip('/')
    dst_path = dst_path.rstrip('/')

    if store_type == 'local':
        if not src_path.endswith('.tar'):
            logger.info(f'Copying files on local fs')
            copytree(src_path, dst_path)
        else:
            logger.info(f'Extracting {src_path}')
            dst_path = os.path.dirname(urlparse(dst_path).path)

            with tarfile.open(src_path, 'r') as tar:
                tar.extractall(path=dst_path)
    elif store_type == 's3':
        src_parsed = urlparse(src_path)

        bucket = src_parsed.netloc
        src_key_prefix = src_parsed.path.strip('/')
        dst_key_prefix = urlparse(dst_path).path.strip('/')

        logger.info(f'Copying to s3://{bucket}/{dst_key_prefix}')

        s3 = boto3.client('s3', **store_params.to_client_kwargs())
        paginator = s3.get_paginator('list_objects_v2')

        list_iterator = paginator.paginate(Bucket=bucket, Prefix=src_key_prefix)

        src_keys = []

        for page in list_iterator:
            src_keys.extend(page.get('Contents', []))

        def s3_copy(s3_bucket, src_prefix, dst_prefix, key):
            key = key['Key']

            dst_key = str(key).lstrip('/').replace(src_prefix, dst_prefix)
            logger.debug(f'{key} -> {dst_key}')

            s3.copy_object(
                Bucket=s3_bucket,
                CopySource=dict(
                    Bucket=s3_bucket,
                    Key=key
                ),
                Key=dst_key
            )

        s3cp = partial(s3_copy, bucket, src_key_prefix, dst_key_prefix)

        with ThreadPoolExecutor(thread_name_prefix='s3_copy_worker') as pool:
            futures = []
            for key in sorted(src_keys, key=lambda x: x['Key']):
                futures.append(pool.submit(s3cp, key))

            for f in futures:
                f.result()

    return True


def main():
    parser = argparse.ArgumentParser(description='Restores zarr from backups')

    parser.add_argument(
        '--rc',
        dest='run_config',
        required=True,
        help='The run configuration YAML file for the dataset being repaired.'
    )

    args = parser.parse_args()

    logger.info('Reading backup and destination data')

    with open(ZARR_REPAIR_FILE) as fp:
        backups = json.load(fp)

    with open(args.run_config) as fp:
        out_cfg = load(fp, Loader=Loader)['output']

    store_type: Literal['local', 's3']

    if 'local' in out_cfg:
        store_type = 'local'
        path_root = out_cfg['local']
        aws_config = None
    elif 's3' in out_cfg:
        store_type = 's3'
        path_root = out_cfg['s3']['url']
        aws_config = AWSConfig(
            key=out_cfg['s3']['auth']['accessKeyID'],
            secret=out_cfg['s3']['auth']['secretAccessKey'],
            region=out_cfg['s3'].get('region', None)
        )
    else:
        raise ValueError('Invalid RC file')

    pre_qf_dst = os.path.join(path_root, out_cfg['naming']['pre_qf'])
    post_qf_dst = os.path.join(path_root, out_cfg['naming']['post_qf'])

    if not delete_group(pre_qf_dst, store_type, aws_config):
        raise RuntimeError(f'Failed to delete corrupted data!')

    if not delete_group(post_qf_dst, store_type, aws_config):
        raise RuntimeError(f'Failed to delete corrupted data!')

    pre_qf_backup = backups['pre_qf_backup']
    post_qf_backup = backups['post_qf_backup']

    if pre_qf_backup is not None:
        if not restore_backup(pre_qf_backup, pre_qf_dst, store_type, aws_config):
            raise RuntimeError(f'Failed to restore backup from {pre_qf_backup}')
    else:
        logger.info(f'No pre-qf backup exists to restore')

    if post_qf_backup is not None:
        if not restore_backup(post_qf_backup, post_qf_dst, store_type, aws_config):
            raise RuntimeError(f'Failed to restore backup from {post_qf_backup}')
    else:
        logger.info(f'No post-qf backup exists to restore')

    logger.info('Data restored successfully; removing old backups')

    if pre_qf_backup is not None:
        if aws_config is not None:
            store_params = aws_config.to_store_params()
        else:
            store_params = {}
        delete_zarr_backup(pre_qf_backup, store_type, store_params)

    if post_qf_backup is not None:
        if aws_config is not None:
            store_params = aws_config.to_store_params()
        else:
            store_params = {}
        delete_zarr_backup(post_qf_backup, store_type, store_params)

    logger.info('Restore complete')


if __name__ == '__main__':
    main()

