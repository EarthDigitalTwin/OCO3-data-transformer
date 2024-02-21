# Copyright 2023 California Institute of Technology (Caltech)
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


import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from os.path import basename, dirname, join
from shutil import copytree, rmtree
from typing import Literal
from urllib.parse import urlparse
from uuid import uuid4

import boto3
from botocore.config import Config
from sam_extract.writers import ZarrWriter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AWSConfig:
    def __init__(self, key, secret, region=None):
        self.key = key
        self.secret = secret
        self.region = region

    @staticmethod
    def from_store_params(params):
        return AWSConfig(
            params['auth']['accessKeyID'],
            params['auth']['secretAccessKey'],
            params.get('region', 'us-west-2')
        )

    def to_store_params(self):
        return dict(region=self.region, auth=dict(accessKeyID=self.key, secretAccessKey=self.secret))

    def to_client_kwargs(self):
        kwargs = dict(
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret
        )

        if self.region:
            kwargs['config'] = Config(region_name=self.region)

        return kwargs


def create_backup(src_path: str, store_type: Literal['local', 's3'], store_params: dict = None) -> str | None:
    backup_id = str(uuid4())

    logger.info(f'Creating backup {backup_id}')

    src_path = src_path.rstrip('/')

    src_basename = basename(src_path)

    dst_basename = f'{".".join(src_basename.split(".")[:-1])}-{backup_id}.zarr'

    try:
        # Check that the path we're deleting is actually Zarr data
        ZarrWriter.open_zarr_group(src_path, store_type, store_params, root=True)
    except Exception as e:
        logger.warning(f'Zarr group at path {src_path} could not be opened for backup. Maybe it does not exist yet. A '
                       f'backup will not be created. Error: {repr(e)}')
        return None

    if store_type == 'local':
        src_path = urlparse(src_path).path
        src_dirname = dirname(src_path)

        logger.info(f'Copying {src_path} to {join(src_dirname, dst_basename)}')

        copytree(src_path, join(src_dirname, dst_basename))

        return join(src_dirname, dst_basename)
    elif store_type == 's3':
        src_parsed = urlparse(src_path)

        bucket = src_parsed.netloc
        src_key_prefix = src_parsed.path.strip('/')
        dst_key_prefix = join(dirname(src_key_prefix), dst_basename)

        logger.info(f'Copying to s3://{bucket}/{dst_key_prefix}')

        s3 = boto3.client('s3', **AWSConfig.from_store_params(store_params).to_client_kwargs())
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

        return f's3://{bucket}/{dst_key_prefix}'
    else:
        raise ValueError(f'Invalid store type: {store_type}')


def delete_backup(path: str, store_type: Literal['local', 's3'], store_params: dict = None) -> bool:
    path = path.rstrip('/')

    logger.info(f'Deleting backup at {path}')

    try:
        # Check that the path we're deleting is actually Zarr data
        ZarrWriter.open_zarr_group(path, store_type, store_params, root=True)
    except Exception as e:
        logger.warning(f'Zarr group at path {path} could not be opened. Maybe it does not exist. Error: {repr(e)}')
        return False

    if store_type == 'local':
        try:
            path = urlparse(path).path.rstrip('/')

            logger.info(f'Recursively deleting {path}')
            rmtree(path)
        except Exception as e:
            logger.exception(e)
            return False
    elif store_type == 's3':
        try:
            src_parsed = urlparse(path)

            bucket = src_parsed.netloc
            src_key_prefix = src_parsed.path.strip('/')

            s3 = boto3.client('s3', **AWSConfig.from_store_params(store_params).to_client_kwargs())
            paginator = s3.get_paginator('list_objects_v2')

            list_iterator = paginator.paginate(Bucket=bucket, Prefix=src_key_prefix)

            to_delete = []

            for page in list_iterator:
                to_delete.extend(page.get('Contents', []))

            to_delete = [dict(Key=k['Key']) for k in to_delete]

            batches = [to_delete[i:i+1000] for i in range(0, len(to_delete), 1000)]

            for batch in batches:
                logger.debug(f'Deleting {len(batch):,} objects')
                resp = s3.delete_objects(Bucket=bucket, Delete=dict(Objects=batch))

                if len(resp['Deleted']) != len(batch):
                    logger.error(f'{len(resp["Errors"]):,} objects could not be deleted')

                    retries = 3

                    while len(resp["Errors"]) > 0 and retries > 0:
                        logger.debug(f'Retrying {len(resp["Errors"])} objects')
                        resp = s3.delete_objects(
                            Bucket=bucket,
                            Delete=dict(Objects=[dict(Key=e['Key']) for e in resp['Errors']])
                        )
        except Exception as e:
            logger.exception(e)
            return False
    else:
        raise ValueError(f'Invalid store type: {store_type}')

    return True
