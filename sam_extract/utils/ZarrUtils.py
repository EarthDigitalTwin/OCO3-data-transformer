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


import logging
import os
import subprocess
import sys
import tarfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from os.path import basename, dirname, join
from shutil import copytree, rmtree
from typing import Dict
from urllib.parse import urlparse
from uuid import uuid4

import boto3
import s3fs
import xarray as xr
from botocore.config import Config
from sam_extract import GROUP_KEYS
from xarray import Dataset

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


def open_zarr_group(path, store_type, params, root=False, **xr_open_kwargs) -> Dict[str, Dataset]:
    logger.debug(f'Opening Zarr array group at {path}')

    if store_type == 'local':
        store = path
    elif store_type == 's3':
        url = urlparse(path)

        bucket = url.netloc
        key = url.path

        if params.get('public', False):
            store = f'https://{bucket}.s3.{params["region"]}.amazonaws.com{key}'  # key has leading /
        else:
            s3 = s3fs.S3FileSystem(
                False,
                key=params['auth']['accessKeyID'],
                secret=params['auth']['secretAccessKey'],
                client_kwargs=dict(region_name=params["region"])
            )
            store = s3fs.S3Map(root=path, s3=s3, check=False)
    else:
        raise ValueError(store_type)

    if root:
        return {
            '/': xr.open_zarr(store, consolidated=True, **xr_open_kwargs),
        }
    else:
        groups = {'/': xr.open_zarr(store, consolidated=True, mask_and_scale=True, **xr_open_kwargs)}

        for group in GROUP_KEYS:
            if group == '/':
                continue

            try:
                groups[group] = xr.open_zarr(
                    store, group=group[1:], consolidated=True, mask_and_scale=True, **xr_open_kwargs
                )
            except:
                pass

        return groups


def create_backup(src_path: str, store_type, store_params: dict = None, verify_zarr=True) -> str | None:
    start_t = datetime.now()
    backup_id = str(uuid4())

    logger.info(f'Creating backup {backup_id}')

    src_path = src_path.rstrip('/')

    src_basename = basename(src_path)

    dst_basename = f'{src_basename.removesuffix(".zarr")}-{backup_id}.zarr'

    if verify_zarr:
        try:
            # Check that the path we're deleting is actually Zarr data
            open_zarr_group(src_path, store_type, store_params, root=True)
        except Exception as e:
            logger.warning(f'Zarr group at path {src_path} could not be opened for backup. Maybe it does not exist yet.'
                           f' A backup will not be created. Error: {repr(e)}')
            logger.exception(e)
            return None
    elif store_type == 'local' and (not os.path.exists(urlparse(src_path).path) or
                                    not os.path.isdir(urlparse(src_path).path)):
        logger.warning(f'No directory to back up at {src_path}')
        return None
    elif store_type == 's3':
        src_parsed = urlparse(src_path)

        bucket = src_parsed.netloc
        src_key_prefix = src_parsed.path.strip('/')

        s3 = boto3.client('s3', **AWSConfig.from_store_params(store_params).to_client_kwargs())

        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=f'{src_key_prefix}/', MaxKeys=1)
            if response['KeyCount'] == 0:
                logger.warning(f'No data could be found at {src_path}')
                return None
        except:
            logger.warning('Could not access S3')
            return None

    if store_type == 'local':
        src_path = urlparse(src_path).path
        src_dirname = dirname(src_path)

        efs = os.getenv('USING_EFS') is not None

        if efs:
            dst_basename = f'{dst_basename}.tar'

        logger.info(f'Copying {src_path} to {join(src_dirname, dst_basename)}')

        if efs:
            files = [join(dp, f) for dp, dn, filenames in os.walk(src_path) for f in filenames]

            with tarfile.open(join(src_dirname, dst_basename), 'w') as tar:
                for i, f in enumerate(files, 1):
                    arc_f = f.removeprefix(src_dirname).lstrip('/')
                    logger.debug(f'a {arc_f}')
                    tar.add(f, arcname=arc_f)

                    if i % 10000 == 0:
                        logger.info(f'Added {i:,} objects to backup {backup_id}')
        else:
            if sys.platform.startswith('win'):
                cmd = ['xcopy', src_path, join(src_dirname, dst_basename)]
            else:
                cmd = ['cp', '-rf', src_path, join(src_dirname, dst_basename)]

            try:
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                err = p.wait()

                if err != 0:
                    stdout, _ = p.communicate()

                    stdout = stdout.decode('utf-8')[-32:]

                    raise RuntimeError(f'Something went wrong. Exit code, stdout (last 32 bytes)'
                                       f': {err} {stdout}')
            except Exception as e:
                logger.exception(e)
                logger.warning('Subprocess copy failed, using shutil fallback (will take longer)')
                copytree(src_path, join(src_dirname, dst_basename), dirs_exist_ok=True)

        logger.info(f'Finished backup {backup_id} in {datetime.now() - start_t}')

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
            logger.trace(f'{key} -> {dst_key}')

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

        logger.info(f'Finished backup {backup_id} in {datetime.now() - start_t}')

        return f's3://{bucket}/{dst_key_prefix}'
    else:
        raise ValueError(f'Invalid store type: {store_type}')


def delete_backup(path: str, store_type, store_params: dict = None, verify=True) -> bool:
    start_t = datetime.now()
    path = path.rstrip('/')

    logger.info(f'Deleting backup at {path}')

    if verify:
        try:
            # Check that the path we're deleting is actually Zarr data
            if not path.endswith('.tar'):
                open_zarr_group(path, store_type, store_params, root=True)
            else:
                likely_zarr = False
                with tarfile.open(path, 'r') as tar:
                    for f in tar.getnames():
                        if basename(f) == '.zgroup':
                            likely_zarr = True
                            break
                if not likely_zarr:
                    logger.error(f'Backup archive file at {path} does not appear to be a Zarr group')
                    return False
        except Exception as e:
            logger.warning(f'Zarr group at path {path} could not be opened. Maybe it does not exist. Error: {repr(e)}')
            return False
    else:
        logger.info('Skipping verification for Zarr backup. Should only do this if we\'re certain the provided path '
                    'is a backup')

    if store_type == 'local':
        try:
            path = urlparse(path).path.rstrip('/')

            if not path.endswith('.tar'):
                logger.debug(f'Recursively deleting {path}')
                rmtree(path)
            else:
                logger.debug(f'Deleting {path}')
                os.remove(path)

            logger.info(f'Removed backup at {path} in {datetime.now() - start_t}')
        except Exception as e:
            logger.exception(e)
            logger.info(f'Failed to remove backup at {path} in {datetime.now() - start_t}')
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

            logger.info(f'Removed backup at {path} in {datetime.now() - start_t}')
        except Exception as e:
            logger.exception(e)
            logger.info(f'Failed to remove backup at {path} in {datetime.now() - start_t}')
            return False
    else:
        raise ValueError(f'Invalid store type: {store_type}')

    return True
