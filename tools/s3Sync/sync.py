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
import pathlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Tuple, List
from urllib.parse import urlparse

import boto3
from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader as Loader


logging.basicConfig(
    level=logging.DEBUG if os.getenv('VERBOSE') is not None else logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] [%(name)s::%(lineno)d] %(message)s'
)

logger = logging.getLogger('sync')

SUPPRESS = [
    'botocore',
    's3transfer',
    'urllib3',
]

for logger_name in SUPPRESS:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

LOCAL_ROOT = os.getenv('LOCAL_ROOT')
S3_ROOT_PREFIX = os.getenv('S3_ROOT_PREFIX')
S3_BUCKET = os.getenv('S3_BUCKET')
PRE_QF_NAME = os.getenv('PRE_QF_NAME')
POST_QF_NAME = os.getenv('POST_QF_NAME')
DRYRUN = os.getenv('DRYRUN') is not None

RC_FILE_OVERRIDE = os.getenv('RC_FILE_OVERRIDE')

if RC_FILE_OVERRIDE is not None and os.path.exists(RC_FILE_OVERRIDE):
    with open(RC_FILE_OVERRIDE) as fp:
        config = load(fp, Loader=Loader)

    try:
        out_cfg = config['output']

        if 's3' in out_cfg:
            logger.info('S3 is already the write target. FS sync not necessary')
            exit(0)

        LOCAL_ROOT = urlparse(out_cfg['local']).path
        PRE_QF_NAME = out_cfg['naming']['pre_qf']
        POST_QF_NAME = out_cfg['naming']['post_qf']
    except Exception as e:
        logger.error('Failed to load specified RC file. Will use env instead')
        logger.exception(e)


def plan(local_dir, bucket, prefix, s3) -> Tuple[List[Tuple[str, str]], List[str]]:
    to_upload: List[Tuple[str, str]] = []  # list map key -> file abs path to upload
    to_delete: List[str] = []

    logger.info(f'Listing S3 bucket "{bucket}" with prefix "{prefix}"')

    s3_list = []

    paginator = s3.get_paginator('list_objects_v2')
    list_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in list_iterator:
        s3_list.extend(page.get('Contents', []))

    def strip_prefix(s: str, prefix: str):
        s = s.removeprefix(prefix).lstrip('/')
        return s

    s3_dict = {strip_prefix(o['Key'], prefix): o for o in s3_list}

    if len(s3_list) != len(s3_dict):
        logger.error(f'S3 list and prefix dictionary are of differing lengths ({len(s3_list)}, {len(s3_dict)}). This'
                     f'should not happen!')
        exit(1)

    local_list = [strip_prefix(os.path.join(dp, f), local_dir) for dp, dn, filenames in os.walk(local_dir) for f in filenames]

    for zarr_key in local_list:
        file_abs_path = os.path.join(local_dir, zarr_key)

        if zarr_key not in s3_dict:  # Brand new Zarr object, must push
            logger.debug(f'Scheduling upload of NEW Zarr object {file_abs_path} to '
                         f's3://{bucket}/{os.path.join(prefix, zarr_key)}')
            to_upload.append((os.path.join(prefix, zarr_key), file_abs_path))
        else:
            s3_obj = s3_dict[zarr_key]

            diff_size = s3_obj['Size'] != os.path.getsize(file_abs_path)
            mod_after = s3_obj['LastModified'] < datetime.fromtimestamp(pathlib.Path(file_abs_path).stat().st_mtime,
                                                                        tz=timezone.utc)

            if diff_size and mod_after:
                logger.debug(f'Scheduling upload of UPDATED Zarr object {file_abs_path} to '
                             f's3://{bucket}/{os.path.join(prefix, zarr_key)} (Reason: diff size & later mod time)')
                updated = True
            elif diff_size:
                logger.debug(f'Scheduling upload of UPDATED Zarr object {file_abs_path} to '
                             f's3://{bucket}/{os.path.join(prefix, zarr_key)} (Reason: diff size)')
                updated = True
            elif mod_after:
                logger.debug(f'Scheduling upload of UPDATED Zarr object {file_abs_path} to '
                             f's3://{bucket}/{os.path.join(prefix, zarr_key)} (Reason: later mod time)')
                updated = True
            else:
                logger.debug(f'Skipping UNCHANGED Zarr object {file_abs_path}')
                updated = False

            if updated:
                to_upload.append((s3_obj['Key'], file_abs_path))

            del s3_dict[zarr_key]

    for zarr_key in s3_dict:  # Remaining keys. Schedule for deletion if they're not dir objects (zero len, end in '/')
        if s3_dict[zarr_key]['Size'] == 0 and s3_dict[zarr_key]['Key'][-1] == '/':
            continue
        else:
            logger.debug(f'Scheduling deletion of S3 object s3://{bucket}/{os.path.join(prefix, zarr_key)} as it is '
                         f'absent from local fs and not apparently a directory object')
            to_delete.append(s3_dict[zarr_key]['Key'])

    return to_upload, to_delete


def main():
    if any([v is None for v in [LOCAL_ROOT, S3_ROOT_PREFIX, S3_BUCKET, PRE_QF_NAME, POST_QF_NAME]]):
        logger.error('Not all required params are set')
        return 1

    if not os.path.exists(LOCAL_ROOT):
        logger.info(f'Local dir "{LOCAL_ROOT}" does not exist')
        return 2

    s3 = boto3.client('s3')

    to_upload: List[Tuple[str, str]] = []  # list map key -> file abs path to upload
    to_delete: List[str] = []

    pre_qf_upload, pre_qf_delete = plan(
        os.path.join(LOCAL_ROOT, PRE_QF_NAME),
        S3_BUCKET,
        f'{S3_ROOT_PREFIX.strip("/")}/{PRE_QF_NAME}',
        s3
    )

    to_upload.extend(pre_qf_upload)
    to_delete.extend(pre_qf_delete)

    post_qf_upload, post_qf_delete = plan(
        os.path.join(LOCAL_ROOT, POST_QF_NAME),
        S3_BUCKET,
        f'{S3_ROOT_PREFIX.strip("/")}/{POST_QF_NAME}',
        s3
    )

    to_upload.extend(post_qf_upload)
    to_delete.extend(post_qf_delete)

    logger.info(f'Found {len(to_upload):,} new or modified Zarr objects to upload to S3')
    logger.info(f'Found {len(to_delete):,} missing objects to delete from S3')

    if DRYRUN:
        logger.warning('DRYRUN ENABLED FILES WILL NOT BE UPLOADED OR DELETED BUT WILL BE LOGGED')

    def s3_copy(key_src_pair):
        key, src_file = key_src_pair

        logger.log(
            logging.DEBUG,
            f'{src_file} -> s3://{S3_BUCKET}/{key}'
        )

        if not DRYRUN:
            s3.upload_file(src_file, S3_BUCKET, key)

    with ThreadPoolExecutor(thread_name_prefix='s3_copy_worker') as pool:
        futures = []
        for p in to_upload:
            futures.append(pool.submit(s3_copy, p))

        for f in futures:
            f.result()

    if len(to_upload) > 0:
        logger.info('Uploads complete')

    to_delete_dicts = [dict(Key=k) for k in to_delete]
    batches = [to_delete_dicts[i:i + 1000] for i in range(0, len(to_delete_dicts), 1000)]

    for batch in batches:
        logger.info(f'Deleting {len(batch):,} objects')

        if not DRYRUN:
            resp = s3.delete_objects(Bucket=S3_BUCKET, Delete=dict(Objects=batch))

            if len(resp['Deleted']) != len(batch):
                logger.error(f'{len(resp["Errors"]):,} objects could not be deleted')

                retries = 3

                while len(resp["Errors"]) > 0 and retries > 0:
                    logger.info(f'Retrying {len(resp["Errors"])} objects')
                    resp = s3.delete_objects(
                        Bucket=S3_BUCKET,
                        Delete=dict(Objects=[dict(Key=e['Key']) for e in resp['Errors']])
                    )
        else:
            for k in batch:
                logger.info(f'(dryrun) Delete s3://{S3_BUCKET}/{k["Key"]}')

    if len(to_delete) > 0:
        logger.info('Deletions complete')

    logger.info('Sync complete')

    return 0


if __name__ == '__main__':
    ret = -1
    try:
        ret = main()
    except Exception as e:
        logger.error('An uncaught error occurred')
        logger.exception(e)
    finally:
        exit(ret)
