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
import boto3
import os
import sys

from pathlib import Path
from shutil import rmtree


def lambda_handler(event, context):
    root_logger = logging.getLogger()
    for each in root_logger.handlers:
        root_logger.removeHandler(each)

    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s')
    log_level = logging.INFO

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(log_formatter)
    stream_handler.setLevel(log_level)

    logger = logging.getLogger('exec_init')
    logger.setLevel(log_level)
    logger.addHandler(stream_handler)

    s3 = boto3.client('s3')

    bucket = os.getenv('BUCKET')
    key = os.getenv('RCT_KEY')

    logger.info('Copying RC template file')

    s3.download_file(bucket, key, '/mnt/transform/run-config.yaml')
    target_key = os.getenv('TARGET_KEY')

    if target_key is not None:
        logger.info('Copying OCO-3 target JSON file')
        s3.download_file(bucket, target_key, '/mnt/transform/targets.json')

    target_key_oco2 = os.getenv('TARGET_KEY_OCO2')

    if target_key_oco2 is not None:
        logger.info('Copying OCO-2 target JSON file')
        s3.download_file(bucket, target_key_oco2, '/mnt/transform/targets_oco2.json')

    gap_file = os.getenv('GAP_FILE')

    if gap_file is not None:
        logger.info('Copying gap file')
        s3.download_file(bucket, gap_file, '/mnt/transform/gaps.json')

    try:
        logger.info('Trying to purge the staging dir if it exists...')
        rmtree('/mnt/transform/inputs/')
        Path('/mnt/transform/inputs/').mkdir(parents=True, exist_ok=True)
    except:
        pass

    logger.info('Trying to remove leftover runtime files if any exist')

    mount_root_dir = '/mnt/transform'

    runtime_temp_files = [
        'cmr-results.json',
        'cmr-results-oco2.json',
        'cmr-results-oco3_sif.json',
        'pipeline-rc.yaml',
        'staging.json'
    ]

    for f in runtime_temp_files:
        try:
            os.remove(os.path.join(mount_root_dir, f))
        except:
            pass

    logger.info('Trying to remove Zarr backup file if it exists')

    try:
        os.remove(os.environ['ZARR_BACKUP_FILE'])
    except:
        pass

    logger.info('done')

    return 0
