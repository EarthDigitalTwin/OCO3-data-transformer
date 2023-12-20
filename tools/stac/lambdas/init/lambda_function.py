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
import boto3
import os
import sys

from pathlib import Path
from shutil import rmtree

def lambda_handler(event, context):
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s')
    log_level = logging.INFO
    log_handlers = []

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(log_formatter)
    stream_handler.setLevel(log_level)
    log_handlers.append(stream_handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s',
        handlers=log_handlers
    )

    logger = logging.getLogger('exec_init')

    s3 = boto3.client('s3')

    bucket = os.getenv('BUCKET')
    key = os.getenv('RCT_KEY')

    logger.info('Copying RC template file')

    s3.download_file(bucket, key, '/mnt/transform/run-config.yaml')

    try:
        logger.info('Trying to purge the staging dir if it exists...')
        rmtree('/mnt/transform/inputs/')
        Path('/mnt/transform/inputs/').mkdir(parents=True, exist_ok=True)
    except:
        pass

    logger.info('done')

    return 0
