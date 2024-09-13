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

    try:
        logger.info('Trying to purge the staging dir if it exists...')
        rmtree('/mnt/transform/inputs/')
        Path('/mnt/transform/inputs/').mkdir(parents=True, exist_ok=True)
    except:
        pass

    logger.info('Trying to remove leftover runtime files if any exist')

    mount_root_dir = '/mnt/transform'

    runtime_temp_files = [
        'cmr-results.json', 'cmr-results-oco2.json', 'pipeline-rc.yaml', 'staging.json'
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

    logger.info('Trying to remove state file if it exists')

    is_global = os.environ.get('GLOBAL', 'false').lower() == 'true'
    state_file_name = "state_global.json" if is_global else 'state.json'

    try:
        os.remove(os.path.join(mount_root_dir, state_file_name))
    except:
        pass

    logger.info('done')

    return 0
