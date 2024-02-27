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

import os

PW_STATE_DIR = os.getenv('ZARR_BACKUP_DIR', '/tmp/oco_pipeline_zarr_state')
PW_STATE_FILE_NAME = 'ZARR_WRITE.json'

from os.path import join

ZARR_REPAIR_FILE = join(PW_STATE_DIR, PW_STATE_FILE_NAME)

from sam_extract.utils.ZarrUtils import AWSConfig
from sam_extract.utils.ZarrUtils import create_backup as backup_zarr
from sam_extract.utils.ZarrUtils import delete_backup as delete_zarr_backup
