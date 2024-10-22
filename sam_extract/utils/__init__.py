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

import os
import threading

from os.path import join

# TODO: There's a circular import issue somewhere in here, but it doesn't cause issues when running normally for some
#  reason. I don't currently have time to untangle this

from sam_extract.utils.DaskLogging import ProgressLogging
from sam_extract.utils.XI import get_xi, get_f_xi, cleanup_xi
from sam_extract.utils.ZarrUtils import AWSConfig
from sam_extract.utils.ZarrUtils import create_backup as backup_zarr
from sam_extract.utils.ZarrUtils import delete_backup as delete_zarr_backup
from sam_extract.utils.Progress import Progress

PW_STATE_DIR = os.getenv('ZARR_BACKUP_DIR', '/tmp/oco_pipeline_zarr_state')
PW_STATE_FILE_NAME = 'ZARR_WRITE.json'

ZARR_REPAIR_FILE = join(PW_STATE_DIR, PW_STATE_FILE_NAME)

INTERP_MAX_PARALLEL = max(int(os.environ.get('INTERP_MAX_PARALLEL', 2)), 1)
INTERP_SEMA = threading.BoundedSemaphore(value=INTERP_MAX_PARALLEL)
