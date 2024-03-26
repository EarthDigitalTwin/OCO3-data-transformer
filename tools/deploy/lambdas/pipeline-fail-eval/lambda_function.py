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


def lambda_handler(event, context):
    backup_file_path = os.getenv('ZARR_BACKUP_FILE')

    if backup_file_path is not None and os.path.exists(backup_file_path):
        fail_type = 'CRITICAL'
    else:
        fail_type = 'GENERAL'

    return dict(FailureType=fail_type)
