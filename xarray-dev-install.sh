#!/bin/bash

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

# This script will install the dev version of xarray that is proposed to be merged in pydata/xarray#8016.
# This should only be used until #8016 is merged and the changes released, in which case the project's
# conda-requirements should be updated to use the official release containing these changes

cd /

echo 'Cloning pydata/xarray repository'

# git clone --branch write-empty-chunks --single-branch https://github.com/RKuttruff/xarray.git
git clone --progress --verbose --branch main --single-branch https://github.com/pydata/xarray.git

cd xarray

echo 'Installing main branch version of xarray'

export SETUPTOOLS_SCM_PRETEND_VERSION=2023.08.01-dev
pip install .

echo 'Deleting .git directory'

rm -rf .git

cd /

echo 'Done'
