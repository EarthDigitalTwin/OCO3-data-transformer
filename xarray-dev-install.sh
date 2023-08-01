#!/bin/bash

# This script will install the dev version of xarray that is proposed to be merged in pydata/xarray#8016.
# This should only be used until #8016 is merged and the changes released, in which case the project's
# conda-requirements should be updated to use the official release containing these changes

cd /

git clone --branch write-empty-chunks --single-branch https://github.com/RKuttruff/xarray.git

cd xarray

export SETUPTOOLS_SCM_PRETEND_VERSION=2023.08.01-dev
pip install .

cd /

