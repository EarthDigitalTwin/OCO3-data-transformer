# Copyright 2025 California Institute of Technology (Caltech)
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
from typing import Dict
from urllib.parse import urlparse

from sam_extract import GROUP_KEYS
from sam_extract.writers import Writer
from xarray import Dataset

logger = logging.getLogger(__name__)


class NetCDFWriter(Writer):
    def __init__(self, path: str, overwrite: bool = False, **kwargs):
        Writer.__init__(self, path, overwrite, **kwargs)

    def write(self, ds: Dict[str, Dataset]):
        logger.info(f'Writing SAM group as NetCDF at {self.path}')

        exists = self._exists()

        if exists and self.overwrite:
            logger.warning('File already exists and will be overwritten')
        elif exists and not self.overwrite:
            logger.error('File exists and cannot be overwritten')
            raise ValueError('File exists and cannot be overwritten')

        comp = {"zlib": True, "complevel": 9}
        encodings = {group: {vname: comp for vname in ds[group].data_vars} for group in ds}

        if self.store == 'local':
            local_path = urlparse(self.path).path

            for group in GROUP_KEYS:
                cdf_group = group[1:]

                if group not in ds:
                    continue

                if cdf_group == '':
                    if self.overwrite:
                        ds[group].to_netcdf(local_path, mode='w', encoding=encodings[group])
                    else:
                        ds[group].to_netcdf(local_path, mode='a', encoding=encodings[group])
                else:
                    ds[group].to_netcdf(local_path, mode='a', group=cdf_group, encoding=encodings[group])
        else:
            raise NotImplementedError()
