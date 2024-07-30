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
import random
from abc import ABC, abstractmethod
from datetime import datetime
from os.path import basename, join
from typing import Tuple, List, Optional, Dict, Type, Literal

import xarray as xr
from sam_extract.runconfig import RunConfig
from sam_extract.writers import ZarrWriter

logger = logging.getLogger(__name__)


class Processor(ABC):
    DEFAULT_INTERPOLATE_METHOD = 'cubic'

    @classmethod
    @abstractmethod
    def process_input(
            cls,
            input_file,
            cfg: RunConfig,
            temp_dir,
            output_pre_qf=True,
            exclude_groups: Optional[List[str]] = None
    ) -> Tuple[Optional[Dict[str, xr.Dataset]], Optional[Dict[str, xr.Dataset]], bool, str]:
        ...

    @staticmethod
    @abstractmethod
    def _empty_dataset(date: datetime, cfg: RunConfig) -> Dict[str, xr.Dataset]:
        ...

    @classmethod
    def empty_dataset(cls, date: datetime, cfg: RunConfig, temp_dir=None) -> Dict[str, xr.Dataset]:
        logger.info(f'Creating empty day of data for {cls}')

        if temp_dir is None:
            return cls._empty_dataset(date, cfg)
        else:
            return Processor.empty_ds_to_zarr(
                cls._empty_dataset(date, cfg),
                cfg,
                temp_dir
            )

    @staticmethod
    def empty_ds_to_zarr(ds: Dict[str, xr.Dataset], cfg: RunConfig, temp_dir: str) -> Dict[str, xr.Dataset]:
        random_string = ''.join(
            random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=12)
        )
        path = join(temp_dir, 'empty', f'{random_string}.zarr')

        chunking: Tuple[int, int, int] = cfg.chunking

        writer = ZarrWriter(path, chunking, overwrite=True, verify=False)
        writer.write(ds)

        return ZarrWriter.open_zarr_group(path, 'local', None)

    @staticmethod
    def granule_to_dt(granule_name: str) -> datetime:
        return datetime.strptime(
            basename(granule_name).split('_')[2],  # Location of date info in oco2/3 lite file names
            "%y%m%d"
        )


PROCESSORS: Dict[Literal['local', 'global'], Dict[str, Type[Processor]]] = {'local': {}, 'global': {}}
