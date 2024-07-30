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
import os.path
from datetime import datetime
from itertools import chain
from tempfile import TemporaryDirectory
from typing import Dict, Tuple, Any
from urllib.parse import urlparse

import numpy as np
import s3fs
import xarray as xr
import zarr

from sam_extract.writers import Writer
from sam_extract.writers.Writer import FIXED_ATTRIBUTES
from xarray import Dataset

logger = logging.getLogger(__name__)

# TEMPORARY: If installed xr module is a version built from pydata/xarray#8016
TEMP_XARRAY_8016 = tuple([int(n) for n in xr.__version__.split('.')[:3]]) >= (2023, 8, 0)

APPEND_WARNING = [
    '',
    ' ******************************************* WARNING *******************************************',
    ' **                                                                                           **',
    ' ** APPENDED-TO ZARR ARRAY WILL HAVE -ALL- CHUNKS (EVEN EMPTY ONES) WRITTEN FOR NEW SLICES!!! **',
    ' **                             xarray: (Issue #8009 | PR # 8016)                             **',
    ' **                                                                                           **',
    ' ***********************************************************************************************',
    '',
]

ISO_8601 = "%Y-%m-%dT%H:%M:%S%zZ"
TIME_CHUNKING = (4000,)  # 3650 days in OCO-3's 10-year nominal mission, rounded up to nearest thousand

ENCODINGS: Dict[str, Dict[str, Dict[str, Any]]] = {}


class ZarrWriter(Writer):
    def __init__(self,
                 path: str,
                 chunking: Tuple[int, int, int],
                 overwrite: bool = False,
                 append_dim: str = 'time',
                 **kwargs):
        Writer.__init__(self, path, overwrite, **kwargs)
        self.__chunking = chunking
        self.__append_dim = append_dim
        self.__correct_ct = None  # Used in the event that the outputted array needs to be corrected resetting the
        # date_created attr because overwrite is set to True

        self.__verify = False if 'verify' not in kwargs else kwargs['verify']

    @staticmethod
    def open_zarr_group(path, store_type, params, root=False, **xr_open_kwargs) -> Dict[str, Dataset]:
        logger.info(f'Opening Zarr array group at {path}')

        if store_type == 'local':
            store = path
        elif store_type == 's3':
            url = urlparse(path)

            bucket = url.netloc
            key = url.path

            if params.get('public', False):
                store = f'https://{bucket}.s3.{params["region"]}.amazonaws.com{key}'  # key has leading /
            else:
                s3 = s3fs.S3FileSystem(
                    False,
                    key=params['auth']['accessKeyID'],
                    secret=params['auth']['secretAccessKey'],
                    client_kwargs=dict(region_name=params["region"])
                )
                store = s3fs.S3Map(root=path, s3=s3, check=False)
        else:
            raise ValueError(store_type)

        if root:
            return {
                '/': xr.open_zarr(store, consolidated=True, **xr_open_kwargs),
            }
        else:
            groups = {'/': xr.open_zarr(store, consolidated=True, mask_and_scale=True, **xr_open_kwargs)}

            for group in Writer.GROUP_KEYS:
                if group == '/':
                    continue

                try:
                    groups[group] = xr.open_zarr(
                        store, group=group[1:], consolidated=True, mask_and_scale=True, **xr_open_kwargs
                    )
                except:
                    pass

            return groups

    def write(
            self,
            ds: Dict[str, Dataset],
            attrs: Dict[str, str] | None = None,
            skip_verification=False,
            global_product=True,
            mission=None
    ):
        logger.info(f'Writing dataset group to Zarr array at {self.path}')

        if attrs is None:
            attrs = {}

        if not TEMP_XARRAY_8016:
            logger.warning('Currently installed version of xarray does not support write_empty_chunks')
        else:
            logger.debug('Currently installed version of xarray supports write_empty_chunks')

        exists = self.exists()
        dynamic_attrs = None
        now = datetime.utcnow().strftime(ISO_8601)

        if exists and self.overwrite:
            logger.warning('File already exists and will be overwritten')

            if self.final:
                dynamic_attrs = dict(
                    date_created=now if self.__correct_ct is None else self.__correct_ct,
                    date_updated=now,
                    coverage_start=np.min(ds['/'].time.values.astype('datetime64[s]')).item().strftime(ISO_8601),
                    coverage_end=np.max(ds['/'].time.values.astype('datetime64[s]')).item().strftime(ISO_8601)
                )
        elif exists and not self.overwrite:
            logger.info('Group exists and will be appended to')

            if self.final:
                logger.debug('Getting dynamic attributes from store')
                zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params, root=True)

                append_start = np.min(ds['/'].time.values.astype('datetime64[s]')).item().strftime(ISO_8601)
                append_end = np.max(ds['/'].time.values.astype('datetime64[s]')).item().strftime(ISO_8601)

                existing_start = zarr_group['/'].attrs.get('coverage_start', append_start)
                existing_end = zarr_group['/'].attrs.get('coverage_end', append_end)

                coverage_start = append_start if append_start <= existing_start else existing_start
                coverage_end = append_end if append_end >= existing_end else existing_end

                dynamic_attrs = dict(
                    date_created=zarr_group['/'].attrs.get('date_created', now),
                    date_updated=now,
                    coverage_start=coverage_start,
                    coverage_end=coverage_end,
                )

        else:
            logger.info('Group does not exist so it will be created.')

            if self.final:
                dynamic_attrs = dict(
                    date_created=now,
                    date_updated=now,
                    coverage_start=np.min(ds['/'].time.values.astype('datetime64[s]')).item().strftime(ISO_8601),
                    coverage_end=np.max(ds['/'].time.values.astype('datetime64[s]')).item().strftime(ISO_8601)
                )

        if self.final:
            if global_product:
                fixed = FIXED_ATTRIBUTES['global']
            else:
                fixed = FIXED_ATTRIBUTES['local'][mission]

            attributes = dict(chain(
                attrs.items(),
                fixed.items(),
                dynamic_attrs.items()
            ))

            ds['/'] = ds['/'].assign_attrs(attributes)

        mode = 'w' if self.overwrite else None
        append_dim = self.__append_dim if (not self.overwrite) and exists else None

        compressor = zarr.Blosc(cname='blosclz', clevel=9)

        if self.overwrite or not exists:
            encodings = {group: {
                vname: {
                    'compressor': compressor,
                    'chunks': self.__chunking,
                    # 'write_empty_chunks': False
                } for vname in ds[group].data_vars
            } for group in ds}

            for group in ds:
                encodings[group]['time'] = {'chunks': TIME_CHUNKING}

            for group in encodings:
                if group in ENCODINGS:
                    for var in encodings[group]:
                        if var in ENCODINGS[group]:
                            encodings[group][var].update(ENCODINGS[group][var])

            if not TEMP_XARRAY_8016:
                for grp in ds:
                    for vname in ds[grp].data_vars:
                        encodings[grp][vname]['write_empty_chunks'] = False
        else:
            if not TEMP_XARRAY_8016:
                for ln in APPEND_WARNING:
                    logger.warning(ln)

            encodings = {group: None for group in Writer.GROUP_KEYS}

        logger.debug(f'Setting Zarr chunk shapes: {self.__chunking}')

        for group in ds:
            for var in ds[group].data_vars:
                ds[group][var] = ds[group][var].chunk(self.__chunking)

            ds[group]['time'].chunk(TIME_CHUNKING)

        logger.debug('Outputting Zarr array')

        if self.store == 'local':
            ds_store = self.path
        else:
            s3 = s3fs.S3FileSystem(
                False,
                key=self.store_params['auth']['accessKeyID'],
                secret=self.store_params['auth']['secretAccessKey'],
                client_kwargs=dict(region_name=self.store_params["region"])
            )
            ds_store = s3fs.S3Map(root=self.path, s3=s3, check=False)

        for group in Writer.GROUP_KEYS:
            if group not in ds:
                continue

            cdf_group = group[1:]

            if TEMP_XARRAY_8016:
                if cdf_group == '':
                    ds[group].to_zarr(
                        ds_store,
                        mode=mode,
                        append_dim=append_dim,
                        encoding=encodings[group],
                        write_empty_chunks=False,
                        consolidated=True,
                    )
                else:
                    ds[group].to_zarr(
                        ds_store,
                        mode=mode,
                        group=cdf_group,
                        append_dim=append_dim,
                        encoding=encodings[group],
                        write_empty_chunks=False,
                        consolidated=True,
                    )
            else:
                if cdf_group == '':
                    ds[group].to_zarr(
                        ds_store,
                        mode=mode,
                        append_dim=append_dim,
                        encoding=encodings[group],
                        consolidated=True,
                    )
                else:
                    ds[group].to_zarr(
                        ds_store,
                        mode=mode,
                        group=cdf_group,
                        append_dim=append_dim,
                        encoding=encodings[group],
                        consolidated=True,
                    )

        logger.info(f'Finished writing Zarr array to {self.path}')

        if self.__verify and not self.overwrite and not skip_verification:
            good = True

            zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params, root=True)
            dim = zarr_group['/'][self.__append_dim].to_numpy()

            # Note: in this section, we need to specify decode_times=False when opening the group for modification
            # otherwise xarray messes with the units which may render the Zarr stores unusable to this program later on

            if not all(np.diff(dim).astype(int) >= 0):
                logger.warning('Appended Zarr array not monotonically increasing along append dimension. '
                               'It will need to be sorted')

                zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params, decode_times=False)

                for key in Writer.GROUP_KEYS:
                    if key not in zarr_group:
                        continue

                    zarr_group[key] = zarr_group[key].sortby(self.__append_dim)

                good = False
            else:
                logger.debug('Appended zarr array is monotonically increasing along append dimension')

            dim = zarr_group['/'][self.__append_dim].to_numpy()

            if any(np.diff(dim).astype(int) == 0):
                logger.warning('Appended Zarr array has repeated slices along the append dimension. '
                               'They will be removed')

                # If we've already fully opened and modified the zarr group, don't reopen it
                if good:
                    zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params, decode_times=False)

                prev = None
                drop = []

                for i, v in enumerate(dim.astype(int)):
                    if v == prev:
                        drop.append(i - 1)

                    prev = v

                logger.info(f'Dropping {len(drop)} duplicate slices at indices: {drop}')

                for key in Writer.GROUP_KEYS:
                    if key not in zarr_group:
                        continue

                    zarr_group[key] = zarr_group[key].drop_duplicates(dim=self.__append_dim, keep='first')

                good = False
            else:
                logger.debug('Appended zarr array contains no duplicate slices along append dimension')

            if not good:
                logger.info('Writing corrected group')

                with TemporaryDirectory(prefix='oco-sam-extract-', suffix='-zarr-scratch-corrected',
                                        ignore_cleanup_errors=True) as td:
                    from sam_extract.utils import ProgressLogging

                    temp_path = os.path.join(td, 'sorted.zarr')

                    writer = ZarrWriter(temp_path, self.__chunking, overwrite=True, verify=False)
                    with ProgressLogging(log_level=logging.INFO, interval=10):
                        writer.write(zarr_group)

                    corrected_group = ZarrWriter.open_zarr_group(temp_path, 'local', None, decode_times=False)

                    self.overwrite = True
                    self.__correct_ct = corrected_group['/'].attrs.get('date_created', None)

                    with ProgressLogging(log_level=logging.INFO, interval=10):
                        self.write(corrected_group)
            else:
                logger.info('Appended Zarr array looks good')
