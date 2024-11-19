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

import numpy as np
import s3fs
import xarray as xr
import zarr
from sam_extract import GROUP_KEYS
from sam_extract.utils import open_zarr_group
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
"""
Array to hold initial encoding data for output array groups.
Top level: group name
Second level: variable name
Third level: encoding parameter

Note: DO NOT directly assign a top-level entry unless it does not exist, use update instead

Eg: 

ENCODINGS['/Group'] = {...}  # No!!!

--- 

if '/Group' not in ENCODINGS:
    ENCODINGS['/Group'] = {}
    
ENCODINGS['/Group'].update({...})  # Do this
"""

# List of attributes that cause errors if set on a data var when appending, if we're appending, we need to remove
# all of these
IMMUTABLE_ATTRS = [
    'missing_value',
    '_FillValue',
    'add_offset',
    'scale_factor'
]


class ZarrWriter(Writer):
    def __init__(self,
                 path: str,
                 chunking: Tuple[int, int, int],
                 overwrite: bool = False,
                 append_dim: str = 'time',
                 quiet=False,
                 **kwargs):
        Writer.__init__(self, path, overwrite, **kwargs)
        self.__chunking = chunking
        self.__append_dim = append_dim
        self.__correct_ct = None  # Used in the event that the outputted array needs to be corrected resetting the
        # date_created attr because overwrite is set to True

        self.__quiet = quiet

        self.__verify = False if 'verify' not in kwargs else kwargs['verify']

    @staticmethod
    def remove_immutable_attributes(group):
        for g in group:
            for var in group[g].data_vars:
                for attr in IMMUTABLE_ATTRS:
                    if attr in group[g][var].attrs:
                        del group[g][var].attrs[attr]

    def write(
            self,
            ds: Dict[str, Dataset],
            attrs: Dict[str, str] | None = None,
            skip_verification=False,
            global_product=True,
            mission=None
    ):
        if not self.__quiet:
            logger.info(f'Writing dataset group to Zarr array at {self.path}')
        else:
            logger.debug(f'Writing dataset group to Zarr array at {self.path}')

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
            if not self.__quiet:
                logger.info('Group exists and will be appended to')
            else:
                logger.debug('Group exists and will be appended to')

            if self.final:
                logger.debug('Getting dynamic attributes from store')
                zarr_group = open_zarr_group(self.path, self.store, self.store_params, root=True)

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

                self.remove_immutable_attributes(ds)

        else:
            if not self.__quiet:
                logger.info('Group does not exist so it will be created.')
            else:
                logger.debug('Group does not exist so it will be created.')

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
                fixed = FIXED_ATTRIBUTES['local'].get(mission, {})

                if mission not in FIXED_ATTRIBUTES['local']:
                    logger.warning(f'No fixed attributes found for {mission}. Please contact developers.')

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

            encodings = {group: None for group in GROUP_KEYS}

        logger.debug(f'Setting Zarr chunk shapes: {self.__chunking}')

        for group in ds:
            for var in ds[group].data_vars:
                ds[group][var] = ds[group][var].chunk(self.__chunking)

            ds[group]['time'].chunk(TIME_CHUNKING)

        if (self.final and exists and not self.overwrite and
                tuple([int(n) for n in xr.__version__.split('.')[:3]]) >= (2024, 10, 0)):
            logger.info('Aligning generated dataset to existing chunk boundaries')

            zarr_group = open_zarr_group(self.path, self.store, self.store_params, decode_times=False)

            for group in ds:
                existing_dataset = zarr_group[group]
                new_dataset = ds[group]

                if type(new_dataset.time.dtype) is type(np.dtype('datetime64[ns]')):
                    existing_dataset = xr.decode_cf(existing_dataset)

                attrs = new_dataset.attrs

                ds[group] = xr.concat(
                    [existing_dataset, new_dataset], dim='time', combine_attrs='drop'
                ).chunk(time=self.__chunking[0]).sel(time=slice(new_dataset.coords['time'][0], None))

                ds[group].attrs.update(attrs)

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

        for group in GROUP_KEYS:
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

        if not self.__quiet:
            logger.info(f'Finished writing Zarr array to {self.path}')
        else:
            logger.debug(f'Finished writing Zarr array to {self.path}')

        if self.__verify and not self.overwrite and not skip_verification:
            good = True

            zarr_group = open_zarr_group(self.path, self.store, self.store_params, root=True)
            dim = zarr_group['/'][self.__append_dim].to_numpy()

            # Note: in this section, we need to specify decode_times=False when opening the group for modification
            # otherwise xarray messes with the units which may render the Zarr stores unusable to this program later on

            if not all(np.diff(dim).astype(int) >= 0):
                logger.warning('Appended Zarr array not monotonically increasing along append dimension. '
                               'It will need to be sorted')

                zarr_group = open_zarr_group(self.path, self.store, self.store_params, decode_times=False)

                for key in GROUP_KEYS:
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
                    zarr_group = open_zarr_group(self.path, self.store, self.store_params, decode_times=False)

                prev = None
                drop = []

                for i, v in enumerate(dim.astype(int)):
                    if v == prev:
                        drop.append(i - 1)

                    prev = v

                logger.info(f'Dropping {len(drop)} duplicate slices at indices: {drop}')

                for key in GROUP_KEYS:
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

                    corrected_group = open_zarr_group(temp_path, 'local', None, decode_times=False)

                    self.overwrite = True
                    self.__correct_ct = corrected_group['/'].attrs.get('date_created', None)

                    with ProgressLogging(log_level=logging.INFO, interval=10):
                        self.write(corrected_group)
            else:
                if not self.__quiet:
                    logger.info('Appended Zarr array looks good')
                else:
                    logger.debug('Appended Zarr array looks good')
