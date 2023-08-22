import logging
import os.path
from datetime import datetime
from itertools import chain
from tempfile import TemporaryDirectory
from typing import Dict, Tuple
from urllib.parse import urlparse

import numpy as np
import s3fs
import xarray as xr
import zarr
from xarray import Dataset

from sam_extract.writers import Writer
from sam_extract.writers.Writer import FIXED_ATTRIBUTES
from sam_extract.targets import FILL_VALUE as TARGET_FILL

logger = logging.getLogger(__name__)

# TEMPORARY: If installed xr module is a version built from pydata/xarray#8016
TEMP_XARRAY_8016 = tuple([int(n) for n in xr.__version__.split('.')[:3]]) >= (2023, 8, 1)

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
    def open_zarr_group(path, store_type, params, root=False) -> Dict[str, Dataset]:
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
                '/': xr.open_zarr(store, consolidated=True),
            }
        else:
            groups = {'/': xr.open_zarr(store, consolidated=True, mask_and_scale=True)}

            for group in Writer.GROUP_KEYS:
                if group == '/':
                    continue

                try:
                    groups[group] = xr.open_zarr(store, group=group[1:], consolidated=True, mask_and_scale=True)
                except:
                    pass

            return groups

    def write(self, ds: Dict[str, Dataset], attrs: Dict[str, str] | None = None):
        logger.info(f'Writing SAM group to Zarr array at {self.path}')

        if attrs is None:
            attrs = {}

        if not TEMP_XARRAY_8016:
            logger.warning('Currently installed version of xarray does not support write_empty_chunks')

        exists = self._exists()
        dynamic_attrs = None
        now = datetime.utcnow().strftime(ISO_8601)

        if exists and self.overwrite:
            logger.warning('File already exists and will be overwritten')

            if self.final:
                dynamic_attrs = dict(
                    date_created=now if self.__correct_ct is None else self.__correct_ct,
                    date_updated=now,
                    coverage_start=datetime.fromtimestamp(
                        ds['/'].time.min().astype(int).item() / 1e9).strftime(ISO_8601),
                    coverage_end=datetime.fromtimestamp(
                        ds['/'].time.max().astype(int).item() / 1e9).strftime(ISO_8601)
                )
        elif exists and not self.overwrite:
            logger.info('File exists and will be appended to')

            if self.final:
                logger.info('Getting dynamic attributes from store')
                zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params, root=True)

                append_start = datetime.fromtimestamp(ds['/'].time.min().astype(int).item() / 1e9).strftime(ISO_8601)
                append_end = datetime.fromtimestamp(ds['/'].time.max().astype(int).item() / 1e9).strftime(ISO_8601)

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
            logger.debug('Array does not exist so it will be created.')

            if self.final:
                dynamic_attrs = dict(
                    date_created=now,
                    date_updated=now,
                    coverage_start=datetime.fromtimestamp(
                        ds['/'].time.min().astype(int).item() / 1e9).strftime(ISO_8601),
                    coverage_end=datetime.fromtimestamp(
                        ds['/'].time.max().astype(int).item() / 1e9).strftime(ISO_8601)
                )

        if self.final:
            attributes = dict(chain(
                attrs.items(),
                FIXED_ATTRIBUTES.items(),
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

            encodings['/']['target_id']['_FillValue'] = TARGET_FILL
            encodings['/']['target_type']['_FillValue'] = TARGET_FILL

            if not TEMP_XARRAY_8016:
                for grp in ds:
                    for vname in ds[grp].data_vars:
                        encodings[grp][vname]['write_empty_chunks'] = False
        else:
            if not TEMP_XARRAY_8016:
                # TODO: Is there a way to ensure write_empty_chunks=false when appending to existing zarr groups?
                #  (continued) It cannot be done here and xarray doesn't preserve its value
                #  (continued)  https://github.com/pydata/xarray/issues/8009
                #  (continued) Fixed in https://github.com/pydata/xarray/pull/8016
                #  (continued) Awaiting new release
                for ln in APPEND_WARNING:
                    logger.warning(ln)

            encodings = {group: None for group in Writer.GROUP_KEYS}

        logger.info(f'Setting Zarr chunk shapes: {self.__chunking}')

        for group in ds:
            for var in ds[group].data_vars:
                ds[group][var] = ds[group][var].chunk(self.__chunking)

        logger.info('Outputting Zarr array')

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

        if self.__verify and not self.overwrite:
            logger.info('Verifying written Zarr array')

            good = True

            zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params, root=True)
            dim = zarr_group['/'][self.__append_dim].to_numpy()

            if not all(np.diff(dim).astype(int) >= 0):
                logger.warning('Appended Zarr array not monotonically increasing along append dimension. '
                               'It will need to be sorted')

                zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params)

                for key in Writer.GROUP_KEYS:
                    if key not in zarr_group:
                        continue

                    zarr_group[key] = zarr_group[key].sortby(self.__append_dim)

                good = False
            else:
                logger.info('Appended zarr array is monotonically increasing along append dimension')

            dim = zarr_group['/'][self.__append_dim].to_numpy()

            if any(np.diff(dim).astype(int) == 0):
                logger.warning('Appended Zarr array has repeated slices along the append dimension. '
                               'They will be removed')

                # If we've already fully opened and modified the zarr group, don't reopen it
                if good:
                    zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params)

                prev = None
                drop = []

                for i, v in enumerate(dim.astype(int)):
                    if v == prev:
                        drop.append(i - 1)

                    prev = v

                logger.info(f'Dropping {len(drop)} duplicate slices')
                logger.debug(f'Dropping slices at indices: {drop}')

                for key in Writer.GROUP_KEYS:
                    if key not in zarr_group:
                        continue

                    zarr_group[key] = zarr_group[key].drop_duplicates(dim=self.__append_dim, keep='first')

                good = False
            else:
                logger.info('Appended zarr array contains no duplicate slices along append dimension')

            if not good:
                logger.info('Writing corrected group')

                with TemporaryDirectory(prefix='oco-sam-extract-', suffix='-zarr-scratch-corrected',
                                        ignore_cleanup_errors=True) as td:
                    temp_path = os.path.join(td, 'sorted.zarr')

                    writer = ZarrWriter(temp_path, self.__chunking, overwrite=True, verify=False)
                    writer.write(zarr_group)

                    corrected_group = ZarrWriter.open_zarr_group(temp_path, 'local', None)

                    self.overwrite = True
                    self.__correct_ct = corrected_group['/'].attrs.get('date_created', None)

                    self.write(corrected_group)
            else:
                logger.info('Appended Zarr array looks good')
