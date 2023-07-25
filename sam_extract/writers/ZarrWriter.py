import logging
from typing import Dict, Tuple
from urllib.parse import urlparse

import numpy as np
import s3fs
import xarray as xr
import zarr
from xarray import Dataset

from sam_extract.writers import Writer

logger = logging.getLogger(__name__)


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
                    secret=params['auth']['secretAccessKey']
                )
                store = s3fs.S3Map(root=path, s3=s3, check=False)
        else:
            raise ValueError(store_type)

        if root:
            return {
                '/': xr.open_zarr(store, consolidated=True),
            }
        else:
            groups = {'/': xr.open_zarr(store, consolidated=True)}

            for group in Writer.GROUP_KEYS:
                if group == '/':
                    continue

                try:
                    groups[group] = xr.open_zarr(store, group=group[1:], consolidated=True)
                except:
                    pass

            return groups

    def write(self, ds: Dict[str, Dataset]):
        logger.info(f'Writing SAM group to Zarr array at {self.path}')

        exists = self._exists()

        if exists and self.overwrite:
            logger.warning('File already exists and will be overwritten')
        elif exists and not self.overwrite:
            logger.info('File exists and will be appended to')
        else:
            logger.debug('Array does not exist so it will be created.')

        mode = 'w' if self.overwrite else None
        append_dim = self.__append_dim if (not self.overwrite) and exists else None

        compressor = zarr.Blosc(cname='blosclz', clevel=9)

        if self.overwrite or not exists:
            encodings = {group: {
                vname: {
                    'compressor': compressor,
                    'chunks': self.__chunking,
                    'write_empty_chunks': False
                } for vname in ds[group].data_vars
            } for group in ds}
        else:
            # TODO: Is there a way to ensure write_empty_chunks=false when appending to existing zarr groups?
            # TODO: (continued) It cannot be done here and xarray doesn't preserve its value
            # TODO: (continued)  https://github.com/pydata/xarray/issues/8009
            # TODO: (continued) Pending fix in https://github.com/pydata/xarray/pull/8016
            logger.warning('')
            logger.warning(' ******************************************* WARNING *******************************************')
            logger.warning(' **                                                                                           **')
            logger.warning(' ** APPENDED-TO ZARR ARRAY WILL HAVE -ALL- CHUNKS (EVEN EMPTY ONES) WRITTEN FOR NEW SLICES!!! **')
            logger.warning(' **                                                                                           **')
            logger.warning(' ***********************************************************************************************')
            logger.warning('')
            
            encodings = {group: None for group in Writer.GROUP_KEYS}

        logger.info('Setting Zarr chunk shapes')

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
                secret=self.store_params['auth']['secretAccessKey']
            )
            ds_store = s3fs.S3Map(root=self.path, s3=s3, check=False)

        for group in Writer.GROUP_KEYS:
            if group not in ds:
                continue

            cdf_group = group[1:]

            if cdf_group == '':
                ds[group].to_zarr(ds_store, mode=mode, append_dim=append_dim, encoding=encodings[group])
            else:
                ds[group].to_zarr(ds_store, mode=mode, group=cdf_group, append_dim=append_dim,
                                  encoding=encodings[group])

        logger.info(f'Finished writing Zarr array to {self.path}')

        if self.__verify and not self.overwrite:
            logger.info('Verifying written Zarr array')

            zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params, root=True)
            dim = zarr_group['/'][self.__append_dim].to_numpy()

            if not all(np.diff(dim).astype(int) >= 0):
                logger.warning('Appended Zarr array not monotonically increasing along append dimension. '
                               'It will need to be sorted')

                zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params)

                self.overwrite = True

                for key in Writer.GROUP_KEYS:
                    if key not in zarr_group:
                        continue

                    zarr_group[key] = zarr_group[key].sortby(self.__append_dim)

                self.write(zarr_group)
            else:
                logger.info('Outputted array looks good')


