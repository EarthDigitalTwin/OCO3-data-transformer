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

            if params['public']:
                store = f'https://{bucket}.s3.{params["region"]}.amazonaws.com{key}'  # key has leading /
            else:
                s3 = s3fs.S3FileSystem(False, )  # TODO: auth here
                store = s3fs.S3Map(root=path, s3=s3, check=False)
        else:
            raise ValueError(store_type)

        if root:
            return {
                '/': xr.open_zarr(store, consolidated=True),
            }
        else:
            return {
                '/': xr.open_zarr(store, consolidated=True),
                '/Meteorology': xr.open_zarr(store, group='Meteorology', consolidated=True),
                '/Preprocessors': xr.open_zarr(store, group='Preprocessors', consolidated=True),
                '/Retrieval': xr.open_zarr(store, group='Retrieval', consolidated=True),
                '/Sounding': xr.open_zarr(store, group='Sounding', consolidated=True),
            }

    def write(self, ds: Dict[str, Dataset]):
        logger.info(f'Writing SAM group to Zarr array at {self.path}')

        exists = self._exists()

        if exists and self.overwrite:
            logger.warning('File already exists and will be overwritten')
        elif exists and not self.overwrite:
            logger.info('File exists and will be appended to')
            raise ValueError('File exists and cannot be overwritten')

        mode = 'w' if self.overwrite else 'a'
        append_dim = self.__append_dim if not self.overwrite else None

        compressor = zarr.Blosc(cname='blosclz', clevel=9)

        encodings = {group: {
            vname: {
                'compressor': compressor,
                'chunks': self.__chunking,
                'write_empty_chunks': False
            } for vname in ds[group].data_vars
        } for group in ds}

        for group in ds:
            for var in ds[group].data_vars:
                ds[group][var] = ds[group][var].chunk(self.__chunking)

        if self.store == 'local':
            for group in Writer.GROUP_KEYS:
                cdf_group = group[1:]

                if cdf_group == '':
                    ds[group].to_zarr(self.path, mode=mode, append_dim=append_dim, encoding=encodings[group])
                else:
                    ds[group].to_zarr(self.path, mode=mode, group=cdf_group, append_dim=append_dim,
                                      encoding=encodings[group])
        else:
            raise NotImplementedError()

        if self.__verify and not self.overwrite:
            zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params, root=True)
            dim = zarr_group['/'][self.__append_dim].to_numpy()

            if not all(np.diff(dim) >= 0):
                logger.warning('Appended Zarr array not monotonically increasing along append dimension. '
                               'It will need to be sorted')

                zarr_group = ZarrWriter.open_zarr_group(self.path, self.store, self.store_params)

                self.overwrite = True

                for key in Writer.GROUP_KEYS:
                    zarr_group[key] = zarr_group[key].sortby(self.__append_dim)

                self.write(zarr_group)


