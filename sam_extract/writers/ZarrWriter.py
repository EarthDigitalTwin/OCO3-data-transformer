import logging
from typing import Dict, Tuple
from urllib.parse import urlparse

import s3fs
import xarray as xr
import zarr
from xarray import Dataset

from sam_extract.writers import Writer

logger = logging.getLogger(__name__)


def __open_zarr_group(path, store_type, params) -> Dict[str, Dataset]:
    logger.info(f'Opening Zarr array group at {path}')

    if store_type == 'local':
        return {
            '/': xr.open_zarr(path, consolidated=True),
            '/Meteorology': xr.open_zarr(path, group='Meteorology', consolidated=True),
            '/Preprocessors': xr.open_zarr(path, group='Preprocessors', consolidated=True),
            '/Retrieval': xr.open_zarr(path, group='Retrieval', consolidated=True),
            '/Sounding': xr.open_zarr(path, group='Sounding', consolidated=True),
        }
    else:
        url = urlparse(path)

        bucket = url.netloc
        key = url.path

        if params['public']:
            store = f'https://{bucket}.s3.{params["region"]}.amazonaws.com{key}' # key has leading /
        else:
            s3 = s3fs.S3FileSystem(False, ) #TODO: auth here
            store = s3fs.S3Map(root=path, s3=s3, check=False)

        return {
            '/': xr.open_zarr(store, consolidated=True),
            '/Meteorology': xr.open_zarr(store, group='Meteorology', consolidated=True),
            '/Preprocessors': xr.open_zarr(store, group='Preprocessors', consolidated=True),
            '/Retrieval': xr.open_zarr(store, group='Retrieval', consolidated=True),
            '/Sounding': xr.open_zarr(store, group='Sounding', consolidated=True),
        }


class ZarrL3Writer(Writer):
    def __init__(self, path: str, chunking: Tuple[int, int, int], overwrite: bool = False, **kwargs):
        Writer.__init__(self, path, overwrite, **kwargs)
        self.__chunking = chunking

    def write(self, ds: Dict[str, Dataset]):
        exists = self._exists()

        if exists and self.overwrite:
            logger.warning('File already exists and will be overwritten')
        elif exists and not self.overwrite:
            logger.info('File exists and will be appended to')
            raise ValueError('File exists and cannot be overwritten')

        mode = 'w' if self.overwrite else 'a'
        append_dim = 'time' if not self.overwrite else None

        compressor = zarr.Blosc(cname='blosclz', clevel=9)

        encodings = {group: {
            {vname: {'compressor': compressor, 'chunk': self.__chunking} for vname in ds[group].data_vars}
        } for group in ds}

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

        # TODO: If appending, verify time is monotonically ascending and fix if not


class ZarrTrajectoryWriter(Writer):
    pass
