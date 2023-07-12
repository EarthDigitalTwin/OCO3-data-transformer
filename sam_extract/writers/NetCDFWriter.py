import logging
from typing import Dict
from urllib.parse import urlparse

from xarray import Dataset

from sam_extract.writers import Writer

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

            for group in Writer.GROUP_KEYS:
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
