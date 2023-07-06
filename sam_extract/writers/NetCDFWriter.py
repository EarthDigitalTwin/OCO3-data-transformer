import logging
from typing import Dict

from xarray import Dataset

from sam_extract.writers import Writer

logger = logging.getLogger(__name__)


class NetCDFWriter(Writer):
    def __init__(self, path: str, overwrite: bool = False, **kwargs):
        Writer.__init__(self, path, overwrite, **kwargs)

    def write(self, ds: Dict[str, Dataset]):
        exists = self._exists()

        if exists and self.overwrite:
            logger.warning('File already exists and will be overwritten')
        elif exists and not self.overwrite:
            logger.error('File exists and cannot be overwritten')
            raise ValueError('File exists and cannot be overwritten')

        mode = 'w' if self.overwrite else 'a'

        comp = {"zlib": True, "complevel": 9}
        encodings = {group: {
            {vname: comp for vname in ds[group].data_vars}
        } for group in ds}

        if self.store == 'local':
            for group in Writer.GROUP_KEYS:
                cdf_group = group[1:]

                if cdf_group == '':
                    ds[group].to_netcdf(self.path, mode=mode, encoding=encodings[group])
                else:
                    ds[group].to_netcdf(self.path, mode=mode, group=cdf_group, encoding=encodings[group])

                mode = 'a'
        else:
            raise NotImplementedError()
