import logging
import tempfile
from typing import Dict, Optional, List, Tuple
from urllib.parse import urlparse, ParseResult

import boto3
import xarray as xr

logger = logging.getLogger(__name__)

ESSENTIAL_VARS = [
    ('/', '*'),
    ('/Sounding', 'operation_mode')
]


class GranuleReader:
    def __init__(self, path: str, drop_dims: Optional[List[Tuple[str, str]]] = None, s3_region=None):
        self.__url = path
        self.__s3_file = None
        self.__s3_region = s3_region
        self.__drop = drop_dims if drop_dims else []

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__s3_file:
            self.__s3_file.close()

    def open(self) -> Dict[str, xr.Dataset]:
        url = urlparse(self.__url)

        if url.scheme == 's3':
            self.__s3_file = self.__download_s3(url)
            path = self.__s3_file.name
        elif url.scheme in ['', 'file']:
            path = url.path
        else:
            raise ValueError(f'Invalid URL scheme: {url.scheme}. Expected file or S3')

        try:
            # Open the input datasets but do not mask out missing values yet
            ds_dict = {
                '/': xr.open_dataset(path, mask_and_scale=False),
                '/Meteorology': xr.open_dataset(path, group='Meteorology', mask_and_scale=False),
                '/Preprocessors': xr.open_dataset(path, group='Preprocessors', mask_and_scale=False),
                '/Retrieval': xr.open_dataset(path, group='Retrieval', mask_and_scale=False),
                '/Sounding': xr.open_dataset(path, group='Sounding', mask_and_scale=False),
            }
        except FileNotFoundError:
            logger.error(f'Input file {path} does not exist')
            raise
        except OSError:
            logger.error('Invalid group name')
            raise
        except Exception:
            logger.error('Something went wrong!')
            raise

        logger.info(f'Granule at {path} loaded successfully); now dropping dimensions provided')

        for group, var in self.__drop:
            if group == '/' or (group, var) in ESSENTIAL_VARS:
                raise ValueError(f'Cannot drop variable {"/".join((group, var))} as it is essential for SAM extraction'
                                 f' and/or desired science use cases.')

            if group not in ds_dict and '/' + group in ds_dict:
                group = '/' + group

            try:
                ds_dict[group] = ds_dict[group].drop_vars(var)
            except ValueError:
                logger.warning(f'Variable to drop {"/".join((group, var))} is not present in the dataset; ignoring')

        for group in ds_dict:
            for var in ds_dict[group].data_vars:
                ds_dict[group][var].attrs['_FillValue'] = float('nan')

        logger.info('Dropped all requested variables')

        return ds_dict

    @staticmethod
    def __download_s3(url: ParseResult):
        fp = tempfile.NamedTemporaryFile()

        client = boto3.client('s3')
        client.download_fileobj(url.hostname, url.path[1:], fp)

        logger.info(
            f'Downloaded file from S3 bucket {url.hostname}, key {url.path[1:]} to {fp.name}'
        )

        return fp
