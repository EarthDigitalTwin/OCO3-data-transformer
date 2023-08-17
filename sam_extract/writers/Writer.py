import logging
from abc import ABC, abstractmethod
from os.path import exists
from typing import Dict
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from xarray import Dataset

import sam_extract

logger = logging.getLogger(__name__)


FIXED_ATTRIBUTES = {
    'pipeline_version': sam_extract.__version__,
    'institution': 'Jet Propulsion Laboratory',
    'source': 'Derived from the OCO3_L2_Lite_FP_10.4r dataset',
    'references': '10.5194/amt-12-2341-2019, '
                  '10.1016/j.rse.2020.112032, '
                  '10.1016/j.rse.2021.112314',
    'comment': 'NetCDF Lite files converted to Zarr on fixed grid',
    'platform': 'ISS',
    'sensor': 'OCO-3',
    'operation_mode': 'Snapshot Area Mapping [SAM]',
    'processing_level': 'L3',
    'contacts': 'Riley Kuttruff <Riley.K.Kuttruff@jpl.nasa.gov>; '
                'Nga Chung <Nga.T.Chung@jpl.nasa.gov>; '
                'Abhishek Chatterjee <Abhishek.Chatterjee@jpl.nasa.gov>',
}


class Writer(ABC):
    GROUP_KEYS = ['/', '/Meteorology', '/Preprocessors', '/Retrieval', '/Sounding']

    def __init__(self, path: str, overwrite: bool = False, **kwargs):
        self.path = path
        self.overwrite = overwrite

        self.store = None
        self.store_params = {}

        self.final = kwargs.get('final', False)

        url = urlparse(path)

        if url.scheme in ['file', '']:
            self.store = 'local'
        elif url.scheme == 's3':
            self.store = 's3'

            # self.store_params['public'] = kwargs['public'] if 'public' in kwargs else False
            self.store_params['region'] = kwargs['region'] if 'region' in kwargs else 'us-west-2'
            self.store_params['auth'] = kwargs['auth'] if 'auth' in kwargs else None
        else:
            raise ValueError(f"Invalid URL scheme provided for output: {url.scheme}. Must be either file:// (explicit "
                             f"or implicit) or s3://")

    def _exists(self) -> bool:
        if self.store == 'local':
            return exists(urlparse(self.path).path)
        else:
            url = urlparse(self.path)

            bucket = url.netloc
            key = url.path

            # key will likely be of the form '/path/to/root'. Remove leading / and append /.zgroup
            # since head_object doesn't seem to work for directories. This will also ensure that we
            # return true iff a zarr array exists @ this path
            if key[0] == '/':
                key = key[1:]

            if key[-1] == '/':
                key = f'{key}.zgroup'
            else:
                key = f'{key}/.zgroup'

            config = Config(region_name=self.store_params['region'])

            s3 = boto3.client(
                's3',
                aws_access_key_id=self.store_params['auth']['accessKeyID'],
                aws_secret_access_key=self.store_params['auth']['secretAccessKey'],
                config=config
            )

            try:
                s3.head_object(Bucket=bucket, Key=key)
                return True
            except s3.exceptions.NoSuchKey:
                return False
            except ClientError as e:
                r = e.response
                err_code = r["Error"]["Code"]

                if err_code == '404':
                    return False

                logger.error(f'An AWS error occurred: Code={err_code} Message={r["Error"]["Message"]}')
                raise
            except Exception as e:
                logger.error('Something went wrong!')
                logger.exception(e)
                raise

    @abstractmethod
    def write(self, ds: Dict[str, Dataset]):
        pass


