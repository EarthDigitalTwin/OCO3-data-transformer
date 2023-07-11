from xarray import Dataset
from typing import Dict
from abc import ABC, abstractmethod

from os.path import exists
from urllib.parse import urlparse
from botocore.exceptions import ClientError
from botocore.config import Config
import boto3
import logging


logger = logging.getLogger(__name__)


class Writer(ABC):
    GROUP_KEYS = ['/', '/Meteorology', '/Preprocessors', '/Retrieval', '/Sounding']

    def __init__(self, path: str, overwrite: bool = False, **kwargs):
        self.path = path
        self.overwrite = overwrite

        self.store = None
        self.store_params = {}

        url = urlparse(path)

        if url.scheme in ['file', '']:
            self.store = 'local'
        elif url.scheme == 's3':
            self.store = 's3'

            self.store_params['public'] = kwargs['public'] if 'public' in kwargs else False
            self.store_params['region'] = kwargs['region'] if 'region' in kwargs else 'us-west-2'
        else:
            raise ValueError(f"Invalid URL scheme provided for output: {url.scheme}. Must be either file:// (explicit "
                             f"or implicit) or s3://")

    def _exists(self) -> bool:
        if self.store == 'local':
            return exists(urlparse(self.path).path)
        else:
            url = urlparse(self.path)

            bucket = url.netloc

            config = Config(region_name=self.store_params['region'])

            s3 = boto3.client('s3', config=config)

            try:
                s3.head_object(Bucket=bucket, Key=url.path)
                return True
            except s3.exceptions.NoSuchKey:
                return False
            except ClientError as e:
                r = e.response
                logger.error(f'An AWS error occurred: Code={r["Error"]["Code"]} Message={r["Error"]["Message"]}')
                raise
            except Exception as e:
                logger.error('Something went wrong!')
                logger.exception(e)
                raise

    @abstractmethod
    def write(self, ds: Dict[str, Dataset]):
        pass


