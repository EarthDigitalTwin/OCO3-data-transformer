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

import hashlib
import logging
import os
import tempfile
import threading
from typing import Dict, Optional, List, Tuple
from urllib.parse import urlparse, ParseResult

import boto3
import xarray as xr
from botocore.exceptions import ClientError
from sam_extract.exceptions import ReaderException

logger = logging.getLogger(__name__)

ESSENTIAL_VARS = [
    ('/', '*'),
    ('/Sounding', 'operation_mode'),
    ('/Sounding', 'target_id'),
    ('/Sounding', 'target_name')
]


BOTO_SESSION = None
BOTO_LOCK = threading.Lock()

CHECKED_URS = []


class GranuleReader:
    def __init__(
            self,
            path: str,
            groups: Dict[str, str],
            drop_dims: Optional[List[Tuple[str, str]]] = None,
            s3_region=None,
            s3_auth: Dict[str, str] = None
    ):
        self.__url = path
        self.__groups = groups
        self.__ds_dict: Dict[str, xr.Dataset] | None = None
        self.__s3_file = None
        self.__s3_region = s3_region
        self.__drop = drop_dims if drop_dims else []
        self.__auth = s3_auth

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__s3_file:
            logger.info(f'Deleting temporary input file {self.__s3_file.name}')
            self.__s3_file.close()
            self.__s3_file = None

        if self.__ds_dict:
            for group in list(self.__ds_dict.keys()):
                self.__ds_dict[group].close()
                del self.__ds_dict[group]

            self.__ds_dict = None

    def open(self) -> Dict[str, xr.Dataset]:
        url = urlparse(self.__url)

        if url.scheme == 's3':
            self.__s3_file = self.__download_s3(url, self.__auth, self.__s3_region)
            path = self.__s3_file.name
        elif url.scheme in ['', 'file']:
            path = url.path
        else:
            raise ValueError(f'Invalid URL scheme: {url.scheme}. Expected file or S3')

        try:
            # Open the input datasets but do not mask out missing values yet
            ds_dict = dict()

            for group in self.__groups:
                logger.debug(
                    f'Opening NetCDF group {self.__groups[group] if self.__groups[group] is not None else "root"}'
                )
                ds_dict[group] = xr.open_dataset(
                    path,
                    group=self.__groups[group],
                    mask_and_scale=False,
                    engine='h5netcdf',
                ).load()
        except FileNotFoundError:
            logger.error(f'Input file {path} does not exist')
            raise ReaderException(f'Input file {path} does not exist')
        except OSError as err:
            logger.error('Invalid group name')
            logger.exception(err)
            raise ReaderException('Invalid group name')
        except Exception as err:
            logger.error('Something went wrong!')
            logger.exception(err)
            raise ReaderException('Something went wrong!')

        logger.info(f'Granule at {path} loaded successfully; now dropping dimensions provided')

        for group, var in self.__drop:
            if group == '/' or (group, var) in ESSENTIAL_VARS:
                raise ValueError(f'Cannot drop variable {"/".join((group, var))} as it is essential for SAM extraction'
                                 f' and/or desired science use cases.')

            if group not in ds_dict and '/' + group in ds_dict:
                group = '/' + group

            if group in ds_dict:
                try:
                    ds_dict[group] = ds_dict[group].drop_vars(var)
                except ValueError:
                    logger.debug(f'Variable to drop {"/".join((group, var))} is not present in the dataset; ignoring')

        for group in ds_dict:
            for var in ds_dict[group].data_vars:
                ds_dict[group][var].attrs['_FillValue'] = float('nan')

        self.__ds_dict = ds_dict

        return ds_dict

    @staticmethod
    def __download_s3(url: ParseResult, auth, region):
        logger.info('Downloading file from s3')

        fp = tempfile.NamedTemporaryFile(suffix='.nc4')

        with BOTO_LOCK:
            global BOTO_SESSION
            if BOTO_SESSION is None:
                logger.debug('Creating global boto session')
                BOTO_SESSION = boto3.session.Session(region_name=region)

            session = BOTO_SESSION

        client_auth_kwargs = dict(
            aws_access_key_id=auth['accessKeyID'],
            aws_secret_access_key=auth['secretAccessKey']
        )

        if 'sessionToken' in auth:
            client_auth_kwargs['aws_session_token'] = auth['sessionToken']

        with BOTO_LOCK:
            try:
                client = session.client(
                    's3',
                    region_name=region,
                    **client_auth_kwargs
                )
            except Exception as e:
                logger.critical('Could not create boto client!')
                logger.exception(e)
                raise

        try:
            client.download_fileobj(url.hostname, url.path[1:], fp)
            fp.flush()
        except ClientError as e:
            logger.error(f'Cannot download file {url.geturl()} from S3. {str(e)}')
            raise ReaderException(f'Cannot download file {url.geturl()} from S3. {str(e)}')

        logger.info(
            f'Downloaded file from S3 bucket {url.hostname}, key {url.path[1:]} to {fp.name}'
        )

        with open(fp.name, 'rb') as hash_fp:
            md5 = hashlib.md5(hash_fp.read()).hexdigest()
            logger.debug(f'MD5 checksum for {fp.name}: {md5}')

        expected_length = None

        try:
            head = client.head_object(Bucket=url.hostname, Key=url.path[1:])
            checksums = {}

            expected_length = head['ContentLength']

            for field in head:
                if field.startswith('Checksum'):
                    checksums[field.lstrip('Checksu,')] = head[field]

            if len(checksums) > 0:
                for alg in checksums:
                    logger.debug(f'{alg} checksum: {checksums[alg]}')
            else:
                logger.debug('No checksums provided from S3')

            logger.debug(f'Expected size of downloaded granule: {expected_length:,} bytes')
        except KeyError:
            logger.warning(f'Couldn\'t parse object HEAD for {url.geturl()}')
        except Exception:
            logger.warning(f'HEAD failed, could not determine metadata for {url.geturl()}')

        logger.debug(f'Size of downloaded file {fp.name}: {os.path.getsize(fp.name):,} bytes')

        if expected_length is not None and expected_length != os.path.getsize(fp.name):
            logger.warning(f'Size of downloaded file differs from expected size from S3. '
                           f'{expected_length:,} (expected) != {os.path.getsize(fp.name):,} (downloaded)')

        return fp
