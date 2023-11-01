# Copyright 2023 California Institute of Technology (Caltech)
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
from getpass import getpass
from netrc import netrc
from subprocess import Popen
from typing import Dict, Optional, List, Tuple
from urllib.parse import urlparse, ParseResult

import boto3
import requests
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
            drop_dims: Optional[List[Tuple[str, str]]] = None,
            s3_region=None,
            s3_auth: Dict[str, str] = None
    ):
        self.__url = path
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
            ds_dict = {
                '/': xr.open_dataset(path, mask_and_scale=False, engine='h5netcdf').load(),
                '/Meteorology': xr.open_dataset(path, group='Meteorology', mask_and_scale=False, engine='h5netcdf').load(),
                '/Preprocessors': xr.open_dataset(path, group='Preprocessors', mask_and_scale=False, engine='h5netcdf').load(),
                '/Retrieval': xr.open_dataset(path, group='Retrieval', mask_and_scale=False, engine='h5netcdf').load(),
                '/Sounding': xr.open_dataset(path, group='Sounding', mask_and_scale=False, engine='h5netcdf').load(),
            }
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
    def __download_s3(url: ParseResult, auth, region):
        logger.info('Downloading file from s3')

        fp = tempfile.NamedTemporaryFile(suffix='.nc4')

        if 'urs' in auth:
            auth = GranuleReader._get_temporary_creds(auth['urs'])

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
            logger.debug(f'Couldn\'t parse object HEAD for {url.geturl()}')
        except Exception:
            logger.debug(f'HEAD failed, could not determine metadata for {url.geturl()}')

        logger.debug(f'Size of downloaded file {fp.name}: {os.path.getsize(fp.name):,} bytes')

        if expected_length is not None and expected_length != os.path.getsize(fp.name):
            logger.warning(f'Size of downloaded file differs from expected size from S3. '
                           f'{expected_length:,} (expected) != {os.path.getsize(fp.name):,} (downloaded)')

        return fp

    @staticmethod
    def _get_temporary_creds(urs_endpoint: str) -> Dict[str, str]:
        logger.info(f'Fetching temporary S3 credentials from {urs_endpoint}')

        response = requests.get(urs_endpoint)
        response.raise_for_status()

        response_dict = response.json()

        return dict(
            accessKeyID=response_dict['accessKeyId'],
            secretAccessKey=response_dict['secretAccessKey'],
            sessionToken=response_dict['sessionToken']
        )

    @staticmethod
    def configure_netrc(username, password, urs='urs.earthdata.nasa.gov',):
        netrc_name = ".netrc"

        if urs in CHECKED_URS:
            return

        logger.info(f'Checking if {urs} netrc is present')

        try:
            netrcDir = os.path.expanduser(f"~/{netrc_name}")
            _ = netrc(netrcDir).authenticators(urs)[0]

            logger.info('Found .netrc in homedir with needed creds')
        except (FileNotFoundError, TypeError):
            logger.warning('.netrc not found or does not contain needed creds, trying to add them')

            # assert username is not None, 'Username must be provided if netrc is not present'
            # assert password is not None, 'Password must be provided if netrc is not present'

            if username is None:
                username = getpass("Earthdata Username: ")

            if password is None:
                password = getpass("Earthdata Password: ")

            homeDir = os.path.expanduser("~")

            with open(os.path.join(homeDir, netrc_name), 'at') as nf:
                nf.write(f'\nmachine {urs}\n')
                nf.write(f'\n    login {username}\n')
                nf.write(f'\n    password {password}\n')

            # Popen('touch {0}{2} | echo machine {1} >> {0}{2}'.format(homeDir + os.sep, urs, netrc_name), shell=True)
            # Popen('echo login {} >> {}{}'.format(username, homeDir + os.sep, netrc_name), shell=True)
            # Popen('echo \'password {} \'>> {}{}'.format(password, homeDir + os.sep, netrc_name),
            #       shell=True)
            # Set restrictive permissions
            Popen('chmod 0600 {0}{1}'.format(homeDir + os.sep, netrc_name), shell=True)

        CHECKED_URS.append(urs)
