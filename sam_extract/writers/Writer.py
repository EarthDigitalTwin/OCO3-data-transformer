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

import logging
from abc import ABC, abstractmethod
from os.path import exists, join
from typing import Dict
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from xarray import Dataset

import sam_extract

logger = logging.getLogger(__name__)


FIXED_ATTRIBUTES = {
    'global': {
        'pipeline_version': sam_extract.__version__,
        'institution': 'Jet Propulsion Laboratory',
        'source': 'Derived from the OCO3_L2_Lite_FP_11r, OCO2_L2_Lite_FP_11.2r, and OCO3_L2_Lite_SIF_11r datasets '
                  'from GES-DISC (https://disc.gsfc.nasa.gov/datasets/OCO3_L2_Lite_FP_11r/summary, '
                  'https://disc.gsfc.nasa.gov/datasets/OCO2_L2_Lite_FP_11.2r/summary, '
                  'https://disc.gsfc.nasa.gov/datasets/OCO3_L2_Lite_SIF_11r/summary)',
        'references': '10.5194/amt-12-2341-2019, '
                      '10.1016/j.rse.2020.112032, '
                      '10.1016/j.rse.2021.112314',
        'comment': 'NetCDF Lite files from any number of source datasets converted to Zarr on fixed grid',
        'platform': 'ISS, OCO-2',
        'sensor': 'OCO-3, OCO-2',
        'operation_mode': 'Snapshot Area Mapping [SAM] + Target',
        'processing_level': 'L3',
        'contacts': 'Riley Kuttruff <Riley.K.Kuttruff@jpl.nasa.gov>; '
                    'Nga Chung <Nga.T.Chung@jpl.nasa.gov>; '
                    'Abhishek Chatterjee <Abhishek.Chatterjee@jpl.nasa.gov>',
    },
    'local': {
        'oco3': {
            'pipeline_version': sam_extract.__version__,
            'institution': 'Jet Propulsion Laboratory',
            'source': 'Derived from the OCO3_L2_Lite_FP_11r dataset from GES-DISC '
                      '(https://disc.gsfc.nasa.gov/datasets/OCO3_L2_Lite_FP_11r/summary)',
            'references': '10.5194/amt-12-2341-2019, '
                          '10.1016/j.rse.2020.112032, '
                          '10.1016/j.rse.2021.112314',
            'comment': 'NetCDF Lite files converted to Zarr (and/or Cloud-Optimized GeoTIFF) on fixed grid',
            'platform': 'ISS',
            'sensor': 'OCO-3',
            'operation_mode': 'Snapshot Area Mapping [SAM] + Target',
            'processing_level': 'L3',
            'contacts': 'Riley Kuttruff <Riley.K.Kuttruff@jpl.nasa.gov>; '
                        'Nga Chung <Nga.T.Chung@jpl.nasa.gov>; '
                        'Abhishek Chatterjee <Abhishek.Chatterjee@jpl.nasa.gov>',
        },
        'oco2': {
            'pipeline_version': sam_extract.__version__,
            'institution': 'Jet Propulsion Laboratory',
            'source': 'Derived from the OCO2_L2_Lite_FP_11.2r dataset from GES-DISC '
                      '(https://disc.gsfc.nasa.gov/datasets/OCO2_L2_Lite_FP_11.2r/summary)',
            # 'references': '10.5194/amt-12-2341-2019, '
            #               '10.1016/j.rse.2020.112032, '
            #               '10.1016/j.rse.2021.112314',
            'comment': 'NetCDF Lite files converted to Zarr (and/or Cloud-Optimized GeoTIFF) on fixed grid',
            'platform': 'OCO-2',
            'sensor': 'OCO-2',
            'operation_mode': 'Target',
            'processing_level': 'L3',
            'contacts': 'Riley Kuttruff <Riley.K.Kuttruff@jpl.nasa.gov>; '
                        'Nga Chung <Nga.T.Chung@jpl.nasa.gov>; '
                        'Abhishek Chatterjee <Abhishek.Chatterjee@jpl.nasa.gov>',
        },
        'oco3_sif': {
            'pipeline_version': sam_extract.__version__,
            'institution': 'Jet Propulsion Laboratory',
            'source': 'Derived from the OCO3_L2_Lite_SIF_11r dataset from GES-DISC '
                      '(https://disc.gsfc.nasa.gov/datasets/OCO3_L2_Lite_SIF_11r/summary)',
            # 'references': '10.5194/amt-12-2341-2019, '
            #               '10.1016/j.rse.2020.112032, '
            #               '10.1016/j.rse.2021.112314',
            'comment': 'NetCDF Lite files of solar-induced florescence data converted to Zarr (and/or '
                       'Cloud-Optimized GeoTIFF) on fixed grid',
            'platform': 'ISS',
            'sensor': 'OCO-3',
            'operation_mode': 'Snapshot Area Mapping [SAM] + Target',
            'processing_level': 'L3',
            'contacts': 'Riley Kuttruff <Riley.K.Kuttruff@jpl.nasa.gov>; '
                        'Nga Chung <Nga.T.Chung@jpl.nasa.gov>; '
                        'Abhishek Chatterjee <Abhishek.Chatterjee@jpl.nasa.gov>',
        },
    }
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

    def exists(self) -> bool:
        if self.store == 'local':
            return exists(join(urlparse(self.path).path, '.zgroup'))
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


