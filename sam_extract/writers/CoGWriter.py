# Copyright 2025 California Institute of Technology (Caltech)
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
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict
from urllib.parse import urlparse

import boto3
from sam_extract.utils import AWSConfig
from sam_extract.writers import Writer
from sam_extract.writers.Writer import FIXED_ATTRIBUTES
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, after_log
from xarray import Dataset

logger = logging.getLogger(__name__)

DATETIME_FMT = "%Y-%m-%dT%H%M%SZ"

DATASET_FILENAME_PREFIXES = {
    'oco2': 'oco2-co2',
    'oco3': 'oco3-co2',
    'oco3_sif': 'oco3-sif',
}


class CoGWriter(Writer):
    DRIVER_OPTIONS = [
        'blocksize', 'compress', 'level', 'max_z_error', 'max_z_error_overview', 'quality', 'jxl_lossless',
        'jxl_effort', 'jxl_distance', 'jxl_alpha_distance', 'num_threads', 'nbits', 'predictor', 'bigtiff',
        'resampling', 'overview_resampling', 'warp_resampling', 'overviews', 'overview_count', 'overview_compress',
        'overview_quality', 'overview_predictor', 'geotiff_version', 'sparse_ok', 'statistics', 'tiling_scheme',
        'zoom_level', 'zoom_level_strategy', 'target_srs', 'res', 'extent', 'aligned_levels', 'add_alpha'
    ]

    def __init__(self,
                 path: str,
                 mission: str,
                 filtered: bool,
                 driver_kwargs: dict = None,
                 efs=False,
                 **kwargs):
        Writer.__init__(self, path, True, **kwargs)
        if driver_kwargs is None:
            driver_kwargs = {}

        self.__mission = mission
        self.__filtered = filtered
        self.__driver_kwargs = driver_kwargs

        # If we're local writing to EFS filesystem, do things a little bit differently
        self.__efs = efs and self.store == 'local'

        if self.__efs:
            self.__tp: ThreadPoolExecutor | None = ThreadPoolExecutor(thread_name_prefix='cog-worker')
            self.__td = TemporaryDirectory(ignore_cleanup_errors=True)
            self.__futures = []
        else:
            self.__tp: ThreadPoolExecutor | None = None
            self.__td = None
            self.__futures = None

        logger.debug(f'Created CoG writer for path {self.path} with driver options {self.__driver_kwargs}')

    @staticmethod
    def validate_options(options: dict):
        validated = {}
        invalid = []

        for opt in options:
            if opt.lower() in CoGWriter.DRIVER_OPTIONS:
                validated[opt] = options[opt]
            else:
                invalid.append(f'{opt}={options[opt]}')

        if len(invalid) > 0:
            logger.warning(f'Ignoring invalid CoG driver options: {invalid}')

        return validated

    @retry(
        stop=stop_after_attempt(2),
        retry=retry_if_exception_type(FileNotFoundError),
        reraise=True,
        after=after_log(logger, logging.WARNING)
    )
    def write(
            self,
            ds: Dict[str, Dataset],
            target_id: str = '',
            attrs: Dict[str, str] | None = None,
            skip_verification=False
    ):
        if target_id == '':
            raise ValueError('target_id must not be empty')

        if attrs is None:
            attrs = {}

        fixed_attrs = FIXED_ATTRIBUTES['local'].get(self.__mission, {})
        if self.__mission not in FIXED_ATTRIBUTES['local']:
            logger.warning(f'No fixed attributes found for {self.__mission}. Please contact developers.')

        attrs.update(fixed_attrs)

        temp_dir: TemporaryDirectory | None = None
        tiffs = []

        if self.store == 'local':
            root_dir = urlparse(self.path).path
        elif self.store == 's3':
            temp_dir = TemporaryDirectory(ignore_cleanup_errors=True)
            root_dir = temp_dir.name
        else:
            raise ValueError('Bad store type')

        try:
            if not self.exists():
                self._create_root(root_dir)

            for group in ds:
                group_name = group.strip('/')

                for var in ds[group].data_vars:
                    var_name = f'{group_name}.{var}' if group_name != '' else var

                    da = ds[group][var]

                    for time in da.time:
                        data = da.sel(time=time)
                        data = data.rio.write_crs("epsg:4326")
                        data.attrs.update(attrs)
                        dt = time.values.astype('datetime64[s]').item()

                        data.attrs['datetime_of_first_sounding'] = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        data.attrs = {k.upper(): v for k, v in data.attrs.items()}

                        try:
                            latitude = data.latitude.to_numpy()

                            if latitude[1] - latitude[0] >= 0:
                                logger.debug(f'Flipping latitude for {self.__mission}:{target_id}.{var}')
                                data = data.isel(latitude=slice(None, None, -1))
                        except Exception as e:
                            logger.warning(f'Could not check latitude ordering for {self.__mission}:{target_id}.{var}'
                                           f'due to {e}')

                        fn_mission = DATASET_FILENAME_PREFIXES.get(self.__mission, self.__mission)

                        filename = (f'{fn_mission}_{target_id}_{dt.strftime(DATETIME_FMT)}_'
                                    f'{"filtered" if self.__filtered else "unfiltered"}_{var_name}.tif')

                        if not self.__efs:
                            out_path = os.path.join(root_dir, filename)
                            logger.debug(f'Writing Cloud-Optimized GeoTIFF to {out_path}')
                            data.rio.to_raster(out_path, driver='COG', sharing=False, **self.__driver_kwargs)
                            tiffs.append(out_path)
                        else:
                            out_path = os.path.join(self.__td.name, filename)
                            logger.debug(f'Writing Cloud-Optimized GeoTIFF {filename}')
                            logger.trace(f'Writing Cloud-Optimized GeoTIFF to {out_path}')
                            data.rio.to_raster(out_path, driver='COG', sharing=False, **self.__driver_kwargs)

                            dst_path = os.path.join(root_dir, filename)

                            # Use copy instead of move because a: the source files will disappear at container exit and
                            # b: there have been issues with the unlink step of shutil.move on EFS

                            self.__futures.append(
                                self.__tp.submit(
                                    CoGWriter.__copy,
                                    out_path, dst_path,
                                )
                            )

                            logger.trace(f'Queued move of {out_path} -> {dst_path}')
            if self.store == 's3':
                logger.debug(f'Pushing {len(tiffs):,} to {self.path}')

                url = urlparse(self.path)

                bucket = url.netloc
                prefix = url.path.strip('/')

                s3 = boto3.client('s3', **AWSConfig.from_store_params(self.store_params).to_client_kwargs())

                def upload(path):
                    key = f'{prefix}/{os.path.basename(path)}'
                    logger.trace(f'S3: {path} -> s3://{bucket}/{key}')
                    s3.upload_file(path, bucket, key)

                with ThreadPoolExecutor(thread_name_prefix='s3_upload_worker') as tp:
                    futures = []

                    for tiff in tiffs:
                        futures.append(tp.submit(upload, tiff))

                    for f in futures:
                        f.result()
        finally:
            if temp_dir is not None:
                temp_dir.cleanup()

    def exists(self) -> bool:
        if self.store == 'local':
            return os.path.isdir(urlparse(self.path).path)
        elif self.store == 's3':
            return True  # Not applicable here. Just create/overwrite as needed
        else:
            raise ValueError('Bad store type')

    def finish(self):
        if self.__efs:
            for f in self.__futures:
                f.result()

            self.__td.cleanup()
            self.__tp.shutdown()

    def _create_root(self, root):
        if self.store == 'local':
            Path(root).mkdir(parents=True, exist_ok=True)
        elif self.store == 's3':
            raise NotImplementedError()
        else:
            raise ValueError('Bad store type')

    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(0.05), reraise=True, after=after_log(logger, logging.WARNING))
    def __copy(src, dst, *, follow_symlinks=True):
        logger.trace(f'Attempting copy of {src} to {dst}')
        shutil.copy2(src, dst, follow_symlinks=follow_symlinks)
        # logger.trace(f'stat {dst}: {os.stat(dst)}')
