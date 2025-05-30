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


import json
import logging
import re
import warnings
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import numpy as np
import xarray as xr
from sam_extract.exceptions import *
from sam_extract.processors import Processor
from sam_extract.processors.Processor import PROCESSORS
from sam_extract.readers import GranuleReader
from sam_extract.runconfig import RunConfig
from sam_extract.utils.dataset_utils import is_nan
from scipy.interpolate import griddata
from shapely.affinity import scale
from shapely.geometry import Polygon, box

logger = logging.getLogger(__name__)

OPERATION_MODE_SAM = 4
OPERATION_MODE_TARGET = 2

PROCESSOR_PREFIX = ''

WARN_ON_UNKNOWN_TARGET = True

GROUPS = {
    '/': None,
    '/Meteorology': 'Meteorology',
    '/Preprocessors': 'Preprocessors',
    '/Retrieval': 'Retrieval',
    '/Sounding': 'Sounding',
}

NEEDED_VARS = {
    '/': ['latitude', 'longitude', 'date', 'vertex_latitude', 'vertex_longitude', 'sounding_id', 'xco2_quality_flag'],
    '/Sounding': ['operation_mode', 'target_id']
}

DEFAULT_INCLUDED_VARS = {
    '/': ['xco2', 'xco2_uncertainty']
}


def tr(s: str, chars: str = None):
    return re.sub(rf'([{chars}])(\1+)', r'\1', s)


def fit_data_to_grid(sam, target, bounds, cfg: RunConfig):
    if len(sam) == 0:
        return None

    if target not in bounds or bounds[target]['bbox'] is None:
        if WARN_ON_UNKNOWN_TARGET:
            logger.error(f'Could not find bounds for {target} in provided targets list. Will have to omit this target '
                         f'from the output')
            return None
        else:
            raise KeyError(target)

    bbox_dict = bounds[target]['bbox']

    sam = sam.copy()

    lats = sam['/'].latitude.to_numpy()
    lons = sam['/'].longitude.to_numpy()
    time = np.array([datetime(*sam['/'].date[0].to_numpy().astype(int)).replace(tzinfo=timezone.utc).timestamp()])

    points = list(zip(lons, lats))

    # Dimensions that will not be interpolated and fit to grid
    drop_dims = {
        '/': ['bands', 'date', 'file_index', 'latitude', 'levels', 'longitude', 'pressure_levels', 'pressure_weight',
              'sensor_zenith_angle', 'solar_zenith_angle', 'source_files', 'time', 'vertex_latitude',
              'vertex_longitude', 'vertices', 'xco2_averaging_kernel', 'xco2_qf_bitflag', 'xco2_qf_simple_bitflag',
              'xco2_quality_flag', 'co2_profile_apriori', 'xco2_apriori'],
        '/Retrieval': ['diverging_steps', 'iterations', 'surface_type', 'SigmaB'],
        '/Sounding': ['att_data_source', 'footprint', 'land_fraction', 'land_water_indicator', 'operation_mode',
                      'orbit', 'pma_azimuth_angle', 'pma_elevation_angle', 'sensor_azimuth_angle',
                      'solar_azimuth_angle', 'target_id', 'target_name']
    }

    logger.debug('Dropping variables that will be excluded from interpolation (ie, non-numeric values)')

    for group in drop_dims:
        if group in sam:
            sam[group] = sam[group].drop_vars(drop_dims[group], errors='ignore')

    lon_grid, lat_grid = np.mgrid[bbox_dict['min_lon']:bbox_dict['max_lon']:complex(0, cfg.grid['longitude']),
                                  bbox_dict['min_lat']:bbox_dict['max_lat']:complex(0, cfg.grid['latitude'])].astype(
        np.dtype('float32')
    )

    logger.debug('Building attribute and coordinate dictionaries')

    lat_attrs = {
        'long_name': 'latitude',
        'standard_name': 'latitude',
        'axis': 'Y',
        'units': 'degrees_north',
        'valid_min': -90.0,
        'valid_max': 90.0,
    }

    lon_attrs = {
        'long_name': 'longitude',
        'standard_name': 'longitude',
        'axis': 'X',
        'units': 'degrees_east',
        'valid_min': -180.0,
        'valid_max': 180.0,
    }

    time_attrs = {
        'long_name': 'time',
        'standard_name': 'time',
        'axis': 'T',
        'units': 'seconds since 1970-01-01 00:00:00',
        'comment': 'Day of the source L2 Lite file from which the data at this time slice was extracted at midnight UTC'
    }

    coords = {
        'longitude': ('longitude', lon_grid.T[0], lon_attrs),
        'latitude': ('latitude', lat_grid[0], lat_attrs),
        'time': ('time', time, time_attrs)
    }

    gridded_ds = {}

    logger.debug(f"Interpolating retained data variables to {cfg.grid['longitude']:,} by {cfg.grid['latitude']:,}"
                 f" grid")

    desired_method = cfg.grid_method(Processor.DEFAULT_INTERPOLATE_METHOD)

    if desired_method != 'nearest' and len(points) < 4:
        # If there are not enough points to interpolate with the desired method (linear and cubic require >= 4), fall
        # back to nearest or skip this slice
        logger.warning(f'Desired interpolation method \'{desired_method}\' not possible with the number of points '
                       f'present ({len(points)}). Defaulting to \'nearest\'')
        method = 'nearest'
    else:
        method = desired_method

    def interpolate(in_grp, grp: str, var_name, m):
        logger.debug(f'Interpolating variable {var_name} in group {grp}')

        input_a = in_grp[grp][var_name].to_numpy()

        grid = griddata(
            points,
            input_a,
            (lon_grid, lat_grid),
            method=m,
            # fill_value=in_grp[grp][var_name].attrs['missing_value']
            fill_value=np.nan
        )

        # I don't recall why I was pulling fill_value from attrs but then deleting the attrs...

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            logger.trace(f'stats: {np.min(input_a)} {np.nanmin(input_a)} {np.nanmax(input_a)} -> '
                         f'{np.min(grid)} {np.nanmin(grid)} {np.nanmax(grid)}')

        return [grid.T]

    for group in sam:
        if len(sam[group].data_vars) == 0:
            logger.debug(f'Skipping group {group} as it contains no data variables')
            continue

        gridded_ds[group] = xr.Dataset(
            data_vars={
                f'{PROCESSOR_PREFIX}{var_name}':
                    (('time', 'latitude', 'longitude'), interpolate(sam, group, var_name, method))
                for var_name in sam[group].data_vars
            },
            coords=coords,
        )

        for var in sam[group]:
            gridded_ds[group][f'{PROCESSOR_PREFIX}{var}'].attrs = sam[group][var].attrs
            if 'missing_value' in gridded_ds[group][f'{PROCESSOR_PREFIX}{var}'].attrs:
                del gridded_ds[group][f'{PROCESSOR_PREFIX}{var}'].attrs['missing_value']
            if '_FillValue' in gridded_ds[group][f'{PROCESSOR_PREFIX}{var}'].attrs:
                del gridded_ds[group][f'{PROCESSOR_PREFIX}{var}'].attrs['_FillValue']

    logger.debug('Completed interpolations to grid')

    gridded_ds['/'].attrs['interpolation_method'] = cfg.grid_method(Processor.DEFAULT_INTERPOLATE_METHOD)

    res_attr = cfg.grid.get('resolution_attr')

    if res_attr:
        gridded_ds['/'].attrs['resolution'] = res_attr

    return gridded_ds


def mask_data(sam, grid_ds, cfg: RunConfig) -> Dict[str, xr.Dataset] | None:
    if sam is None:
        return None

    if grid_ds is None:
        return None

    logger.debug('Constructing polygons to build mask')

    latitudes = grid_ds['/'].latitude.to_numpy()
    longitudes = grid_ds['/'].longitude.to_numpy()

    geo_mask = np.full((len(latitudes), len(longitudes)), False)

    lon_len = longitudes[1] - longitudes[0]
    lat_len = latitudes[1] - latitudes[0]

    scaling = cfg.mask_scaling
    scaling = min(max(scaling, 1), 1.5)

    logger.trace(f'Footprint scaling factor: {scaling}')
    logger.trace(f'Creating bounding polys for region of {len(sam["/"].vertex_latitude):,} footprints')

    for lats, lons in zip(
            sam['/'].vertex_latitude.to_numpy(),
            sam['/'].vertex_longitude.to_numpy()
    ):
        vertices = [(lons[i].item(), lats[i].item()) for i in range(len(lats))]
        vertices.append((lons[0].item(), lats[0].item()))
        if scaling != 1.0:
            p: Polygon = scale(Polygon(vertices), scaling, scaling)
        else:
            p = Polygon(vertices)

        logger.trace(f'Created poly with bbox {p.bounds}')

        indices = []

        minx, miny, maxx, maxy = p.bounds

        # Expand bounds to fill pixels
        minx -= (lon_len / 2)
        miny -= (lat_len / 2)
        maxx += (lon_len / 2)
        maxy += (lat_len / 2)

        indices.append((
            np.argwhere(np.logical_and(miny <= latitudes, latitudes <= maxy)),
            np.argwhere(np.logical_and(minx <= longitudes, longitudes <= maxx)),
            p
        ))

        n_lats = sum([len(ind[0]) for ind in indices])
        n_lons = sum([len(ind[1]) for ind in indices])
        n_pts = sum([len(ind[0]) * len(ind[1]) for ind in indices])

        logger.trace(f'Checking for polys in ({p.bounds})')
        logger.trace(f'Applying bounding poly to geo mask across {n_lats:,} latitudes, {n_lons:,} '
                     f'longitudes. {n_pts:,} total points.')

        valid_points = 0

        for lat_indices, lon_indices, poly in indices:
            for lon_i in lon_indices:
                for lat_i in lat_indices:
                    lon_i = tuple(lon_i)
                    lat_i = tuple(lat_i)

                    if geo_mask[lat_i][lon_i]:
                        continue

                    lon = longitudes[lon_i]
                    lat = latitudes[lat_i]
                    grid_poly = box(lon - lon_len, lat - lat_len, lon + lon_len, lat + lat_len)

                    valid = grid_poly.intersects(poly)

                    if valid:
                        geo_mask[lat_i][lon_i] = True
                        valid_points += 1

    # Apply mask RIGHT AWAY, seems to be BAD for memory otherwise
    mask = np.array([geo_mask])

    logger.debug('Applying mask to dataset')

    for group in grid_ds:
        for var in grid_ds[group].data_vars:
            grid_ds[group][var] = grid_ds[group][var].where(mask)

    return grid_ds


class OCO3SamProcessor(Processor):
    @classmethod
    def process_input(
            cls,
            input_file,
            cfg: RunConfig,
            temp_dir,
            output_pre_qf=True,
    ):
        process_vars = Processor.determine_variables_to_load(
            NEEDED_VARS,
            DEFAULT_INCLUDED_VARS,
            cfg.variables('oco3')
        )

        additional_params = {'variables': process_vars}

        if isinstance(input_file, dict):
            path = input_file['path']

            additional_params['s3_auth'] = {
                'accessKeyID': input_file.get('accessKeyID'),
                'secretAccessKey': input_file.get('secretAccessKey')
            }

            if 'urs' in input_file:
                additional_params['s3_auth']['urs'] = input_file['urs']
                additional_params['s3_auth']['urs_user'] = input_file.get('urs_user')
                additional_params['s3_auth']['urs_pass'] = input_file.get('urs_pass')

            if 'region' in input_file:
                additional_params['s3_region'] = input_file['region']
            else:
                additional_params['s3_region'] = 'us-west-2'

            input_file = path  # It's used later...
        else:
            path = input_file

        logger.info(f'Processing OCO-3 input at {path}')

        try:
            with GranuleReader(path, GROUPS, **additional_params) as ds:
                mode_array = ds['/Sounding']['operation_mode']
                target_array = ds['/Sounding']['target_id']

                logger.info('Splitting into individual SAM regions')

                region_slices = []
                in_region = False
                start = None
                target_id = None

                n_merged = 0

                def merge_last_or_append(slices: List[Tuple[slice, int]], item: Tuple[slice, int], margin=2):
                    if len(slices) == 0:
                        slices.append(item)
                    else:
                        last = slices[-1]

                        if abs(last[0].stop - item[0].start) < margin and last[1] == item[1]:
                            slices[-1] = (slice(last[0].start, item[0].stop), last[1])
                            logger.debug(f'Merged region {last} and {item} to {slices[-1]} since they are close and '
                                         f'similar')
                        else:
                            slices.append(item)

                for i, (mode, target) in enumerate(zip(mode_array.to_numpy(), target_array.to_numpy())):
                    if mode.item() == OPERATION_MODE_SAM:
                        if not in_region:
                            in_region = True
                            target_id = target
                            start = i
                        elif target != target_id:
                            if merge_last_or_append(region_slices, (slice(start, i), target_id)):
                                n_merged += 1
                            target_id = target
                            start = i

                    if mode.item() != OPERATION_MODE_SAM:
                        if in_region:
                            if merge_last_or_append(region_slices, (slice(start, i), target_id)):
                                n_merged += 1
                            target_id = None
                            in_region = False

                if in_region:
                    if merge_last_or_append(region_slices, (slice(start, i + 1), target_id)):
                        n_merged += 1

                n_sams = len(region_slices)

                logger.info('Splitting into individual target regions')

                in_region = False
                start = None
                target_id = None

                for i, (mode, target) in enumerate(zip(mode_array.to_numpy(), target_array.to_numpy())):
                    if mode.item() == OPERATION_MODE_TARGET:
                        if not in_region:
                            in_region = True
                            target_id = target
                            start = i
                        elif target != target_id:
                            if merge_last_or_append(region_slices, (slice(start, i), target_id)):
                                n_merged += 1
                            target_id = target
                            start = i

                    if mode.item() != OPERATION_MODE_TARGET:
                        if in_region:
                            if merge_last_or_append(region_slices, (slice(start, i), target_id)):
                                n_merged += 1
                            target_id = None
                            in_region = False

                if in_region:
                    if merge_last_or_append(region_slices, (slice(start, i + 1), target_id)):
                        n_merged += 1

                n_targets = len(region_slices) - n_sams

                extracted_sams_pre_qf = []
                extracted_sams_post_qf = []

                logger.info(f'Identified {n_sams} SAM regions and {n_targets} Target regions ({n_merged} merges)')
                logger.info('Filtering out bad quality soundings in selected ranges')

                for s, target in region_slices:
                    if target in ['Missing', 'missing']:
                        logger.error(f'Region from sounding_id range {ds["/"].sounding_id[s.start].item()} to '
                                     f'{ds["/"].sounding_id[s.stop-1].item()} does not have a defined target and will '
                                     f'need to be excluded')
                        continue

                    sam_group = {group: ds[group].isel(sounding_id=s) for group in ds if len(ds[group].data_vars) > 0}

                    if output_pre_qf:
                        extracted_sams_pre_qf.append((sam_group, target))

                    quality = sam_group['/'].xco2_quality_flag == 0

                    # If this SAM has no good data
                    if not any(quality):
                        logger.info(f'Dropping region from sounding_id range '
                                    f'{ds["/"].sounding_id[s.start].item()} to '
                                    f'{ds["/"].sounding_id[s.stop-1].item()} ({len(quality):,} soundings) as there are '
                                    f'no points flagged as good.')
                        continue

                    extracted_sams_post_qf.append(
                        ({group: sam_group[group].where(quality, drop=True) for group in sam_group}, target)
                    )

                if output_pre_qf:
                    logger.info(f'Extracted {len(extracted_sams_pre_qf)} regions total, {len(extracted_sams_post_qf)} '
                                f'regions with good data')
                else:
                    logger.info(f'Extracted {len(extracted_sams_post_qf)} regions with good data')

                processed_sams_pre_qf = []
                processed_sams_post_qf = []

                with open(cfg.target_file_3) as fp:
                    target_bounds = json.load(fp)

                if output_pre_qf:
                    if len(extracted_sams_pre_qf) > 0:
                        for i, (sam, target) in enumerate(extracted_sams_pre_qf, start=1):
                            gridding_start = datetime.now()

                            processed_data = (
                                mask_data(
                                    sam,
                                    fit_data_to_grid(
                                        sam,
                                        target,
                                        target_bounds,
                                        cfg
                                    ),
                                    cfg
                                ), target
                            )

                            if cfg.drop_empty and (processed_data[0] is None or is_nan(processed_data[0])):
                                logger.warning(f'Dropped empty pre-qf slice for target {target} '
                                               f'({target_bounds.get(target, dict()).get("name", "")}) in '
                                               f'{datetime.now() - gridding_start}. [{i}/{len(extracted_sams_pre_qf)}]')
                            else:
                                logger.info(f'Gridded unfiltered region for target: {target} '
                                            f'({target_bounds.get(target, dict()).get("name", "")}) in '
                                            f'{datetime.now() - gridding_start}. [{i}/{len(extracted_sams_pre_qf)}]')

                                processed_sams_pre_qf.append(processed_data)
                    else:
                        logger.info('No pre-qf data to extract.')

                    extracted_sams_pre_qf.clear()
                    del extracted_sams_pre_qf

                logger.debug('Fitting filtered data to output grid')

                if len(extracted_sams_post_qf) > 0:
                    for i, (sam, target) in enumerate(extracted_sams_post_qf, start=1):
                        gridding_start = datetime.now()

                        processed_data = (
                            mask_data(
                                sam,
                                fit_data_to_grid(
                                    sam,
                                    target,
                                    target_bounds,
                                    cfg
                                ),
                                cfg
                            ), target
                        )

                        if cfg.drop_empty and (processed_data[0] is None or is_nan(processed_data[0])):
                            logger.warning(f'Dropped empty post-qf slice for target {target} '
                                           f'({target_bounds.get(target, dict()).get("name", "")}) in '
                                           f'{datetime.now() - gridding_start}. [{i}/{len(extracted_sams_post_qf)}]')
                        else:
                            logger.info(f'Gridded filtered region for target: {target} '
                                        f'({target_bounds.get(target, dict()).get("name", "")}) in '
                                        f'{datetime.now() - gridding_start}. [{i}/{len(extracted_sams_post_qf)}]')
                            processed_sams_post_qf.append(processed_data)
                else:
                    logger.info('No post-qf data to extract.')

                extracted_sams_post_qf.clear()
                del extracted_sams_post_qf

                logger.info(f'Finished processing input at {path}')

                return processed_sams_pre_qf, processed_sams_post_qf, True, path
        except ReaderException:
            return None, None, False, path
        except Exception as err:
            logger.error(f'Process task for {path} failed')
            logger.exception(err)
            raise

    @staticmethod
    def _empty_dataset(date: datetime, cfg):
        raise NotImplementedError()


PROCESSORS['local']['oco3'] = OCO3SamProcessor
