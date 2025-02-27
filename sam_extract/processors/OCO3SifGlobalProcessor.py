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
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple

import numpy as np
import xarray as xr
from sam_extract.exceptions import *
from sam_extract.processors import Processor
from sam_extract.processors.Processor import PROCESSORS
from sam_extract.readers import GranuleReader
from sam_extract.runconfig import RunConfig
from sam_extract.targets import FILL_VALUE as TARGET_FILL
from sam_extract.targets import extract_id, determine_id_type
from sam_extract.utils import INTERP_SEMA, get_f_xi, get_xi, open_zarr_group
from sam_extract.writers import ZarrWriter
from sam_extract.writers.ZarrWriter import ENCODINGS
from scipy.interpolate import griddata
from shapely.affinity import scale
from shapely.geometry import Polygon, box, MultiPolygon
from shapely.ops import unary_union

logger = logging.getLogger(__name__)

OPERATION_MODE_SAM = 3
OPERATION_MODE_TARGET = 2

PROCESSOR_PREFIX = 'OCO3_SIF_global'

GROUPS = {
    '/': None,
    # '/Cloud': 'Cloud',
    # '/Geolocation': 'Geolocation',
    '/Metadata': 'Metadata',
    # '/Meteo': 'Meteo',
    # '/Offset': 'Offset',
    # '/Science': 'Science',
    '/Sequences': 'Sequences',
}

NEEDED_VARS = {
    '/': ['Latitude', 'Longitude', 'Delta_Time', 'Latitude_Corners', 'Longitude_Corners', 'sounding_dim',
          'Quality_Flag'],
    '/Metadata': ['MeasurementMode'],
    '/Sequences': ['SequencesId', 'SequencesIndex', 'SequencesName'],
}

DEFAULT_INCLUDED_VARS = {
    '/': ['Daily_SIF_757nm']
}

SIF_EPOCH = datetime(1990, 1, 1)


def tr(s: str, chars: str = None):
    return re.sub(rf'([{chars}])(\1+)', r'\1', s)


def fit_data_to_grid(sams, cfg: RunConfig):
    logger.debug('Concatenating extracted datasets for interpolation')

    if len(sams) == 0:
        return None

    interp_ds = {group: xr.concat([sam[group] for sam in sams], 'sounding_dim') for group in sams[0]}

    lats = interp_ds['/'].Latitude.to_numpy()
    lons = interp_ds['/'].Longitude.to_numpy()
    time = SIF_EPOCH + timedelta(seconds=interp_ds['/'].Delta_Time[0].item())
    time = time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    time = np.array([time.timestamp()])

    points = list(zip(lons, lats))

    # Dimensions that will not be interpolated and fit to grid
    drop_dims = {
        '/': ['Delta_Time', 'Quality_Flag', 'SAz', 'Latitude', 'Latitude_Corners', 'SZA', 'Longitude',
              'Longitude_Corners', 'VAz', 'VZA'],
        '/Metadata': ['BuildId', 'CollectionLabel', 'FootprintId', 'MeasurementMode', 'OrbitId', 'SoundingId'],
        '/Sequences': ['SegmentsIndex', 'SequencesId', 'SequencesIndex', 'SequencesMode', 'SequencesName']
    }

    logger.info('Dropping variables that will be excluded from interpolation (ie, non-numeric values)')

    for group in drop_dims:
        if group in interp_ds:
            interp_ds[group] = interp_ds[group].drop_vars(drop_dims[group], errors='ignore')

    _, lon_coord, lat_coord = get_xi(cfg)

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
        'longitude': ('longitude', lon_coord, lon_attrs),
        'latitude': ('latitude', lat_coord, lat_attrs),
        'time': ('time', time, time_attrs)
    }

    gridded_ds = {}

    logger.info(f"Interpolating retained data variables to {cfg.grid['longitude']:,} by {cfg.grid['latitude']:,} grid")

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
        logger.info(f'Interpolating variable {var_name} in group {grp}')

        xi = np.load(get_f_xi(), mmap_mode='r')

        grid = griddata(
            points,
            in_grp[grp][var_name].to_numpy(),
            xi,
            method=m,
            fill_value=in_grp[grp][var_name].attrs['missing_value']
        ).transpose()

        try:
            getattr(xi, '_mmap').close()
        except:
            pass
        finally:
            del xi

        return [grid]

    for group in interp_ds:
        if len(interp_ds[group].data_vars) == 0:
            logger.debug(f'Skipping group {group} as it contains no data variables')
            continue

        with INTERP_SEMA:
            logger.debug(f'Acquired semaphore {repr(INTERP_SEMA)}')

            gridded_ds[group] = xr.Dataset(
                data_vars={
                    f'{PROCESSOR_PREFIX}_{var_name}':
                        (('time', 'latitude', 'longitude'), interpolate(interp_ds, group, var_name, method))
                    for var_name in interp_ds[group].data_vars
                },
                coords=coords,
            )

        logger.debug(f'Released semaphore {repr(INTERP_SEMA)}')

        for var in interp_ds[group]:
            gridded_ds[group][f'{PROCESSOR_PREFIX}_{var}'].attrs = interp_ds[group][var].attrs

    logger.info('Completed interpolations to grid')

    gridded_ds['/'].attrs['interpolation_method'] = cfg.grid_method(Processor.DEFAULT_INTERPOLATE_METHOD)

    res_attr = cfg.grid.get('resolution_attr')

    if res_attr:
        gridded_ds['/'].attrs['resolution'] = res_attr

    return gridded_ds


def mask_data(sams, targets, op_modes, grid_ds, cfg: RunConfig):
    if sams is None:
        return None

    if grid_ds is None:
        return None

    assert len(sams) == len(targets)

    logger.info('Constructing polygons to build mask')

    latitudes = grid_ds['/'].latitude.to_numpy()
    longitudes = grid_ds['/'].longitude.to_numpy()

    sam_polys = []

    scaling = cfg.mask_scaling
    scaling = min(max(scaling, 1), 1.5)

    logger.debug(f'Footprint scaling factor: {scaling}')

    meta_dict = {}

    for i, (sam, target, op_mode) in enumerate(zip(sams, targets, op_modes)):
        logger.info(f'Creating bounding polys for region of {len(sam["/"].Latitude_Corners):,} footprints '
                    f'[{i+1}/{len(sams)}]')

        footprint_polygons = []

        for lats, lons, (tid, tn) in zip(
                sam['/'].Latitude_Corners.to_numpy(),
                sam['/'].Longitude_Corners.to_numpy(),
                target,
        ):
            vertices = [(lons[i].item(), lats[i].item()) for i in range(len(lats))]
            vertices.append((lons[0].item(), lats[0].item()))
            if scaling != 1.0:
                p: Polygon = scale(Polygon(vertices), scaling, scaling)
            else:
                p = Polygon(vertices)

            tid, tn = tid, tn

            if isinstance(tid, np.ndarray):
                tid = tid.item()

            if isinstance(tn, np.ndarray):
                tn = tn.item()

            meta_dict[p.wkt] = (tid, tn, op_mode)
            footprint_polygons.append(p)

        if scaling != 1.0:
            bounding_poly = unary_union(footprint_polygons)
        else:
            bounding_poly = MultiPolygon(footprint_polygons)

        logger.debug(f'Created poly with bbox {bounding_poly.bounds}')

        sam_polys.append((bounding_poly.bounds, footprint_polygons))

    logger.info('Producing geo mask from generated polys')

    geo_mask = np.full((len(latitudes), len(longitudes)), False)
    target_ids = np.full((len(latitudes), len(longitudes)), TARGET_FILL, dtype='int')
    target_types = np.full((len(latitudes), len(longitudes)), TARGET_FILL, dtype='byte')
    operation_mode = np.full((len(latitudes), len(longitudes)), TARGET_FILL, dtype='byte')

    lon_len = longitudes[1] - longitudes[0]
    lat_len = latitudes[1] - latitudes[0]

    for i, (bounds, polys) in enumerate(sam_polys):
        indices = []

        logger.debug(f'Determining coordinates from {len(polys)} sub-polygons in bounds {bounds}')

        for poly in polys:
            minx, miny, maxx, maxy = poly.bounds

            # Expand bounds to fill pixels
            minx -= (lon_len / 2)
            miny -= (lat_len / 2)
            maxx += (lon_len / 2)
            maxy += (lat_len / 2)

            indices.append((
                np.argwhere(np.logical_and(miny <= latitudes, latitudes <= maxy)),
                np.argwhere(np.logical_and(minx <= longitudes, longitudes <= maxx)),
                meta_dict.get(poly.wkt, ('UNKNOWN', 'UNKNOWN')),
                poly
            ))

        n_lats = sum([len(ind[0]) for ind in indices])
        n_lons = sum([len(ind[1]) for ind in indices])
        n_pts = sum([len(ind[0]) * len(ind[1]) for ind in indices])

        logger.debug(f'Checking for polys in ({bounds})')
        logger.info(f'Applying bounding poly to geo mask across {n_lats:,} latitudes, {n_lons:,} '
                    f'longitudes. {n_pts:,} total points. [{i+1}/{len(sam_polys)}]')

        valid_points = 0

        for lat_indices, lon_indices, (tid, tn, op_mode), poly in indices:
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

                        id_type = determine_id_type(tid)

                        if target_ids[lat_i][lon_i] == TARGET_FILL:
                            target_ids[lat_i][lon_i] = extract_id(tid, id_type)

                        if target_types[lat_i][lon_i] == TARGET_FILL:
                            target_types[lat_i][lon_i] = id_type

                        if operation_mode[lat_i][lon_i] == TARGET_FILL:
                            operation_mode[lat_i][lon_i] = op_mode

        logger.debug(f'Finished applying polys in ({bounds}) to geo mask. Added {valid_points:,} valid points')

    # Apply mask RIGHT AWAY, seems to be BAD for memory otherwise

    mask = np.array([geo_mask])

    logger.info('Applying mask to dataset')

    for group in grid_ds:
        for var in grid_ds[group].data_vars:
            grid_ds[group][var] = grid_ds[group][var].where(mask)

    logger.info('Adding target id and name variables to dataset')

    target_id_attrs = dict(
        units='none',
        long_name='OCO-3 Target (or SAM) ID numerical component',
        comment='ID number is derived for target types ECOSTRESS and SIF as they are non-numerical. Special value of 0 '
                'is used for non-numerical ids for other types'
    )

    target_type_attrs = dict(
        units='none',
        long_name='OCO-3 Target (or SAM) ID target type: 1=fossil, 2=ecostress, 3=sif, 4=volcano, 5=tccon, 6=other',
    )

    operation_mode_attrs = dict(
        units='none',
        long_name='OCO-3 Operation Mode: 2=Target, 4=Snapshot Area Map',
    )

    target_ids_da = xr.DataArray(
        data=np.array([target_ids], dtype='int'),
        coords={dim: grid_ds['/'].coords[dim] for dim in grid_ds['/'].coords},
        dims=[
            'time',
            'latitude',
            'longitude'
        ],
        name=f'{PROCESSOR_PREFIX}_target_id',
        attrs=target_id_attrs
    )

    target_types_da = xr.DataArray(
        data=np.array([target_types], dtype='byte'),
        coords={dim: grid_ds['/'].coords[dim] for dim in grid_ds['/'].coords},
        dims=[
            'time',
            'latitude',
            'longitude'
        ],
        name=f'{PROCESSOR_PREFIX}_target_type',
        attrs=target_type_attrs
    )

    operation_mode_da = xr.DataArray(
        data=np.array([operation_mode], dtype='byte'),
        coords={dim: grid_ds['/'].coords[dim] for dim in grid_ds['/'].coords},
        dims=[
            'time',
            'latitude',
            'longitude'
        ],
        name=f'{PROCESSOR_PREFIX}_operation_mode',
        attrs=operation_mode_attrs
    )

    grid_ds['/'][f'{PROCESSOR_PREFIX}_target_id'] = target_ids_da
    grid_ds['/'][f'{PROCESSOR_PREFIX}_target_type'] = target_types_da
    grid_ds['/'][f'{PROCESSOR_PREFIX}_operation_mode'] = operation_mode_da

    return grid_ds


class OCO3SamSIFGlobalProcessor(Processor):
    @classmethod
    def process_input(
            cls,
            input_file,
            cfg: RunConfig,
            temp_dir,
            output_pre_qf=True,
    ) -> Tuple[Optional[Dict[str, xr.Dataset]], Optional[Dict[str, xr.Dataset]], bool, str]:
        process_vars = Processor.determine_variables_to_load(
            NEEDED_VARS,
            DEFAULT_INCLUDED_VARS,
            cfg.variables('oco3_sif')
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

        logger.info(f'Processing OCO-3 SIF input at {path}')

        try:
            with GranuleReader(path, GROUPS, **additional_params) as ds:
                mode_array = ds['/Metadata']['MeasurementMode']

                logger.info('Splitting into individual SAM regions')

                n_sams = 0
                n_targets = 0

                region_slices = []
                in_region = False
                start = None

                for i, mode in enumerate(mode_array.to_numpy()):
                    if mode.item() == OPERATION_MODE_SAM:
                        if not in_region:
                            in_region = True
                            start = i
                    else:
                        if in_region:
                            in_region = False
                            region_slices.append((slice(start, i), OPERATION_MODE_SAM))
                            n_sams += 1

                if in_region:
                    region_slices.append((slice(start, i), OPERATION_MODE_SAM))
                    n_sams += 1

                logger.info('Splitting into individual target regions')

                in_region = False
                start = None

                for i, mode in enumerate(mode_array.to_numpy()):
                    if mode.item() == OPERATION_MODE_TARGET:
                        if not in_region:
                            in_region = True
                            start = i
                    else:
                        if in_region:
                            in_region = False
                            region_slices.append((slice(start, i), OPERATION_MODE_TARGET))
                            n_targets += 1

                if in_region:
                    region_slices.append((slice(start, i), OPERATION_MODE_TARGET))
                    n_targets += 1

                extracted_sams_pre_qf = []
                extracted_sams_post_qf = []

                extracted_targets_pre_qf = []
                extracted_targets_post_qf = []

                extracted_op_modes_pre_qf = []
                extracted_op_modes_post_qf = []

                logger.info(f'Identified {n_sams} SAM regions and {n_targets} Target regions')

                logger.info('Filtering out bad quality soundings in selected ranges')

                seq_ids = ds['/Sequences']['SequencesId'].values
                seq_names = ds['/Sequences']['SequencesName'].values

                for s, op_mode in region_slices:
                    sam_group = {group: ds[group].isel(sounding_dim=s) for group in ds if len(ds[group].data_vars) > 0}
                    seq_group = ds['/Sequences'].isel(sounding_dim=s)

                    seq_idx = seq_group['SequencesIndex'].values

                    tid_group = [(
                        seq_ids[int(idx)] if int(idx) >= 0 else 'none',
                        seq_names[int(idx)] if int(idx) >= 0 else 'none',
                    ) for idx in seq_idx]

                    if output_pre_qf:
                        extracted_sams_pre_qf.append(sam_group)
                        extracted_targets_pre_qf.append(tid_group)
                        extracted_op_modes_pre_qf.append(op_mode)

                    quality = sam_group['/'].Quality_Flag

                    # There has to be a better way to combine these
                    best = quality == 0
                    good = quality == 1

                    quality[:] = np.logical_or(best.values, good.values)

                    # If this SAM has no good data
                    if not any(quality):
                        logger.info(f'Dropping region from sounding_dim range '
                                    f'{ds["/"].sounding_dim[s.start].item()} to '
                                    f'{ds["/"].sounding_dim[s.stop].item()} ({len(quality):,} soundings) as there are'
                                    f' no points flagged as good.')
                        continue

                    extracted_sams_post_qf.append(
                        {group: sam_group[group].where(quality, drop=True) for group in sam_group}
                    )

                    seq_group = ds['/Sequences'].isel(sounding_dim=s).where(quality, drop=True)

                    seq_idx = seq_group['SequencesIndex'].values

                    tid_group = [(
                        seq_ids[int(idx)] if int(idx) >= 0 else 'none',
                        seq_names[int(idx)] if int(idx) >= 0 else 'none',
                    ) for idx in seq_idx]

                    extracted_targets_post_qf.append(
                        tid_group
                    )
                    extracted_op_modes_post_qf.append(op_mode)

                if output_pre_qf:
                    logger.info(f'Extracted {len(extracted_sams_pre_qf)} regions total, {len(extracted_sams_post_qf)} '
                                f'regions with good data')
                else:
                    logger.info(f'Extracted {len(extracted_sams_post_qf)} regions with good data')

                chunking: Tuple[int, int, int] = cfg.chunking

                if output_pre_qf:
                    logger.info('Fitting unfiltered data to output grid')

                    if len(extracted_sams_pre_qf) > 0:
                        gridded_groups_pre_qf = mask_data(
                            extracted_sams_pre_qf, extracted_targets_pre_qf, extracted_op_modes_pre_qf,
                            fit_data_to_grid(
                                extracted_sams_pre_qf,
                                cfg
                            ),
                            cfg
                        )
                    else:
                        logger.info('No pre-qf data to extract, creating an empty day of data')

                        time = SIF_EPOCH + timedelta(seconds=ds['/'].Delta_Time[0].item())
                        time = time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

                        gridded_groups_pre_qf = OCO3SamSIFGlobalProcessor.empty_dataset(time, cfg)

                    extracted_sams_pre_qf.clear()
                    extracted_targets_pre_qf.clear()
                    del extracted_sams_pre_qf, extracted_targets_pre_qf

                    if gridded_groups_pre_qf is not None:
                        temp_path_pre = os.path.join(temp_dir, 'pre_qf', os.path.basename(input_file)) + '.zarr'

                        logger.info('Outputting unfiltered product slice to temporary Zarr array')

                        writer = ZarrWriter(str(temp_path_pre), chunking, overwrite=True, verify=False)
                        writer.write(gridded_groups_pre_qf)

                        del gridded_groups_pre_qf

                        ret_pre_qf = open_zarr_group(temp_path_pre, 'local', None, decode_times=False)
                    else:
                        ret_pre_qf = None

                logger.info('Fitting filtered data to output grid')

                if len(extracted_sams_post_qf) > 0:
                    gridded_groups_post_qf = mask_data(
                        extracted_sams_post_qf, extracted_targets_post_qf, extracted_op_modes_post_qf,
                        fit_data_to_grid(
                            extracted_sams_post_qf,
                            cfg
                        ),
                        cfg
                    )
                else:
                    logger.info('No post-qf data to extract, creating an empty day of data')

                    time = SIF_EPOCH + timedelta(seconds=ds['/'].Delta_Time[0].item())
                    time = time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

                    gridded_groups_post_qf = OCO3SamSIFGlobalProcessor.empty_dataset(time, cfg)

                extracted_sams_post_qf.clear()
                extracted_targets_post_qf.clear()
                del extracted_sams_post_qf, extracted_targets_post_qf

                if gridded_groups_post_qf is not None:
                    temp_path_post = os.path.join(temp_dir, 'post_qf', os.path.basename(input_file)) + '.zarr'

                    logger.info('Outputting filtered SAM product slice to temporary Zarr array')

                    writer = ZarrWriter(str(temp_path_post), chunking, overwrite=True, verify=False)
                    writer.write(gridded_groups_post_qf)

                    ret_post_qf = open_zarr_group(temp_path_post, 'local', None, decode_times=False)
                else:
                    ret_post_qf = None

                logger.info(f'Finished processing input at {path}')

                return ret_pre_qf, ret_post_qf, True, path
        except ReaderException:
            return None, None, False, path
        except Exception as err:
            logger.error(f'Process task for {path} failed')
            logger.exception(err)
            raise

    @staticmethod
    def _empty_dataset(date: datetime, cfg: RunConfig):
        selected_variables = cfg.variables('oco3_sif')

        if len(selected_variables) == 0:
            selected_variables = DEFAULT_INCLUDED_VARS

        variables = {}

        for group in selected_variables:
            variables[group] = [f'{PROCESSOR_PREFIX}_{var}' for var in selected_variables[group]]

        variables.setdefault('/', []).extend([
            f'{PROCESSOR_PREFIX}_target_id',
            f'{PROCESSOR_PREFIX}_target_type',
            f'{PROCESSOR_PREFIX}_operation_mode',
        ])

        _, lon_coord, lat_coord = get_xi(cfg)
        time = np.array([date.timestamp()])

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
            'longitude': ('longitude', lon_coord, lon_attrs),
            'latitude': ('latitude', lat_coord, lat_attrs),
            'time': ('time', time, time_attrs)
        }

        shape = (1, cfg.grid['latitude'], cfg.grid['longitude'])

        gridded_ds = {
            group: xr.Dataset(
                data_vars={
                    var: (
                        ('time', 'latitude', 'longitude'),
                        np.full(
                            shape,
                            ENCODINGS.get(group, {})[var]['_FillValue'] if var in ENCODINGS.get(group, {}) else np.nan,
                            ENCODINGS.get(group, {})[var]['dtype'] if var in ENCODINGS.get(group, {}) else 'float64'
                        )
                    ) for var in variables[group]
                },
                coords=coords
            )
            for group in variables
        }

        gridded_ds['/'].attrs['interpolation_method'] = cfg.grid_method(Processor.DEFAULT_INTERPOLATE_METHOD)

        res_attr = cfg.grid.get('resolution_attr')

        if res_attr:
            gridded_ds['/'].attrs['resolution'] = res_attr

        return gridded_ds


if '/' not in ENCODINGS:
    ENCODINGS['/'] = {}

ENCODINGS['/'].update({
    f'{PROCESSOR_PREFIX}_target_id': {'_FillValue': TARGET_FILL, 'dtype': 'int32'},
    f'{PROCESSOR_PREFIX}_target_type': {'_FillValue': TARGET_FILL, 'dtype': 'int8'},
    f'{PROCESSOR_PREFIX}_operation_mode': {'_FillValue': TARGET_FILL, 'dtype': 'int8'},
})

PROCESSORS['global']['oco3_sif'] = OCO3SamSIFGlobalProcessor
