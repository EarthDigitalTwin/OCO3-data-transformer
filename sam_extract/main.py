import argparse
import logging
import os.path
from tempfile import TemporaryDirectory
from typing import List, Optional
from datetime import datetime

import numpy as np
import xarray as xr
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from scipy.interpolate import griddata
from shapely.affinity import scale
from shapely.geometry import Polygon, box
from shapely.ops import unary_union
from yaml import load

from sam_extract.readers import GranuleReader
from sam_extract.writers import NetCDFWriter, ZarrWriter

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)


def __fit_data_to_grid(sams, cfg):
    """
    Interpolate the sam data onto a grid.
    :param sams: List of xr datasets for individual SAMs
    :param cfg: Config. Can set interpolation method + grid dims
    :return: xr
    """

    logger.info('Concatenating SAM datasets for interpolation')

    if len(sams) == 0:
        return None

    # interp_ds = {
    #     '/': xr.concat([sam['/'] for sam in sams], 'sounding_id'),
    #     '/Meteorology': xr.concat([sam['/Meteorology'] for sam in sams], 'sounding_id'),
    #     '/Preprocessors': xr.concat([sam['/Preprocessors'] for sam in sams], 'sounding_id'),
    #     '/Retrieval': xr.concat([sam['/Retrieval'] for sam in sams], 'sounding_id'),
    #     '/Sounding': xr.concat([sam['/Sounding'] for sam in sams], 'sounding_id'),
    # }

    interp_ds = {group: xr.concat([sam[group] for sam in sams], 'sounding_id') for group in sams[0]}

    lats = interp_ds['/'].latitude.to_numpy()
    lons = interp_ds['/'].longitude.to_numpy()
    time = np.array([datetime(*interp_ds['/'].date[0].to_numpy()[:3].astype(int)).timestamp()])

    points = list(zip(lons, lats))

    # Dimensions that will not be interpolated and fit to grid
    drop_dims = {
        '/': ['bands', 'date', 'file_index', 'latitude', 'levels', 'longitude', 'pressure_levels', 'pressure_weight',
              'sensor_zenith_angle', 'solar_zenith_angle', 'source_files', 'time', 'vertex_latitude',
              'vertex_longitude', 'vertices', 'xco2_averaging_kernel', 'xco2_qf_bitflag', 'xco2_qf_simple_bitflag',
              'xco2_quality_flag', 'co2_profile_apriori'],
        # '/Meteorology': ['psurf_apriori_o2a'],
        # '/Preprocessors': ['h2o_ratio'],
        '/Retrieval': ['diverging_steps', 'iterations', 'surface_type', 'SigmaB'],
        '/Sounding': ['att_data_source', 'footprint', 'land_fraction', 'land_water_indicator', 'operation_mode',
                      'orbit', 'pma_azimuth_angle', 'pma_elevation_angle', 'sensor_azimuth_angle',
                      'solar_azimuth_angle', 'target_id', 'target_name']
    }

    logger.info('Dropping variables that will be excluded from interpolation (ie, non-numeric values)')

    for group in drop_dims:
        if group in interp_ds:
            interp_ds[group] = interp_ds[group].drop_vars(drop_dims[group], errors='ignore')

    lon_grid, lat_grid = np.mgrid[-180:180:complex(0, cfg['grid']['longitude']),
                                  -90:90:complex(0, cfg['grid']['latitude'])].astype(np.dtype('float32'))

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
        'longitude': ('longitude', lon_grid.transpose()[0], lon_attrs),
        'latitude': ('latitude', lat_grid[0], lat_attrs),
        'time': ('time', time, time_attrs)
    }

    gridded_ds = {}

    logger.info(f"Interpolating retained data variables to {cfg['grid']['longitude']} by {cfg['grid']['latitude']}"
                f" grid")

    def __interpolate(in_grp, grp, var):
        logger.info(f'Interpolating variable {var} in group {grp}')
        return [griddata(points,
                in_grp[grp][var].to_numpy(),
                (lon_grid, lat_grid),
                method=cfg['grid'].get('method', 'cubic'),
                fill_value=in_grp[grp][var].attrs['missing_value'])]

    for group in interp_ds:
        gridded_ds[group] = xr.Dataset(
            data_vars={
                var_name: (('time', 'longitude', 'latitude'),
                           __interpolate(interp_ds, group, var_name))
                for var_name in interp_ds[group].data_vars
            },
            coords=coords,
            # attrs=interp_ds[group].attrs     # Add attrs at later point when ds is fully constructed
        )

        for var in gridded_ds[group]:
            gridded_ds[group][var].attrs = interp_ds[group][var].attrs

    logger.info('Completed interpolations to grid')

    return gridded_ds


def __mask_data(sams, grid_ds, cfg):
    """
    Construct a mask from the observation footprints. We should mask out areas on the grid
    outside of these footprints (+/- some tolerance? dwithin in shapely)

    Construct multipolys from each sam's footprints then build a mask array if each point's lat
    lon is within tolerance of that geometry

    Also here we should add the non-numeric data to the grid. Ex. SAM targets. If a point on the grid
    is valid, append the data to that point that makes it valid

    Return the grid after masking
    :param sams:
    :param cfg:
    :return:
    """

    if sams is None:
        return None

    logger.info('Constructing data mask')

    latitudes = grid_ds['/'].latitude.to_numpy()  # .tolist()
    longitudes = grid_ds['/'].longitude.to_numpy()  # .tolist()

    sam_polys = []

    for sam in sams:
        logger.info(f'Creating bounding poly for SAM of {len(sam["/"].vertex_latitude):,} footprints')

        footprint_polygons = []

        for lats, lons in zip(sam['/'].vertex_latitude, sam['/'].vertex_longitude):
            v = [(lons[i].item(), lats[i].item()) for i in range(len(lats))]
            v.append((lons[0].item(), lats[0].item()))
            footprint_polygons.append(scale(Polygon(v), 1.1, 1.1))

        bounding_poly = unary_union(footprint_polygons)

        sam_polys.append(bounding_poly)

    logger.info('Producing geo mask from SAM polys')

    geo_mask = np.full((len(longitudes), len(latitudes)), False)

    lon_len = longitudes[1] - longitudes[0]
    lat_len = latitudes[1] - latitudes[0]

    for poly in sam_polys:
        minx, miny, maxx, maxy = poly.bounds

        lat_indices = np.argwhere(np.logical_and(miny <= latitudes, latitudes <= maxy))
        lon_indices = np.argwhere(np.logical_and(minx <= longitudes, longitudes <= maxx))

        for lon_i in lon_indices:
            for lat_i in lat_indices:
                lon_i = tuple(lon_i)
                lat_i = tuple(lat_i)

                lon = longitudes[lon_i]
                lat = latitudes[lat_i]
                grid_poly = box(lon - lon_len, lat - lat_len, lon + lon_len, lat + lat_len)

                geo_mask[lon_i][lat_i] = geo_mask[lon_i][lat_i] or grid_poly.intersects(poly)

    mask = np.array([geo_mask])

    logger.info('Applying mask to dataset')

    for group in grid_ds:
        for var in grid_ds[group].data_vars:
            grid_ds[group][var] = grid_ds[group][var].where(
                mask,
                # other=grid_ds[group][var].attrs['missing_value']
            )

    return grid_ds


def process_input(input_url,
                  cfg,
                  temp_dir,
                  input_region=None,
                  output_pre_qf=True,
                  exclude_groups: Optional[List[str]] = None):
    additional_params = {'drop_dims': cfg['drop-dims']}

    if exclude_groups is None:
        exclude_groups = []

    if '/' in exclude_groups:
        raise ValueError('Cannot exclude root group')

    if cfg['input']['type'] == 'aws':
        additional_params['s3_region'] = input_region

    path = input_url

    logger.info(f'Processing input at {path}')
    logger.info('Opening granule')

    with GranuleReader(path, **additional_params) as ds:
        mode_array = ds['/Sounding']['operation_mode']

        logger.info('Splitting into individual SAM groups')

        sam_slices = []
        sam = False

        start = None
        for i, v in enumerate(mode_array.to_numpy()):
            if v.item() == 4:
                if not sam:
                    sam = True
                    start = i
            else:
                if sam:
                    sam = False
                    sam_slices.append(slice(start, i))

        if sam:
            sam_slices.append(slice(start, i))

        extracted_sams_pre_qf = []
        extracted_sams_post_qf = []

        logger.info('Filtering out bad quality soundings in SAM ranges')

        for s in sam_slices:
            sam_group = {group: ds[group].isel(sounding_id=s) for group in ds if group not in exclude_groups}

            if output_pre_qf:
                extracted_sams_pre_qf.append(sam_group)

            quality = sam_group['/'].xco2_quality_flag == 0

            # If this SAM has no good data
            if not any(quality):
                logger.info(f'Dropping SAM from sounding_id range '
                            f'{ds["/"].sounding_id[s.start].item()} to '
                            f'{ds["/"].sounding_id[s.stop].item()} ({len(quality):,} soundings) as there are no '
                            f'points flagged as good.')
                continue

            extracted_sams_post_qf.append({group: sam_group[group].where(quality, drop=True) for group in sam_group})

        if output_pre_qf:
            logger.info(f'Extracted {len(extracted_sams_pre_qf)} SAMs total, {len(extracted_sams_post_qf)} SAMs with '
                        f'good data')
        else:
            logger.info(f'Extracted {len(extracted_sams_post_qf)} SAMs with good data')

        if output_pre_qf:
            logger.info('Fitting unfiltered SAM data to output grid')

            gridded_groups_pre_qf = __mask_data(
                extracted_sams_pre_qf,
                __fit_data_to_grid(
                    extracted_sams_pre_qf,
                    cfg
                ),
                cfg
            )

            if gridded_groups_pre_qf is not None:
                temp_path_pre = os.path.join(temp_dir, 'pre_qf', os.path.basename(input_url)) + '.zarr'

                logger.info('Outputting unfiltered SAM product slice to temporary Zarr array')

                writer = ZarrWriter(temp_path_pre, (5, 250, 250), overwrite=True, verify=False)
                writer.write(gridded_groups_pre_qf)

                del gridded_groups_pre_qf

                ret_pre_qf = ZarrWriter.open_zarr_group(temp_path_pre, 'local', None)
            else:
                ret_pre_qf = None

        logger.info('Fitting filtered SAM data to output grid')

        gridded_groups_post_qf = __mask_data(
            extracted_sams_post_qf,
            __fit_data_to_grid(
                extracted_sams_post_qf,
                cfg
            ),
            cfg
        )

        if gridded_groups_post_qf is not None:
            temp_path_post = os.path.join(temp_dir, 'post_qf', os.path.basename(input_url)) + '.zarr'

            logger.info('Outputting filtered SAM product slice to temporary Zarr array')

            writer = ZarrWriter(temp_path_post, (5, 250, 250), overwrite=True, verify=False)
            writer.write(gridded_groups_post_qf)

            ret_post_qf = ZarrWriter.open_zarr_group(temp_path_post, 'local', None)
        else:
            ret_post_qf = None

        logger.info(f'Finished processing input at {path}')

        return ret_pre_qf, ret_post_qf


def __merge_groups(groups):
    logger.info(f'Merging {len(groups)} interpolated groups')

    groups = [group for group in groups if group is not None]

    if len(groups) == 0:
        return None

    return {group: xr.concat([g[group] for g in groups], dim='time').sortby('time') for group in groups[0]}

    # return {
    #     '/': xr.concat([g['/'] for g in groups], dim='time').sortby('time'),
    #     '/Meteorology': xr.concat([g['/Meteorology'] for g in groups], dim='time').sortby('time'),
    #     '/Preprocessors': xr.concat([g['/Preprocessors'] for g in groups], dim='time').sortby('time'),
    #     '/Retrieval': xr.concat([g['/Retrieval'] for g in groups], dim='time').sortby('time'),
    #     '/Sounding': xr.concat([g['/Sounding'] for g in groups], dim='time').sortby('time'),
    # }


def main(cfg):
    if cfg['input']['type'] == 'aws':
        pass
        # TODO monitoring code here
    else:
        in_files = [
            # "../test_data/jun22/oco3_LtCO2_220531_B10400Br_220929040438s.nc4",

            # "../test_data/jun22/oco3_LtCO2_220601_B10400Br_220929042003s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220602_B10400Br_220929042003s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220603_B10400Br_220929042003s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220604_B10400Br_220929042003s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220605_B10400Br_220929042003s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220606_B10400Br_220929042047s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220607_B10400Br_220929042103s.nc4",

            # "../test_data/jun22/oco3_LtCO2_220608_B10400Br_220929042114s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220609_B10400Br_220929042122s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220610_B10400Br_220929042125s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220611_B10400Br_220929042205s.nc4",

            # "../test_data/jun22/oco3_LtCO2_220612_B10400Br_220929042219s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220613_B10400Br_220929042220s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220614_B10400Br_220929042227s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220615_B10400Br_220929042230s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220616_B10400Br_220929042311s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220617_B10400Br_220929042318s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220618_B10400Br_220929042323s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220619_B10400Br_220929042331s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220622_B10400Br_220929042338s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220623_B10400Br_220929042350s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220624_B10400Br_220929042413s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220625_B10400Br_220929042419s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220626_B10400Br_220929042421s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220627_B10400Br_220929042425s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220628_B10400Br_220929042510s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220629_B10400Br_220929042521s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220630_B10400Br_221003210853s.nc4",
            # "../test_data/jun22/oco3_LtCO2_220701_B10400Br_221004062104s.nc4",

            "../test_data/la/oco3_LtCO2_200303_B10400Br_220318000013s.nc4",
            "../test_data/la/oco3_LtCO2_200505_B10400Br_220318001036s.nc4",
            "../test_data/la/oco3_LtCO2_200527_B10400Br_220318001255s.nc4",
            "../test_data/la/oco3_LtCO2_200814_B10400Br_220318002549s.nc4",
            "../test_data/la/oco3_LtCO2_210325_B10400Br_220318010127s.nc4",
            "../test_data/la/oco3_LtCO2_220218_B10400Br_220505141844s.nc4",
            "../test_data/la/oco3_LtCO2_220813_B10400Br_221010202453s.nc4",
            "../test_data/la/oco3_LtCO2_221028_B10400Br_221205203441s.nc4",
        ]

        with TemporaryDirectory(prefix='oco-sam-extract-', suffix='-zarr-scratch', ignore_cleanup_errors=True) as td:
            exclude = [
                '/Preprocessors',
                '/Meteorology',
                '/Sounding'
            ]

            process = partial(process_input, cfg=cfg, temp_dir=td, input_region=None, exclude_groups=exclude)

            with ThreadPoolExecutor(max_workers=cfg.get('max-workers'), thread_name_prefix='process-worker') as pool:
                processed_groups_pre = []
                processed_groups_post = []

                for result_pre, result_post in pool.map(process, in_files):
                    processed_groups_pre.append(result_pre)
                    processed_groups_post.append(result_post)

            merged_pre = __merge_groups(processed_groups_pre)

            if merged_pre is not None:
                logger.info('Merged processed pre_qf data')

                zarr_writer = ZarrWriter(
                    'file:///Users/rileykk/oco3/oco-sam-extract/la_sams_sample_pre_qf.zarr',
                    (5, 250, 250),
                    overwrite=True
                )
                zarr_writer.write(merged_pre)
            else:
                logger.info('No pre_qf data generated')

            merged_post = __merge_groups(processed_groups_post)

            if merged_post is not None:
                logger.info('Merged processed post_qf data')

                zarr_writer = ZarrWriter(
                    'file:///Users/rileykk/oco3/oco-sam-extract/la_sams_sample_post_qf.zarr',
                    (5, 250, 250),
                    overwrite=True
                )
                zarr_writer.write(merged_post)
            else:
                logger.info('No post_qf data generated, all SAMs on these days may have been filtered out for bad qf')


def parse_args():
    parser = argparse.ArgumentParser()

    input_config = parser.add_argument_group('Config file', 'Use a config yaml file')

    local = parser.add_argument_group('Single input file', 'Process only a single input file with default '
                                                           'settings')

    local.add_argument('-f', '--input-file',
                       help='Path to local OCO-3 lite NetCDF file to process',
                       metavar='FILE',
                       dest='in_file')

    local.add_argument('-o', '--output',
                       help='Path to directory to store output zarr arrays',
                       metavar='DIR',
                       dest='out')

    input_config.add_argument('-i', help='Configuration yaml file', metavar='YAML', dest='cfg')

    args = parser.parse_args()

    if args.cfg and (args.out or args.in_file):
        raise ValueError('Must specify either single input file (+ output dir) or provide a config yaml file. Not both')
    elif not args.cfg and not all((args.out, args.in_file)):
        raise ValueError('If providing single input file, both input file and output dir must be provided')

    if args.cfg:
        with open(args.cfg) as f:
            cfg_yml = load(f, Loader=Loader)

        config_dict = {}

        try:
            output = cfg_yml['output']
            inp = cfg_yml['input']

            if output['local']:
                config_dict['output']['type'] = 'local'
                config_dict['output']['local'] = output['local']
            elif output['s3']['url']:
                if not output['s3']['region']:
                    output['s3']['region'] = 'us-west-2'

                config_dict['output']['type'] = 'aws'
                config_dict['output'] = {'s3': output['s3']}
            else:
                raise ValueError('No output params configured')

            if not any((inp['single-file'], inp['queue']['url'])):
                raise ValueError('No input params configured')

            if inp['queue']['url'] and not inp['queue']['region']:
                inp['queue']['region'] = 'us-west-2'

            if inp['single-file']:
                config_dict['input']['type'] = 'local'
                config_dict['input']['single-file'] = inp['single-file']
            else:
                config_dict['input']['type'] = 'aws'
                config_dict['input'] = {'queue': inp['queue']}

            config_dict['drop-dims'] = [(dim['group'], dim['name']) for dim in cfg_yml['drop-dims']]
        except KeyError as e:
            logger.exception(e)
            raise ValueError('Invalid configuration')
    else:
        # TODO Eventually I'll enable & implement single use S3

        config_dict = {
            'output': {
                'local': {
                    'path': args.out
                },
                'type': 'local'
            },
            'input': {
                'single-file': args.in_file,
                'type': 'local'
            },
            'drop-dims': [
                # ('/Meteorology', 'psurf_apriori_o2a'),
                ('/Meteorology', 'psurf_apriori_sco2'),
                ('/Meteorology', 'psurf_apriori_wco2'),
                ('/Meteorology', 'windspeed_u_met'),
                ('/Meteorology', 'windspeed_v_met'),
                ('/Preprocessors', 'xco2_weak_idp'),
                ('/Preprocessors', 'xco2_strong_idp'),
                ('/Preprocessors', 'max_declocking_o2a'),
                ('/Preprocessors', 'csstd_ratio_wco2'),
                ('/Preprocessors', 'dp_abp'),
                # ('/Preprocessors', 'h2o_ratio'),
                ('/Preprocessors', 'co2_ratio'),
                ('/Retrieval', 'surface_type'),
                ('/Retrieval', 'psurf'),
                ('/Retrieval', 'SigmaB'),
                ('/Retrieval', 'windspeed'),
                ('/Retrieval', 'windspeed_apriori'),
                ('/Retrieval', 'psurf_apriori'),
                ('/Retrieval', 't700'),
                ('/Retrieval', 'fs'),
                ('/Retrieval', 'fs_rel'),
                ('/Retrieval', 'tcwv'),
                ('/Retrieval', 'tcwv_apriori'),
                ('/Retrieval', 'tcwv_uncertainty'),
                ('/Retrieval', 'dp'),
                ('/Retrieval', 'dp_o2a'),
                ('/Retrieval', 'dp_sco2'),
                ('/Retrieval', 'dpfrac'),
                ('/Retrieval', 's31'),
                ('/Retrieval', 's32'),
                ('/Retrieval', 'co2_grad_del'),
                ('/Retrieval', 'dws'),
                ('/Retrieval', 'aod_fine'),
                ('/Retrieval', 'eof2_2_rel'),
                ('/Retrieval', 'aod_dust'),
                ('/Retrieval', 'aod_bc'),
                ('/Retrieval', 'aod_oc'),
                ('/Retrieval', 'aod_seasalt'),
                ('/Retrieval', 'aod_sulfate'),
                ('/Retrieval', 'aod_strataer'),
                ('/Retrieval', 'aod_water'),
                ('/Retrieval', 'aod_ice'),
                ('/Retrieval', 'aod_total'),
                ('/Retrieval', 'ice_height'),
                ('/Retrieval', 'dust_height'),
                ('/Retrieval', 'h2o_scale'),
                ('/Retrieval', 'deltaT'),
                ('/Retrieval', 'albedo_o2a'),
                ('/Retrieval', 'albedo_wco2'),
                ('/Retrieval', 'albedo_sco2'),
                ('/Retrieval', 'albedo_slope_o2a'),
                ('/Retrieval', 'albedo_slope_wco2'),
                ('/Retrieval', 'albedo_slope_sco2'),
                ('/Retrieval', 'chi2_o2a'),
                ('/Retrieval', 'chi2_wco2'),
                ('/Retrieval', 'chi2_sco2'),
                ('/Retrieval', 'rms_rel_o2a'),
                ('/Retrieval', 'rms_rel_wco2'),
                ('/Retrieval', 'rms_rel_sco2'),
                ('/Retrieval', 'iterations'),
                ('/Retrieval', 'diverging_steps'),
                ('/Retrieval', 'dof_co2'),
                ('/Retrieval', 'xco2_zlo_bias'),
                ('/Sounding', 'solar_azimuth_angle'),
                ('/Sounding', 'sensor_azimuth_angle'),
                ('/Sounding', 'pma_elevation_angle'),
                ('/Sounding', 'pma_azimuth_angle'),
                ('/Sounding', 'polarization_angle'),
                ('/Sounding', 'att_data_source'),
                ('/Sounding', 'land_fraction'),
                ('/Sounding', 'glint_angle'),
                ('/Sounding', 'airmass'),
                ('/Sounding', 'snr_o2a'),
                ('/Sounding', 'snr_wco2'),
                ('/Sounding', 'snr_sco2'),
                ('/Sounding', 'footprint'),
                ('/Sounding', 'land_water_indicator'),
                ('/Sounding', 'altitude'),
                ('/Sounding', 'altitude_stddev'),
                ('/Sounding', 'zlo_wco2'),
                ('/Sounding', 'target_id'),
                ('/Sounding', 'target_name'),
            ],
            'grid': {
                'latitude': 1800 * 4,
                'longitude': 3600 * 4
            },
            'mask-tolerance': 1,
            'max-workers': 4
        }

    return config_dict


if __name__ == '__main__':
    start = datetime.now()
    v = -1

    try:
        main(parse_args())
        v = 0
    except Exception as e:
        logger.critical('An unrecoverable exception has occurred')
        logger.exception(e)
        v = 1
    finally:
        logger.info(f'Exiting code {v}. Runtime={datetime.now() - start}')
        exit(v)
