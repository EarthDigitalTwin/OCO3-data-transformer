import argparse
import logging
import os.path
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from tempfile import TemporaryDirectory
from time import sleep
from typing import List, Optional

import numpy as np
import pika
import xarray as xr
import yaml
from pika.channel import Channel
from pika.exceptions import AMQPChannelError, AMQPConnectionError, ConnectionClosedByBroker
from pika.spec import Basic, BasicProperties
from schema import Optional as Opt
from schema import Schema, Or, SchemaError
from scipy.interpolate import griddata
from shapely.affinity import scale
from shapely.geometry import Polygon, box, MultiPolygon
from shapely.ops import unary_union
from yaml import load
from yaml.scanner import ScannerError

from sam_extract.exceptions import *
from sam_extract.readers import GranuleReader
from sam_extract.writers import ZarrWriter

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] [%(name)s::%(lineno)d] %(message)s'
)

logger = logging.getLogger('sam_extract.main')

# Semi-suppress some loggers that are super verbose when DEBUG lvl is config'd
SUPPRESS = [
    'botocore',
    's3transfer',
    's3fs',
    'urllib3',
    'asyncio',
]

HARD_SUPPRESS = [
    'pika',
]

for logger_name in SUPPRESS:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

for logger_name in HARD_SUPPRESS:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)

DEFAULT_INTERPOLATE_METHOD = 'cubic'

RMQ_SCHEMA = Schema({
    'inputs': [
        Or(
            str,
            {
                'path': str,
                Opt('accessKeyID'): str,
                Opt('secretAccessKey'): str,
                Opt('urs'): str
            }
        )
    ]
})


FILES_SCHEMA = Schema([
    Or(
        str,
        {
            'path': str,
            Opt('accessKeyID'): str,
            Opt('secretAccessKey'): str,
            Opt('urs'): str
        }
    )
])


AWS_CRED_SCHEMA = Schema({
    'path': str,
    'accessKeyID': str,
    'secretAccessKey': str,
})


def __validate_files(files):
    FILES_SCHEMA.validate(files)

    for file in files:
        if isinstance(file, dict):
            if 'urs' not in file:
                AWS_CRED_SCHEMA.validate(file)
            else:
                if 'accessKeyID' in file or 'secretAccessKey' in file:
                    logger.warning(f'Provided AWS creds for {file["path"]} will be overridden by dynamic STS '
                                   f'creds')


def fit_data_to_grid(sams, cfg):
    logger.info('Concatenating SAM datasets for interpolation')

    if len(sams) == 0:
        return None

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
              'xco2_quality_flag', 'co2_profile_apriori', 'xco2_apriori'],
        '/Retrieval': ['diverging_steps', 'iterations', 'surface_type', 'SigmaB'],
        '/Sounding': ['att_data_source', 'footprint', 'land_fraction', 'land_water_indicator', 'operation_mode',
                      'orbit', 'pma_azimuth_angle', 'pma_elevation_angle', 'sensor_azimuth_angle',
                      'solar_azimuth_angle', 'target_id', 'target_name']
    }

    logger.info('Dropping variables that will be excluded from interpolation (ie, non-numeric values)')

    for group in drop_dims:
        if group in interp_ds:
            interp_ds[group] = interp_ds[group].drop_vars(drop_dims[group], errors='ignore')

    logger.debug('Building coordinate meshes')

    lon_grid, lat_grid = np.mgrid[-180:180:complex(0, cfg['grid']['longitude']),
                                  -90:90:complex(0, cfg['grid']['latitude'])].astype(np.dtype('float32'))

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
        'longitude': ('longitude', lon_grid.transpose()[0], lon_attrs),
        'latitude': ('latitude', lat_grid[0], lat_attrs),
        'time': ('time', time, time_attrs)
    }

    gridded_ds = {}

    logger.info(f"Interpolating retained data variables to {cfg['grid']['longitude']:,} by {cfg['grid']['latitude']:,}"
                f" grid")

    def interpolate(in_grp, grp, var):
        logger.info(f'Interpolating variable {var} in group {grp}')
        return [griddata(points,
                in_grp[grp][var].to_numpy(),
                (lon_grid, lat_grid),
                method=cfg['grid'].get('method', DEFAULT_INTERPOLATE_METHOD),
                fill_value=in_grp[grp][var].attrs['missing_value'])]

    for group in interp_ds:
        gridded_ds[group] = xr.Dataset(
            data_vars={
                var_name: (('time', 'longitude', 'latitude'),
                           interpolate(interp_ds, group, var_name))
                for var_name in interp_ds[group].data_vars
            },
            coords=coords,
            # attrs=interp_ds[group].attrs     # Add attrs at later point when ds is fully constructed
        )

        for var in gridded_ds[group]:
            gridded_ds[group][var].attrs = interp_ds[group][var].attrs

    logger.info('Completed interpolations to grid')

    gridded_ds['/'].attrs['interpolation_method'] = cfg['grid'].get('method', DEFAULT_INTERPOLATE_METHOD)

    res_attr = cfg['grid'].get('resolution_attr')

    if res_attr:
        gridded_ds['/'].attrs['resolution'] = res_attr

    return gridded_ds


def mask_data(sams, grid_ds, cfg):
    if sams is None:
        return None

    if grid_ds is None:
        return None

    logger.info('Constructing SAM polygons to build mask')

    latitudes = grid_ds['/'].latitude.to_numpy()
    longitudes = grid_ds['/'].longitude.to_numpy()

    sam_polys = []

    scaling = cfg.get('mask-scaling', 1.1)
    scaling = min(max(scaling, 1), 1.5)

    logger.info(f'Footprint scaling factor: {scaling}')

    for i, sam in enumerate(sams):
        logger.info(f'Creating bounding poly for SAM of {len(sam["/"].vertex_latitude):,} footprints '
                    f'[{i+1}/{len(sams)}]')

        footprint_polygons = []

        for lats, lons in zip(sam['/'].vertex_latitude, sam['/'].vertex_longitude):
            v = [(lons[i].item(), lats[i].item()) for i in range(len(lats))]
            v.append((lons[0].item(), lats[0].item()))
            footprint_polygons.append(scale(Polygon(v), scaling, scaling))

        bounding_poly = unary_union(footprint_polygons)

        logger.debug(f'Created poly with bbox {bounding_poly.bounds}')

        sam_polys.append(bounding_poly)

    logger.info('Producing geo mask from SAM polys')

    geo_mask = np.full((len(longitudes), len(latitudes)), False)

    lon_len = longitudes[1] - longitudes[0]
    lat_len = latitudes[1] - latitudes[0]

    for i, poly in enumerate(sam_polys):
        indices = []

        if isinstance(poly, MultiPolygon):
            logger.debug(f'Determining coordinates from {len(poly.geoms)} sub-polygons in bounding MutliPolygon')

            for geom in poly.geoms:
                minx, miny, maxx, maxy = geom.bounds

                indices.append((
                    np.argwhere(np.logical_and(miny <= latitudes, latitudes <= maxy)),
                    np.argwhere(np.logical_and(minx <= longitudes, longitudes <= maxx))
                ))
        else:
            logger.debug(f'Determining coordinates from bounding Geometry ({type(poly)})')

            minx, miny, maxx, maxy = poly.bounds

            indices.append((
                np.argwhere(np.logical_and(miny <= latitudes, latitudes <= maxy)),
                np.argwhere(np.logical_and(minx <= longitudes, longitudes <= maxx))
            ))

        n_lats = sum([len(ind[0]) for ind in indices])
        n_lons = sum([len(ind[1]) for ind in indices])
        n_pts = sum([len(ind[0]) * len(ind[1]) for ind in indices])

        logger.debug(f'Checking for poly ({poly.bounds})')
        logger.info(f'Applying bounding poly to geo mask across {n_lats:,} latitudes, {n_lons:,} '
                    f'longitudes. {n_pts:,} total points. [{i+1}/{len(sam_polys)}]')

        for lat_indices, lon_indices in indices:
            for lon_i in lon_indices:
                for lat_i in lat_indices:
                    lon_i = tuple(lon_i)
                    lat_i = tuple(lat_i)

                    if geo_mask[lon_i][lat_i]:
                        continue

                    lon = longitudes[lon_i]
                    lat = latitudes[lat_i]
                    grid_poly = box(lon - lon_len, lat - lat_len, lon + lon_len, lat + lat_len)

                    geo_mask[lon_i][lat_i] = grid_poly.intersects(poly)

        logger.debug(f'Finished applying poly ({poly.bounds}) to geo mask')

    mask = np.array([geo_mask])

    logger.info('Applying mask to dataset')

    for group in grid_ds:
        for var in grid_ds[group].data_vars:
            grid_ds[group][var] = grid_ds[group][var].where(mask)

    return grid_ds


def process_input(input_file,
                  cfg,
                  temp_dir,
                  output_pre_qf=True,
                  exclude_groups: Optional[List[str]] = None):
    additional_params = {'drop_dims': cfg['drop-dims']}

    if exclude_groups is None:
        exclude_groups = []

    if '/' in exclude_groups:
        raise ValueError('Cannot exclude root group')

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

    logger.info(f'Processing input at {path}')
    logger.info('Opening granule')

    try:
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

            if len(extracted_sams_post_qf) == 0:
                if not output_pre_qf or len(extracted_sams_pre_qf) == 0:
                    logger.info('No SAM data to work with, skipping input')
                    return None, None, True, path


            if output_pre_qf:
                logger.info('Fitting unfiltered SAM data to output grid')

                gridded_groups_pre_qf = mask_data(
                    extracted_sams_pre_qf,
                    fit_data_to_grid(
                        extracted_sams_pre_qf,
                        cfg
                    ),
                    cfg
                )

                if gridded_groups_pre_qf is not None:
                    temp_path_pre = os.path.join(temp_dir, 'pre_qf', os.path.basename(input_file)) + '.zarr'

                    logger.info('Outputting unfiltered SAM product slice to temporary Zarr array')

                    writer = ZarrWriter(temp_path_pre, (5, 250, 250), overwrite=True, verify=False)
                    writer.write(gridded_groups_pre_qf)

                    del gridded_groups_pre_qf

                    ret_pre_qf = ZarrWriter.open_zarr_group(temp_path_pre, 'local', None)
                else:
                    ret_pre_qf = None

            logger.info('Fitting filtered SAM data to output grid')

            gridded_groups_post_qf = mask_data(
                extracted_sams_post_qf,
                fit_data_to_grid(
                    extracted_sams_post_qf,
                    cfg
                ),
                cfg
            )

            if gridded_groups_post_qf is not None:
                temp_path_post = os.path.join(temp_dir, 'post_qf', os.path.basename(input_file)) + '.zarr'

                logger.info('Outputting filtered SAM product slice to temporary Zarr array')

                writer = ZarrWriter(temp_path_post, (5, 250, 250), overwrite=True, verify=False)
                writer.write(gridded_groups_post_qf)

                ret_post_qf = ZarrWriter.open_zarr_group(temp_path_post, 'local', None)
            else:
                ret_post_qf = None

            logger.info(f'Finished processing input at {path}')

            return ret_pre_qf, ret_post_qf, True, path
    except ReaderException:
        return None, None, False, path


def merge_groups(groups):
    logger.info(f'Merging {len(groups)} interpolated groups')

    groups = [group for group in groups if group is not None]

    if len(groups) == 0:
        return None

    return {group: xr.concat([g[group] for g in groups], dim='time').sortby('time') for group in groups[0]}


def process_inputs(in_files, cfg):
    logger.info(f'Interpolating {len(in_files)} L2 Lite file(s) with interpolation method '
                f'{cfg["grid"].get("method", DEFAULT_INTERPOLATE_METHOD)}')

    def output_cfg(cfg):
        cfg = cfg['output']

        additional_params = {'verify': True}

        if cfg['type'] == 'local':
            path_root = cfg['local']
        else:
            path_root = cfg['s3']['url']
            additional_params['region'] = cfg['s3']['region']
            additional_params['auth'] = cfg['s3']['auth']

        return path_root, additional_params

    with TemporaryDirectory(prefix='oco-sam-extract-', suffix='-zarr-scratch', ignore_cleanup_errors=True) as td:
        exclude = [
            '/Preprocessors',
            '/Meteorology',
            '/Sounding'
        ]

        process = partial(process_input, cfg=cfg, temp_dir=td, exclude_groups=exclude)

        with ThreadPoolExecutor(max_workers=cfg.get('max-workers'), thread_name_prefix='process_worker') as pool:
            processed_groups_pre = []
            processed_groups_post = []
            failed_inputs = []

            for result_pre, result_post, success, path in pool.map(process, in_files):
                if success:
                    if result_pre is not None:
                        processed_groups_pre.append(result_pre)
                    else:
                        logger.info(f'No pre-QF SAM data generated for {path}; likely none was present')

                    if result_post is not None:
                        processed_groups_post.append(result_post)
                    else:
                        logger.info(f'No post-QF SAM data generated for {path}; likely no SAMs were present or they '
                                    f'were all filtered out')
                else:
                    failed_inputs.append(path)

        if len(failed_inputs) == len(in_files):
            logger.critical('No input files could be read!')
            raise NoValidFilesException()
        elif len(failed_inputs) > 0:
            logger.error(f'Some input files failed because they could not be read:')
            for failed in failed_inputs:
                logger.error(f' - {failed}')

        merged_pre = merge_groups(processed_groups_pre)

        output_root, output_kwargs = output_cfg(cfg)

        if merged_pre is not None:
            logger.info('Merged processed pre_qf data')

            zarr_writer = ZarrWriter(
                # f'file:///Users/rileykk/oco3/oco-sam-extract/la_sams_sample_pre_qf_1km_{method}.zarr',
                os.path.join(output_root, cfg['output']['naming']['pre_qf']),
                (5, 250, 250),
                overwrite=False,
                **output_kwargs
            )
            zarr_writer.write(merged_pre)
        else:
            logger.info('No pre_qf data generated')

        merged_post = merge_groups(processed_groups_post)

        if merged_post is not None:
            logger.info('Merged processed post_qf data')

            zarr_writer = ZarrWriter(
                # f'file:///Users/rileykk/oco3/oco-sam-extract/la_sams_sample_post_qf_1km_{method}.zarr',
                os.path.join(output_root, cfg['output']['naming']['post_qf']),
                (5, 250, 250),
                overwrite=False,
                **output_kwargs
            )
            zarr_writer.write(merged_post)
        else:
            logger.info('No post_qf data generated, all SAMs on these days may have been filtered out for bad qf')


def main(cfg):
    if cfg['input']['type'] == 'queue':
        def on_message(
                channel: Channel,
                method_frame: Basic.Deliver,
                header_frame: BasicProperties,
                body: bytes):
            logger.info('Recieved message from input queue')
            logger.debug(method_frame.delivery_tag)
            logger.debug(method_frame)
            logger.debug(header_frame)
            logger.debug(body)

            try:
                msg_dict = load(body.decode('utf-8'), Loader=Loader)
                RMQ_SCHEMA.validate(msg_dict)

                __validate_files(msg_dict['inputs'])

                logger.info('Successfully decoded and validated message, starting pipeline')
                pipeline_start_time = datetime.now()

                # process_inputs(msg_dict['inputs'], cfg)

                thread = threading.Thread(
                    target=process_inputs,
                    args=(msg_dict['inputs'], cfg),
                    name='process-message-thread'
                )
                thread.start()

                i = 120

                while thread.is_alive():
                    channel.connection.sleep(1.0)

                    i -= 1
                    if i == 0:
                        i = 120
                        channel.connection.process_data_events()

                logger.info(f'Pipeline completed in {datetime.now() - pipeline_start_time}')
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except (yaml.YAMLError, ScannerError):
                logger.error('Invalid message received: bad yaml. Dropping it from queue')
                channel.basic_reject(delivery_tag=method_frame.delivery_tag, requeue=False)
            except SchemaError:
                logger.error(f'Invalid message received: improper schema. Dropping it from queue')
                channel.basic_reject(delivery_tag=method_frame.delivery_tag, requeue=False)
            except NonRetryableException:
                logger.error('The message could not be properly processed and will be dropped from the queue')
                channel.basic_reject(delivery_tag=method_frame.delivery_tag, requeue=False)
            except NonRecoverableError:
                # logger.critical('An unrecoverable exception occurred, exiting.')
                channel.basic_nack(delivery_tag=method_frame.delivery_tag)  # nacking because it may not be related to
                                                                            # the message. This scenario should need to
                                                                            # be manually investigated
                raise
            except KeyboardInterrupt:
                channel.basic_nack(delivery_tag=method_frame.delivery_tag)
                raise
            except Exception:
                channel.basic_nack(delivery_tag=method_frame.delivery_tag)
                raise
            # TODO: Improve exceptions. Custom wrapper classes: Bad msg (drop) [eg, invalid or nonexistent path, bad
            #  input fmt], Transient (requeue) [transient errors], Unrecoverable (requeue) [kill the whole thing]

        queue_config = cfg['input']['queue']

        creds = pika.PlainCredentials(queue_config['username'], queue_config['password'])

        rmq_host = queue_config['host']
        rmq_port = queue_config.get('port', 5672)

        params = pika.ConnectionParameters(
            host=rmq_host,
            port=rmq_port,
            credentials=creds,
            heartbeat=600
        )

        retries = 5

        while True:
            try:
                logger.info(f'Connecting to RMQ at {rmq_host}:{rmq_port}...')

                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.basic_qos(prefetch_count=1)

                channel.queue_declare(queue_config['queue'], durable=True,)

                retries = 5

                logger.info('Connected to RMQ, listening for messages')
                channel.basic_consume(queue_config['queue'], on_message)
                try:
                    channel.start_consuming()
                except KeyboardInterrupt:
                    logger.info('Received interrupt. Stopping listening to queue and closing connection')
                    channel.stop_consuming()
                    connection.close()
                    break
                except NonRecoverableError:
                    logger.critical('An unrecoverable exception occurred, exiting.')
                    channel.stop_consuming()
                    connection.close()
                    raise
            except ConnectionClosedByBroker:
                logger.warning('Connection closed by server, retrying')
                sleep(2)
                retries -= 1
                continue
            except AMQPChannelError as err:
                logger.error("Caught a channel error: {}, stopping...".format(err))
                logger.exception(err)
                break
                # Recover on all other connection errors
            except AMQPConnectionError:
                if retries > 0:
                    logger.error("Connection was closed, retrying...")
                    sleep(2)
                    retries -= 1
                    continue
                else:
                    logger.critical("Could not reconnect to RMQ")
                    raise
            except Exception as err:
                logger.error('An unexpected error occurred')
                logger.exception(err)
                raise

    elif cfg['input']['type'] == 'files':
        in_files = cfg['input']['files']
        process_inputs(in_files, cfg)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', help='Configuration yaml file', metavar='YAML', dest='cfg', required=True)

    parser.add_argument(
        '--ed-user',
        help='Earthdata username',
        dest='edu',
        default=None
    )

    parser.add_argument(
        '--ed-pass',
        help='Earthdata password',
        dest='edp',
        default=None
    )

    args = parser.parse_args()

    with open(args.cfg) as f:
        config_dict = load(f, Loader=Loader)

    try:
        output = config_dict['output']
        inp = config_dict['input']

        if 's3' in output and 'local' in output:
            raise ValueError('Must specify either s3 or local, not both')

        if 'local' in output:
            config_dict['output']['type'] = 'local'
        elif 's3' in output:
            if 'region' not in output['s3']:
                output['s3']['region'] = 'us-west-2'

            config_dict['output']['type'] = 's3'
        else:
            raise ValueError('No output params configured')

        if 'naming' not in output:
            raise ValueError('Must specify naming for output')

        if 'queue' in inp and 'files' in inp:
            raise ValueError('Must specify either files or queue, not both')

        if 'queue' in inp:
            config_dict['input']['type'] = 'queue'
        elif 'files' in inp:
            config_dict['input']['type'] = 'files'

            __validate_files(inp['files'])
        else:
            raise ValueError('No input params configured')

        if 'drop-dims' in config_dict:
            config_dict['drop-dims'] = [
                (dim['group'], dim['name']) for dim in config_dict['drop-dims']
            ]
        else:
            config_dict['drop-dims'] = []
    except KeyError as e:
        logger.exception(e)
        raise ValueError('Invalid configuration')

    GranuleReader.configure_netrc(username=args.edu, password=args.edp)

    return config_dict


if __name__ == '__main__':
    start_time = datetime.now()
    v = -1

    try:
        main(parse_args())
        v = 0
    except Exception as e:
        logger.critical('An unrecoverable exception has occurred')
        logger.exception(e)
        v = 1
    finally:
        logger.info(f'Exiting code {v}. Runtime={datetime.now() - start_time}')
        exit(v)
