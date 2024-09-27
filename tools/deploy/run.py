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

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import urllib.parse
import uuid
from copy import deepcopy
from datetime import datetime
from tempfile import NamedTemporaryFile

from schema import Schema, SchemaError, Or
from yaml import dump, load

try:
    from yaml import CDumper as Dumper
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Dumper, Loader


# Env vars for Lambda
#
# LAMBDA_STATE            : Path to state JSON file in mounted filesystem
# LAMBDA_RC_TEMPLATE      : Path to RC template file in mounted filesystem
# LAMBDA_STAC_PATH        : Path to CMR search result file in mounted filesystem
# LAMBDA_STAGE_PATH       : Path to data stage output file in mounted filesystem
# LAMBDA_GAP_FILE         : Path to gap file (see --gapfile arg)
# LAMBDA_TARGET_FILE      : Path to target file for OCO-3 SAM definitions
# LAMBDA_TARGET_FILE_OCO2 : Path to target file for OCO-2 target definitions
# LAMBDA_SKIP             : Comma-separated list of datasets to skip


SOURCE_DATASETS = [
    'oco3',  # OCO-3 XCO2
    'oco2',  # OCO-2 XCO2
    'oco3_sif',  # OCO-3 SIF
]


STATE_SCHEMA = Schema({
    'days': int,
    'granules': int,
    'processed': Or({str: {str: str}}, {})  # {Date -> {Dataset -> Filename}}
})

GAP_SCHEMA = Schema([{
    str: {
        'start': str,
        'stop': Or(str, None)
    }
}])

NO_NEW_DATA = 255
FAILED_PIPELINE = 1
FAILED_CRITICAL = 2
FAILED_UNITY = 3
FAILED_GENERAL = 4

PHASE_STAC_CHECK = 1
PHASE_POST_STAGE = 2
PHASE_FINISH = 3
PHASE_CLEANUP = 4


UNDETERMINED_END_DATE = datetime(2999, 12, 31)

# WON'T make any assumptions about EOMs until they are well known. That is, there is no chance of an extension
# Use dummy dates for now...
DATE_RANGES = dict(
    oco2=dict(start=datetime(2014, 9, 6), end=UNDETERMINED_END_DATE),
    oco3=dict(start=datetime(2019, 8, 6), end=UNDETERMINED_END_DATE),
    oco3_sif=dict(start=datetime(2019, 8, 6), end=UNDETERMINED_END_DATE),
)


GAPS = [
    {
        'oco3': dict(start="2023-11-13", stop="2024-07-16"),
        'oco3_sif': dict(start="2023-11-13", stop="2024-07-16")
    }
]


try:
    lp = os.getenv("LAMBDA_PHASE")
    if lp is not None:
        LAMBDA_PHASE = int(lp)
        IN_LAMBDA = True
    else:
        LAMBDA_PHASE = None
        IN_LAMBDA = False
except ValueError:
    print(f'Bad LAMBDA_PHASE env value: "{os.getenv("LAMBDA_PHASE")}"')
    exit(1)


def container_to_host_path(path: str, host_mount: str, container_mount: str):
    return path.replace(container_mount, host_mount, 1)


def load_state(path: str):
    if not os.path.exists(path):
        logger.warning(f'No state file present at {path}; initializing a new one')

        state = dict(days=0, granules=0, processed={})

        if IN_LAMBDA:
            with open(path, 'w') as fp:
                json.dump(state, fp)
    else:
        with open(path) as fp:
            state = json.load(fp)

    try:
        STATE_SCHEMA.validate(state)
    except SchemaError:
        logger.error('Invalid state file structure; reinitializing it')

        state = dict(days=0, granules=0, processed={})

    return state


if os.getenv('NO_DC_JSON') is None:
    def get_exit_code(cfg_file, multi=False):
        p = subprocess.Popen([
            dc, '-f', cfg_file, 'ps', '-a', '--format', 'json'
        ], stdout=subprocess.PIPE)

        p.wait()

        output = p.communicate()[0].decode('utf-8')
        containers = [json.loads(o) for o in output.split('\n') if len(o) > 0]
        service_name = list(load(open(cfg_file), Loader=Loader)['services'].keys())[0]

        container = [c for c in containers if c['Service'] == service_name][0]

        return container['ExitCode']
else:
    def get_exit_code(cfg_file, multi=False):
        p = subprocess.Popen([
            dc, '-f', cfg_file, 'ps', '-q'
        ], stdout=subprocess.PIPE)

        p.wait()

        output = p.communicate()[0].decode('utf-8')
        container_ids = [o for o in output.split('\n') if len(o) > 0]

        if multi:
            exit_codes = []

            for container_id in container_ids:
                p = subprocess.Popen([
                    docker, 'inspect', container_id
                ], stdout=subprocess.PIPE)

                p.wait()

                output = p.communicate()[0].decode('utf-8')

                inspect = json.loads(output)[0]

                exit_codes.append(inspect['State']['ExitCode'])

            if any([ec != 0 for ec in exit_codes]):
                exit_code = [ec for ec in exit_codes if ec != 0][0]
            else:
                exit_code = 0
        else:
            if len(container_ids) > 1:
                logger.warning('More than one container id in dc service... Picking first')

            container_id = container_ids[0]

            p = subprocess.Popen([
                docker, 'inspect', container_id
            ], stdout=subprocess.PIPE)

            p.wait()

            output = p.communicate()[0].decode('utf-8')

            inspect = json.loads(output)[0]

            exit_code = inspect['State']['ExitCode']

        return exit_code


def granule_to_dt(granule_name: str) -> str:
    dt = datetime.strptime(
        os.path.basename(granule_name).split('_')[2],  # Location of date info in oco2/3 lite file names
        "%y%m%d"
    )

    return dt.strftime("%Y-%m-%d")


def stac_filter(cmr_features, state, gap_file):
    class Gap:
        def __init__(self, start, stop):
            self.start = datetime.strptime(start, '%Y-%m-%d')
            self.stop = datetime.strptime(stop, '%Y-%m-%d') if stop is not None else None

        def is_in(self, obj):
            if isinstance(obj, str):
                obj = datetime.strptime(obj, '%Y-%m-%d')

            return self.start <= obj and (self.stop is None or obj <= self.stop)

        def __contains__(self, obj):
            return self.is_in(obj)

    known_gaps = []

    for gap in GAPS:
        collection = list(gap.keys())[0]
        known_gaps.append((Gap(gap[collection]['start'], gap[collection]['stop']), collection))

    if gap_file:
        try:
            with open(gap_file) as fp:
                gap_json = json.load(fp)

            try:
                GAP_SCHEMA.validate(gap_json)

                for gap in gap_json:
                    collection = list(gap.keys())[0]
                    known_gaps.append((Gap(gap[collection]['start'], gap[collection]['stop']), collection))
            except SchemaError:
                logger.error('Bad gap file schema, ignoring it')
            except FileNotFoundError:
                logger.error(f'Specified gap file {gap_file} does not exist')
        except:
            logger.error('Unexpected error parsing gap file')
            raise

    # Granule can be:
    # - 1: Present
    # - 2: Absent, but before start of available data
    # - 3: Absent, but after end of mission
    # - 4: Absent, but in a known gap
    # - 5: Absent for reason unknown
    #
    # A granule is included if neither granule is of type 5 or there is a later included granule

    date_map = {}

    for date in cmr_features:
        features = cmr_features[date]

        for collection in SOURCE_DATASETS:
            if collection in features:
                date_map.setdefault(date, {})[collection] = 'PRESENT'
            elif collection in skip:
                # Temporarily null out oco2 for non-global
                date_map.setdefault(date, {})[collection] = 'EXPECTED ABSENT'
            else:
                date_dt = datetime.strptime(date, '%Y-%m-%d')

                if date_dt < DATE_RANGES[collection]['start'] or DATE_RANGES[collection]['end'] < date_dt:
                    date_map.setdefault(date, {})[collection] = 'EXPECTED ABSENT'
                elif any([date_dt in gap for gap, c in known_gaps if c == collection]):
                    date_map.setdefault(date, {})[collection] = 'EXPECTED ABSENT'
                else:
                    date_map.setdefault(date, {})[collection] = 'ABSENT'

    date_map = {d: date_map[d] for d in sorted(date_map.keys())}
    mapped_dates = list(sorted(date_map.keys()))
    new_data = False
    last_valid_i = None

    for i in range(len(mapped_dates) - 1, -1, -1):
        d = mapped_dates[i]

        if all([date_map[d][c] != 'ABSENT' for c in SOURCE_DATASETS]):
            new_data = True
            last_valid_i = i
            break

    if not new_data:
        return []

    logger.info(f'Complete data is confirmed available from {mapped_dates[0]} to {mapped_dates[last_valid_i]}')

    reduced_cmr_features = {d: cmr_features[d] for d in mapped_dates[:last_valid_i + 1]}

    return_features = []

    for date in reduced_cmr_features:
        if date not in state['processed']:
            for collection in SOURCE_DATASETS:
                if collection in reduced_cmr_features[date]:
                    filename, feature = reduced_cmr_features[date][collection]
                    return_features.append((granule_to_dt(filename), feature))
        elif reduced_cmr_features[date].keys() != state['processed'][date].keys():
            logger.warning(f'For date {date}, data has been processed but new data has now been returned for this date'
                           f' this will trigger a repair down the line')

            added_collections = []

            for collection in reduced_cmr_features[date]:
                added_collections.append(collection)
                filename, feature = reduced_cmr_features[date][collection]
                return_features.append((granule_to_dt(filename), feature))

            for collection in state['processed'][date]:
                if collection in added_collections:
                    continue
                filename, feature = reduced_cmr_features[date][collection]
                return_features.append((granule_to_dt(filename), feature))

    return return_features


parser = argparse.ArgumentParser()

parser.add_argument(
    '--stac-search-dc',
    dest='dc_search',
    help='STAC Search Docker Compose config file path',
    default='dc-001-search-cmr-oco3.yaml'
)

parser.add_argument(
    '--stac-dl-dc',
    dest='dc_dl',
    help='STAC Download Docker Compose config file path',
    default='dc-002-download.yaml'
)

parser.add_argument(
    '--rc-template',
    dest='rc',
    help='Zarr generation pipeline YAML config template',
    default=os.getenv('LAMBDA_RC_TEMPLATE', 'run-config.yaml')
)

parser.add_argument(
    '--pipeline-image',
    dest='image',
    help='Zarr generation pipeline Docker image',
    required=os.getenv('LAMBDA_STATE') is None
)

parser.add_argument(
    '--state',
    dest='state',
    help='State file path. Contains granule count from last run + processed granules',
    default=os.getenv('LAMBDA_STATE', 'state.json')
)


def limit_int(s):
    i = int(s)
    if i <= 0:
        raise ValueError('Limit must be >0')
    return i


parser.add_argument(
    '--limit',
    dest='limit',
    help='Max number of granules to process. Useful if storage is limited and you wish to avoid trying to stage more '
         'than can be stored',
    type=limit_int,
    default=os.getenv('LAMBDA_GRANULE_LIMIT')
)

parser.add_argument(
    '--logging',
    dest='logging',
    help='Path to logging output directory'
)

parser.add_argument(
    '-v',
    dest='verbose',
    action='store_true',
    help='Verbose logging'
)

parser.add_argument(
    '--gapfile',
    dest='gaps',
    help='Path to JSON file containing known, extended data gaps in missions. '
         'Fmt: [{mission: {start: dt, end: dt/null}}]'
)

parser.add_argument(
    '--target-file-oco3',
    dest='target_file_3',
    help='Path to JSON file containing OCO3 SAM target data. '
         'Fmt: {target_id: {name: str, bbox: {min_lon: f, min_lat: f, max_lon: f, max_lat: f}}',
    default=os.getenv('LAMBDA_TARGET_FILE')
)

parser.add_argument(
    '--target-file-oco2',
    dest='target_file_2',
    help='Path to JSON file containing OCO2 SAM target data. '
         'Fmt: {target_id: {name: str, bbox: {min_lon: f, min_lat: f, max_lon: f, max_lat: f}}',
    default=os.getenv('LAMBDA_TARGET_FILE_OCO2')
)

parser.add_argument(
    '--skip',
    dest='skips',
    help='Comma-separated list of datasets to skip',
    type=str,
    default=''
)

args = parser.parse_args()

start_time = datetime.now()

run_id = str(uuid.uuid4())

log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s')
log_level = logging.DEBUG if args.verbose else logging.INFO
log_file_root = f'{datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")}'
log_handlers = []

stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(log_level)
log_handlers.append(stream_handler)

logger = logging.getLogger('oco3_driver')
logger.setLevel(log_level)
logger.addHandler(stream_handler)

if args.logging:
    file_handler = logging.FileHandler(os.path.join(args.logging, f'{log_file_root}-driver.log'), mode='w')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(log_level)
    log_handlers.append(file_handler)
    logger.addHandler(file_handler)

SUPPRESS = [
    'botocore',
    's3transfer',
    'urllib3',
]

for logger_name in SUPPRESS:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

root_logger = logging.getLogger()
for each in root_logger.handlers:
    root_logger.removeHandler(each)

dc = shutil.which('docker-compose')
docker = shutil.which('docker')

if dc is None and not IN_LAMBDA:
    logger.error('docker-compose command could not be found in PATH')
    raise OSError('docker-compose command could not be found in PATH')

with open(args.rc) as fp:
    global_product = load(fp, Loader=Loader)['output'].get('global', True)
    skip = list(set(os.getenv('LAMBDA_SKIP', '').lower().split(',') + args.skips.lower().split(',')))


def main(phase_override=None):
    the_time = datetime.now()
    state = load_state(args.state)
    mount_dir = os.getenv('LAMBDA_MOUNT_DIR')
    global LAMBDA_PHASE

    if phase_override is not None:
        LAMBDA_PHASE = phase_override

    if LAMBDA_PHASE is None:
        logger.info('Beginning CMR search')

        with open(f'{log_file_root}-stac-search.log', 'w') as fp:
            search_p = subprocess.Popen(
                [dc, '-f', args.dc_search, 'up'],
                stdout=fp,
                stderr=subprocess.STDOUT
            )

        search_p.wait()

        logger.info(f'CMR search completed in {datetime.now() - the_time}')

        try:
            search_exit_code = get_exit_code(args.dc_search, multi=True)
        except Exception as e:
            logger.critical('Could not determine CMR search status! Exiting')
            raise
        finally:
            subprocess.Popen(
                [dc, '-f', args.dc_search, 'down'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT
            ).wait()

        if search_exit_code != 0:
            logger.error('CMR search returned nonzero exit code, stopping...')
            return FAILED_UNITY

    if LAMBDA_PHASE is None or LAMBDA_PHASE == PHASE_STAC_CHECK:
        if LAMBDA_PHASE is None:
            with open(args.dc_search) as fp:
                search_config = load(fp, Loader=Loader)

            stac_files = dict(
                oco2=container_to_host_path(
                    search_config['services']['cumulus_granules_search_oco2']['environment']['OUTPUT_FILE'],
                    *search_config['services']['cumulus_granules_search_oco2']['volumes'][0].split(':')[:2]
                ),
                oco3=container_to_host_path(
                    search_config['services']['cumulus_granules_search_oco3']['environment']['OUTPUT_FILE'],
                    *search_config['services']['cumulus_granules_search_oco3']['volumes'][0].split(':')[:2]
                ),
                oco3_sif=container_to_host_path(
                    search_config['services']['cumulus_granules_search_oco3_sif']['environment']['OUTPUT_FILE'],
                    *search_config['services']['cumulus_granules_search_oco3_sif']['volumes'][0].split(':')[:2]
                ),
            )
        else:
            stac_file_root = os.getenv('LAMBDA_STAC_PATH')
            stac_files = dict(
                oco2=os.path.join(stac_file_root, 'cmr-results-oco2.json'),
                oco3=os.path.join(stac_file_root, 'cmr-results.json'),
                oco3_sif=os.path.join(stac_file_root, 'cmr-results-oco3_sif.json'),
            )

        logger.info('Comparing CMR results to state')

        cmr_feature_count = 0
        cmr_features = {}

        for collection in stac_files:
            with open(stac_files[collection]) as fp:
                cmr_results = json.load(fp)

            if collection in skip:
                # Temporarily null out oco2 for non-global
                cmr_results['features'] = []

            for f in cmr_results['features']:
                cmr_feature_count += 1
                filename = f['assets']['data']['href'].split('/')[-1]

                cmr_features.setdefault(granule_to_dt(filename), {})[collection] = (filename, f)

        if LAMBDA_PHASE is None:
            gap_file = args.gaps
        else:
            gap_file = os.getenv('LAMBDA_GAP_FILE')

        dl_features = stac_filter(cmr_features, state, gap_file)

        if len(dl_features) == 0:
            logger.info('No new data to process')
            return NO_NEW_DATA

        granule_date_mapping = {}

        for date, feature in dl_features:
            granule_date_mapping.setdefault(date, []).append(feature)

        logger.info(f"CMR returned {cmr_feature_count:,} features with {len(dl_features):,} new granules "
                    f"over {len(granule_date_mapping):,} days to process")

        if args.limit and len(granule_date_mapping) > args.limit:
            logger.info(f'CMR returned more days of data ({len(granule_date_mapping):,}) than the provided limit '
                        f'({args.limit:,}). Truncating list of granules to process.')

            granule_date_mapping = dict(list(granule_date_mapping.items())[:args.limit])

            logger.info(f'Reduced selection to {len(granule_date_mapping)} days with '
                        f'{sum([len(granule_date_mapping[k]) for k in granule_date_mapping])} granules')

        dl_features = []

        for date in granule_date_mapping:
            dl_features.extend(granule_date_mapping[date])

        cmr_results['features'] = dl_features

        with open(stac_files['oco3'], 'w') as fp:
            json.dump(cmr_results, fp, indent=4)

        if IN_LAMBDA:
            return 0

    the_time = datetime.now()

    if LAMBDA_PHASE is None:
        logger.info('Staging granules...')

        with open(f'{log_file_root}-stac-download.log', 'w') as fp:
            stage_p = subprocess.Popen(
                [dc, '-f', args.dc_dl, 'up'],
                stdout=fp,
                stderr=subprocess.STDOUT
            )

        stage_p.wait()

        logger.info(f'Granule staging completed in {datetime.now() - the_time}')

        try:
            download_exit_code = get_exit_code(args.dc_dl)
        except Exception as e:
            logger.critical('Could not determine staging process status! Exiting')
            raise
        finally:
            subprocess.Popen(
                [dc, '-f', args.dc_dl, 'down'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT
            ).wait()

        if download_exit_code != 0:
            logger.error('Granule staging returned nonzero exit code, stopping...')
            return FAILED_UNITY

    if LAMBDA_PHASE is None or LAMBDA_PHASE == PHASE_POST_STAGE:
        if not IN_LAMBDA:
            with open(args.dc_dl) as fp:
                dl_config = load(fp, Loader=Loader)

            granule_dir = container_to_host_path(
                dl_config['services']['cumulus_granules_download']['environment']['DOWNLOAD_DIR'],
                *dl_config['services']['cumulus_granules_download']['volumes'][0].split(':')[:2]
            )

            with open(args.dc_search) as fp:
                search_config = load(fp, Loader=Loader)

            stac_file = container_to_host_path(
                search_config['services']['cumulus_granules_search_oco3']['environment']['OUTPUT_FILE'],
                *search_config['services']['cumulus_granules_search_oco3']['volumes'][0].split(':')[:2]
            )
        else:
            granule_dir = os.getenv('LAMBDA_STAGE_DIR')
            stac_file = os.path.join(os.getenv('LAMBDA_STAC_PATH'), 'cmr-results.json')

        with open(os.path.join(granule_dir, 'downloaded_feature_collection.json')) as fp:
            downloaded_granules = [feature['assets']['data']['href'].lstrip('./') for feature in json.load(fp)['features']]
            downloaded_granules.sort()

        with open(stac_file) as fp:
            n_needed = len(json.load(fp)['features'])

        if len(downloaded_granules) != n_needed:
            logger.error(f'Mismatch between downloaded granules {len(downloaded_granules)} and number selected '
                         f'{n_needed}; download error likely; quitting')
            return FAILED_UNITY
        else:
            logger.info(f'Downloaded correct number of granules: {n_needed}')

        granule_date_mapping = {}

        for granule in downloaded_granules:
            if granule.startswith('oco3_LtCO2'):
                collection = 'oco3'
            elif granule.startswith('oco3_LtSIF'):
                collection = 'oco3_sif'
            else:
                collection = 'oco2'

            granule_date_mapping.setdefault(granule_to_dt(granule), {})[collection] = (
                os.path.join('/var/inputs/', granule))

        with open(args.rc) as fp:
            pipeline_config = load(fp, Loader=Loader)

        pipeline_config['input']['files'] = [granule_date_mapping[date] for date in granule_date_mapping]

        if LAMBDA_PHASE is not None:
            with open(os.path.join(mount_dir, 'pipeline-rc.yaml'), 'w') as fp:
                dump(pipeline_config, fp, Dumper=Dumper, sort_keys=False)

            with open(os.path.join(mount_dir, 'staging.json'), 'w') as fp:
                json.dump(dict(granule_dir=granule_dir, granules=downloaded_granules), fp)

            return 0
        else:
            output_cfg = deepcopy(pipeline_config['output'])

            if 'local' in output_cfg:
                output_mount_dir = urllib.parse.urlparse(output_cfg['local']).path
                pipeline_config['output']['local'] = '/var/outputs/'
            else:
                output_mount_dir = None

            logger.info('Starting product generation...')

            with NamedTemporaryFile(
                mode='w', prefix='rc-', suffix='.yaml'
            ) as rc_fp:
                dump(pipeline_config, rc_fp, Dumper=Dumper, sort_keys=False)
                rc_fp.flush()

                the_time = datetime.now()

                with open(f'{log_file_root}-pipeline.log', 'w') as log_fp:
                    p_args = [
                        docker,
                        'run',
                        '--name',
                        'oco-sam-l3',
                        '-v',
                        f'{rc_fp.name}:/etc/config.yaml',
                        '-v',
                        f'{granule_dir}:/var/inputs/',
                    ]

                    if output_mount_dir is not None:
                        p_args.extend([
                            '-v',
                            f'{output_mount_dir}:/var/outputs/',
                        ])

                    if not global_product:
                        p_args.extend([
                            '-v',
                            f'{args.target_file_3}:/etc/targets.json',
                            '-v',
                            f'{args.target_file_2}:/etc/targets_oco2.json'
                        ])

                    p_args.extend([
                        args.image,
                        'python',
                        '/sam_extract/main.py',
                        '-i',
                        '/etc/config.yaml',
                    ])

                    pipeline_p = subprocess.Popen(
                        p_args,
                        stdout=log_fp,
                        stderr=subprocess.STDOUT
                    ).wait()

                logger.info(f'Product generation completed in {datetime.now() - the_time}')

            inspect_p = subprocess.Popen(
                [
                    docker,
                    'container',
                    'inspect',
                    'oco-sam-l3',
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )

            inspect_p.wait()

            try:
                data, err = inspect_p.communicate()
                if inspect_p.returncode == 0:
                    stats = json.loads(data.decode('utf-8'))[0]

                    exit_code = int(stats['State']['ExitCode'])

                    if exit_code != 0:
                        logger.error('Product generation failed: {exit_code}')
                        logger.debug(json.dumps(stats, indent=4))
                        raise OSError('Product generation failed')
                else:
                    logger.error(f'Inspect subprocess error {err}. Assuming product generation failed')
                    raise OSError('Product generation failed')
            finally:
                subprocess.Popen(
                    [
                        docker,
                        'rm',
                        'oco-sam-l3',
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT
                ).wait()

    if LAMBDA_PHASE is None or LAMBDA_PHASE in [PHASE_FINISH, PHASE_CLEANUP]:
        if LAMBDA_PHASE is not None:
            with open(os.path.join(mount_dir, 'staging.json')) as fp:
                s = json.load(fp)

                downloaded_granules = s['granules']
                granule_dir = s['granule_dir']

        for g in downloaded_granules:
            if g.startswith('oco3_LtCO2'):
                collection = 'oco3'
            elif g.startswith('oco3_LtSIF'):
                collection = 'oco3_sif'
            else:
                collection = 'oco2'

            date = granule_to_dt(g)

            state['processed'].setdefault(date, {})[collection] = g

        state['days'] = len(state['processed'])
        state['granules'] = sum([len(state['processed'][d]) for d in state['processed']])

        logger.info('Cleaning up & updating state')

        for g in [os.path.join(granule_dir, g) for g in downloaded_granules]:
            os.remove(g)

        os.remove(os.path.join(granule_dir, 'downloaded_feature_collection.json'))

        if LAMBDA_PHASE is None or LAMBDA_PHASE == PHASE_FINISH:
            with open(args.state, 'w') as fp:
                json.dump(state, fp, indent=4)
        else:
            logger.info('Not updating state')

        if LAMBDA_PHASE is not None:
            os.remove(os.path.join(mount_dir, 'staging.json'))
            os.remove(os.path.join(mount_dir, 'pipeline-rc.yaml'))

    logger.info(f'Script completed in {datetime.now() - start_time}')

    return 0


def lambda_handler(event, context):
    ret = FAILED_GENERAL

    try:
        ret = main(event.get('phase', PHASE_STAC_CHECK))
    except:
        ret = FAILED_GENERAL
    finally:
        return {
            'ExitCode': ret
        }


if __name__ == '__main__':
    ret = None

    try:
        ret = main()
    except Exception as e:
        ret = FAILED_GENERAL
        logger.error('Caught exception:')
        logger.exception(e)
    finally:
        exit(ret)
