import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime
from tempfile import NamedTemporaryFile

from schema import Schema, SchemaError
from yaml import dump, load

try:
    from yaml import CDumper as Dumper
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Dumper, Loader


STATE_SCHEMA = Schema({
    'count': int,
    'processed': [str]
})


def container_to_host_path(path: str, host_mount: str, container_mount: str):
    return path.replace(container_mount, host_mount, 1)


def load_state(path: str):
    if not os.path.exists(path):
        logger.warning(f'No state file present at {path}; initializing a new one')

        state = dict(count=0, processed=[])
    else:
        with open(path) as fp:
            state = json.load(fp)

    try:
        STATE_SCHEMA.validate(state)
    except SchemaError:
        logger.error('Invalid state file structure; reinitializing it')

        state = dict(count=0, processed=[])

    return state


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
    default='run-config.yaml'
)

parser.add_argument(
    '--pipeline-image',
    dest='image',
    help='Zarr generation pipeline Docker image',
    required=True
)

parser.add_argument(
    '--state',
    dest='state',
    help='State file path. Contains granule count from last run + processed granules',
    default='state.json'
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
    type=limit_int
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

args = parser.parse_args()

start_time = datetime.now()

run_id = str(uuid.uuid4())

log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s')
log_level = logging.DEBUG if args.verbose else logging.INFO
# log_file_root = f'{datetime.utcnow().strftime("%Y-%m-%d")}-{run_id}'
log_file_root = f'{datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")}'
log_handlers = []

stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(log_level)
log_handlers.append(stream_handler)

if args.logging:
    file_handler = logging.FileHandler(os.path.join(args.logging, f'{log_file_root}-driver.log'), mode='w')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(log_level)
    log_handlers.append(file_handler)


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s',
    handlers=log_handlers
)

logger = logging.getLogger('oco3_driver')

dc = shutil.which('docker-compose')
docker = shutil.which('docker')

if dc is None:
    logger.error('docker-compose command could not be found in PATH')
    raise OSError('docker-compose command could not be found in PATH')

the_time = datetime.now()

with open(f'{log_file_root}-stac-search.log', 'w') as fp:
    search_p = subprocess.Popen(
        [dc, '-f', args.dc_search, 'up'],
        stdout=fp,
        stderr=subprocess.STDOUT
    )

search_p.wait()

logger.info(f'STAC search completed in {datetime.now() - the_time}')

subprocess.Popen(
    [dc, '-f', args.dc_search, 'down'],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.STDOUT
).wait()

with open(args.dc_search) as fp:
    search_config = load(fp, Loader=Loader)

stac_file = container_to_host_path(
    search_config['services']['cumulus_granules_search']['environment']['OUTPUT_FILE'],
    *search_config['services']['cumulus_granules_search']['volumes'][0].split(':')[:2]
)

with open(stac_file) as fp:
    cmr_results = json.load(fp)

logger.info('Comparing CMR results to state')

state = load_state(args.state)

dl_features = []

for f in cmr_results['features']:
    filename = f['assets']['data']['href'].split('/')[-1]

    if filename not in state['processed']:
        dl_features.append(f)

if len(dl_features) == 0:
    logger.info('No new data to process')
    exit(0)

logger.info(f"CMR returned {len(dl_features):,} new granules to process")

if args.limit and len(dl_features) > args.limit:
    logger.warning(f'CMR returned more granules ({len(dl_features):,}) than the provided limit ({args.limit:,}). '
                   f'Truncating list of granules to process.')

    dl_features = dl_features[:args.limit]

cmr_results['features'] = dl_features

with open(stac_file, 'w') as fp:
    json.dump(cmr_results, fp, indent=4)

the_time = datetime.now()

with open(f'{log_file_root}-stac-download.log', 'w') as fp:
    stage_p = subprocess.Popen(
        [dc, '-f', args.dc_dl, 'up'],
        stdout=fp,
        stderr=subprocess.STDOUT
    )

stage_p.wait()

logger.info(f'Granule staging completed in {datetime.now() - the_time}')

subprocess.Popen(
    [dc, '-f', args.dc_dl, 'down'],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.STDOUT
).wait()

with open(args.dc_dl) as fp:
    dl_config = load(fp, Loader=Loader)

granule_dir = container_to_host_path(
    dl_config['services']['cumulus_granules_download']['environment']['DOWNLOAD_DIR'],
    *dl_config['services']['cumulus_granules_download']['volumes'][0].split(':')[:2]
)

with open(os.path.join(granule_dir, 'downloaded_feature_collection.json')) as fp:
    downloaded_granules = [feature['assets']['data']['href'].lstrip('./') for feature in json.load(fp)['features']]
    downloaded_granules.sort()

with open(args.rc) as fp:
    pipeline_config = load(fp, Loader=Loader)

pipeline_config['input']['files'] = [os.path.join('/var/inputs/', g) for g in downloaded_granules]

logger.info('Starting pipeline')

with NamedTemporaryFile(
    mode='w', prefix='rc-', suffix='.yaml'
) as rc_fp:
    dump(pipeline_config, rc_fp, Dumper=Dumper, sort_keys=False)
    rc_fp.flush()

    the_time = datetime.now()

    with open(f'{log_file_root}-pipeline.log', 'w') as log_fp:
        pipeline_p = subprocess.Popen(
            [
                docker,
                'run',
                # '--rm',
                '--name',
                'oco-sam-l3',
                '-v',
                f'{rc_fp.name}:/etc/config.yaml',
                '-v',
                f'{granule_dir}:/var/inputs/',
                args.image,
                'python',
                '/sam_extract/main.py',
                '-i',
                '/etc/config.yaml',
                '--skip-netrc'
            ],
            stdout=log_fp,
            stderr=subprocess.STDOUT
        ).wait()

    logger.info(f'Pipeline completed in {datetime.now() - the_time}')

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
            logger.error('Pipeline failed')
            raise OSError('Pipeline failed')
    else:
        logger.error(f'Inspect subprocess error {err}. Assuming pipeline failed')
        raise OSError('Pipeline failed')
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

state['count'] += len(downloaded_granules)
state['processed'].extend(downloaded_granules)

logger.info('Cleaning up & updating state')

for g in [os.path.join(granule_dir, g) for g in downloaded_granules]:
    os.remove(g)

with open(args.state, 'w') as fp:
    json.dump(state, fp, indent=4)

logger.info(f'Script completed in {datetime.now() - start_time}')
