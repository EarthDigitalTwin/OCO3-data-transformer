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

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime
from functools import partial
from tempfile import NamedTemporaryFile

import boto3
from schema import Schema, SchemaError
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
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

parser.add_argument(
    '--local-write-dir',
    dest='lwd',
    help='If writing to S3 and this is defined, write/append final output to this dir, copy chunks to S3, then delete '
         'non-edge, non-coord chunks. Useful for scenarios with long write times.'
)

parser.add_argument(
    '--purge-lw',
    dest='keep_local',
    action='store_false',
    help=argparse.SUPPRESS
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

SUPPRESS = [
    'botocore',
    's3transfer',
    'urllib3',
]

for logger_name in SUPPRESS:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

dc = shutil.which('docker-compose')
docker = shutil.which('docker')

if dc is None:
    logger.error('docker-compose command could not be found in PATH')
    raise OSError('docker-compose command could not be found in PATH')

the_time = datetime.now()

logger.info('Beginning CMR search')

with open(f'{log_file_root}-stac-search.log', 'w') as fp:
    search_p = subprocess.Popen(
        [dc, '-f', args.dc_search, 'up'],
        stdout=fp,
        stderr=subprocess.STDOUT
    )

search_p.wait()

logger.info(f'CMR search completed in {datetime.now() - the_time}')

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

replaced_output = False
output_cfg = deepcopy(pipeline_config['output'])

if 's3' in output_cfg and args.lwd:
    del pipeline_config['output']['s3']
    pipeline_config['output']['local'] = '/var/outputs/'
    replaced_output = True
    logger.info('Will write Zarr locally & copy to S3')
    output_mount_dir = args.lwd
elif 'local' in output_cfg:
    output_mount_dir = urllib.parse.urlparse(output_cfg['local']).path
    pipeline_config['output']['local'] = '/var/outputs/'
else:
    output_mount_dir = None


def run_pipeline(rerun=False):
    logger.info('Starting pipeline')

    with NamedTemporaryFile(
        mode='w', prefix='rc-', suffix='.yaml'
    ) as rc_fp:
        dump(pipeline_config, rc_fp, Dumper=Dumper, sort_keys=False)
        rc_fp.flush()

        the_time = datetime.now()

        pipeline_logfile = f'{log_file_root}-pipeline.log' if not rerun else f'{log_file_root}-pipeline-rerun.log'

        user_id = os.getenv('USERID', os.getuid())
        group_id = os.getenv('GRPID', os.getgid())
        u_param = f'{user_id}:{group_id}'

        with open(pipeline_logfile, 'w') as log_fp:
            if output_mount_dir is None:
                p_args = [
                    docker,
                    'run',
                    '--name',
                    'oco-sam-l3',
                    '-u',
                    u_param,
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
                ]
            else:
                p_args = [
                    docker,
                    'run',
                    '--name',
                    'oco-sam-l3',
                    '-u',
                    u_param,
                    '-v',
                    f'{rc_fp.name}:/etc/config.yaml',
                    '-v',
                    f'{granule_dir}:/var/inputs/',
                    '-v',
                    f'{output_mount_dir}:/var/outputs/',
                    args.image,
                    'python',
                    '/sam_extract/main.py',
                    '-i',
                    '/etc/config.yaml',
                    '--skip-netrc'
                ]

            pipeline_p = subprocess.Popen(
                p_args,
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
                logger.error(f'Pipeline failed: {exit_code}')
                logger.debug(json.dumps(stats, indent=4))
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

    return pipeline_logfile


pipeline__log = run_pipeline()

if replaced_output:
    session = boto3.Session(profile_name=os.getenv('AWS_PROFILE'))
    s3 = session.client('s3')

    with open(pipeline__log) as fp:
        repaired = any(['Writing corrected group' in l for l in fp.readlines()])

    if not repaired:
        logger.info('Copying files to S3')

        output_files = [os.path.join(dp, f) for z in
                        [output_cfg['naming']['pre_qf'], output_cfg['naming']['post_qf']]
                        for dp, dn, fn in os.walk(os.path.join(args.lwd, z)) for f in fn]

        s3_url = urllib.parse.urlparse(output_cfg['s3']['url'])

        bucket = s3_url.netloc
        prefix_key = s3_url.path[1:]

        output_keys = [f.replace(args.lwd, prefix_key, 1).replace('//', '/') for f in output_files]

        with tqdm(total=len(output_keys), desc='S3 Upload', unit=' object', ncols=120) as pb:
            for src, dst in zip(output_files, output_keys):
                with open(src, 'rb') as fp:
                    object_data = fp.read()

                s3.put_object(Bucket=bucket, Key=dst, Body=object_data)

                pb.update()

            pb.close()
    else:
        logger.error('Pipeline produced output that was incorrect; need to retry with source S3 as output')

        replaced_output = False
        pipeline_config['output'] = output_cfg

        run_pipeline(rerun=True)

        lwd_files = [os.path.join(dp, f) for dp, dn, fn in os.walk(args.lwd) for f in fn]

        logger.info('Pipeline complete; purging local write directory')

        for f in lwd_files:
            os.remove(f)

        logger.info('Pulling S3 data to local write directory')

        zarr_urls = [
            f"{output_cfg['s3']['url'].rstrip('/')}/{output_cfg['naming']['pre_qf']}",
            f"{output_cfg['s3']['url'].rstrip('/')}/{output_cfg['naming']['post_qf']}",
        ]

        root_key = urllib.parse.urlparse(output_cfg['s3']['url'].rstrip('/')).path[1:]

        with logging_redirect_tqdm():
            with tqdm(total=float('inf'), desc='S3 Download', unit=' object') as pb:
                for s3_url in zarr_urls:
                    s3_url = urllib.parse.urlparse(s3_url)

                    bucket = s3_url.netloc
                    prefix_key = s3_url.path[1:]

                    paginator = s3.get_paginator('list_objects_v2')

                    bucket_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix_key)

                    logger.debug(f'paginating {bucket} w/ prefix {prefix_key}')

                    for page in bucket_iterator:
                        for obj in page.get('Contents', []):
                            dl_path = os.path.join(args.lwd, obj['Key'].replace(root_key, '', 1).lstrip('/'))

                            logger.debug(f'Download: s3://{bucket}/{obj["Key"]} -> {dl_path}')
                            s3.download_file(bucket, obj['Key'], dl_path)

                            pb.update()

                pb.close()

        output_files = [os.path.join(dp, f) for z in
                        [output_cfg['naming']['pre_qf'], output_cfg['naming']['post_qf']]
                        for dp, dn, fn in os.walk(os.path.join(args.lwd, z)) for f in fn]

    if not args.keep_local:
        chunk_files = [f for f in output_files if re.search('\\d+\\.\\d+\\.\\d+', f) is not None]

        logger.debug(f'There are {len(chunk_files):,} chunks out of {len(output_files):,} files')

        max_time_chunk = max([int(os.path.basename(f).split('.')[0]) for f in chunk_files])

        logger.debug(f'Highest chunk time idx={max_time_chunk}')

        to_purge = [f for f in chunk_files if int(os.path.basename(f).split('.')[0]) < max_time_chunk]

        if len(to_purge) == len(chunk_files):
            logger.error('Something went wrong when computing chunks to delete: All were selected')
            logger.error(f'There are {len(chunk_files):,} chunks out of {len(output_files):,} files')
            logger.error(f'Highest chunk time idx={max_time_chunk}')

            try:
                with open('/tmp/chunks.json', 'w') as fp:
                    json.dump(
                        dict(
                            files=sorted(output_files), chunks=sorted(chunk_files), selected_to_purge=sorted(to_purge)
                        ), fp, indent=4
                    )

                logger.info('Dumped chunk lists to /tmp/chunks.json')
            except:
                pass

        logger.info(f'Deleting {len(to_purge):,} old chunks')

        for f in to_purge:
            try:
                os.remove(f)
            except OSError as e:
                logger.error(f'Failed to remove file {f}: {e}')


state['count'] += len(downloaded_granules)
state['processed'].extend(downloaded_granules)

logger.info('Cleaning up & updating state')

for g in [os.path.join(granule_dir, g) for g in downloaded_granules]:
    os.remove(g)

with open(args.state, 'w') as fp:
    json.dump(state, fp, indent=4)

logger.info(f'Script completed in {datetime.now() - start_time}')
