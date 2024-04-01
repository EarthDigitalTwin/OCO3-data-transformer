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
import gc
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from typing import Tuple, Optional, List

import pika
import psutil
import xarray as xr
import yaml
from pika.channel import Channel
from pika.exceptions import AMQPChannelError, AMQPConnectionError, ConnectionClosedByBroker
from pika.spec import Basic, BasicProperties
from sam_extract.exceptions import *
from sam_extract.processors import Processor, PROCESSORS
from sam_extract.utils import ProgressLogging
from sam_extract.utils import ZARR_REPAIR_FILE, PW_STATE_DIR, backup_zarr, delete_zarr_backup, cleanup_xi
from sam_extract.writers import ZarrWriter
from schema import Optional as Opt
from schema import Schema, Or, SchemaError
from yaml import load
from yaml.scanner import ScannerError

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


AWS_CRED_SCHEMA = Schema({
    'path': str,
    'accessKeyID': str,
    'secretAccessKey': str,
})


RMQ_SCHEMA = Schema({
    'inputs': [
        Or(
            str, None, AWS_CRED_SCHEMA,
            {Opt(p): Or(str, None, AWS_CRED_SCHEMA) for p in PROCESSORS.keys()}
        )
    ]
})


FILES_SCHEMA = Schema([
    Or(
        str, None, AWS_CRED_SCHEMA,
        {Opt(p): Or(str, None, AWS_CRED_SCHEMA) for p in PROCESSORS.keys()}
    )
])


DEFAULT_EXCLUDE_GROUPS = [
    '/Preprocessors',
    '/Meteorology',
    '/Sounding'
]


def __validate_files(files):
    FILES_SCHEMA.validate(files)

    for file in files:
        if isinstance(file, dict) and 'path' in file:
            if 'urs' not in file:
                AWS_CRED_SCHEMA.validate(file)
            else:
                if 'accessKeyID' in file or 'secretAccessKey' in file:
                    logger.warning(f'Provided AWS creds for {file["path"]} will be overridden by dynamic STS '
                                   f'creds')


def merge_groups(groups):
    logger.info(f'Merging {len(groups)} interpolated groups')

    groups = [group for group in groups if group is not None]

    if len(groups) == 0:
        return None

    return {group: xr.concat([g[group] for g in groups], dim='time').sortby('time') for group in groups[0]}, len(groups)


def process_inputs(in_files, cfg):
    logger.info(f'Interpolating {len(in_files)} L2 Lite file(s) with interpolation method '
                f'{cfg["grid"].get("method", Processor.DEFAULT_INTERPOLATE_METHOD)}')

    def output_cfg(config):
        config = config['output']

        additional_params = {'verify': True, 'final': True}

        if config['type'] == 'local':
            path_root = config['local']
        else:
            path_root = config['s3']['url']
            additional_params['region'] = config['s3']['region']
            additional_params['auth'] = config['s3']['auth']

        return path_root, additional_params

    with TemporaryDirectory(prefix='oco-sam-extract-', suffix='-zarr-scratch', ignore_cleanup_errors=True) as td:
        exclude = cfg['exclude-groups']

        chunking: Tuple[int, int, int] = cfg['chunking']['config']

        output_root, output_kwargs = output_cfg(cfg)

        zarr_writer_pre = ZarrWriter(
            os.path.join(output_root, cfg['output']['naming']['pre_qf']),
            chunking,
            overwrite=False,
            **output_kwargs
        )

        zarr_writer_post = ZarrWriter(
            os.path.join(output_root, cfg['output']['naming']['post_qf']),
            chunking,
            overwrite=False,
            **output_kwargs
        )

        backup_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='backup_worker')

        backup_pre_future: Future = backup_executor.submit(
            backup_zarr,
            zarr_writer_pre.path,
            zarr_writer_pre.store,
            zarr_writer_pre.store_params
        )

        backup_post_future: Future = backup_executor.submit(
            backup_zarr,
            zarr_writer_post.path,
            zarr_writer_post.store,
            zarr_writer_post.store_params
        )

        def process_input(
                input,
                cfg,
                temp_dir,
                output_pre_qf=True,
                exclude_groups: Optional[List[str]] = None
        ):
            processed_data_tuples = []

            if isinstance(input, str) or (isinstance(input, dict) and 'path' in input):
                processed_data_tuples.append(PROCESSORS['oco3'].process_input(
                    input,
                    cfg=cfg,
                    temp_dir=temp_dir,
                    exclude_groups=exclude_groups
                ))

                # Bit hacky, but it's better than bundling the dt with the input spec
                dt = Processor.granule_to_dt(input if isinstance(input, str) else input['path'])

                for k in PROCESSORS.keys():
                    if k == 'oco3':
                        continue

                    empty_ds = PROCESSORS[k].empty_dataset(dt, cfg, temp_dir)

                    processed_data_tuples.append((
                        empty_ds,
                        empty_ds,
                        True,
                        None
                    ))
            elif isinstance(input, dict):
                input = {k: input[k] for k in input if input[k] is not None}

                input_keys = set(input.keys())
                output_keys = set(PROCESSORS.keys())

                if len(input) == 0:
                    logger.critical('Empty input dictionary provided (this should not be possible)')
                    raise NoValidFilesException()

                if not input_keys.issubset(output_keys):
                    # This should also not be possible, but let's deal with it anyway
                    logger.warning(f'Ignoring some input file types that are not supported: {input_keys - output_keys}')
                    input = {k: input[k] for k in input_keys.intersection(output_keys)}

                for t in input:
                    processed_data_tuples.append(PROCESSORS[t].process_input(
                        input[t],
                        cfg=cfg,
                        temp_dir=temp_dir,
                        exclude_groups=exclude_groups
                    ))

                # Bit hacky, but it's better than bundling the dt with the input spec
                sample_input = input[list(input_keys)[0]]
                sample_granule = sample_input['path'] if isinstance(sample_input, dict) else sample_input
                dt = Processor.granule_to_dt(sample_granule)

                for t in output_keys - input_keys:
                    empty_ds = PROCESSORS[t].empty_dataset(dt, cfg, temp_dir)

                    processed_data_tuples.append((
                        empty_ds,
                        empty_ds,
                        True,
                        None
                    ))
            else:
                raise TypeError(f'Invalid input type {type(input)}')

            def merge(groups):
                if len(groups) <= 1:
                    return groups[0]

                return {group: xr.merge([g[group] for g in groups]) for group in groups[0]}

            return (
                merge([pdt[0] for pdt in processed_data_tuples]),
                merge([pdt[1] for pdt in processed_data_tuples]),
                all([pdt[2] for pdt in processed_data_tuples]),
                input
            )

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
                        logger.info(f'No pre-QF data generated for {path}; likely no SAMs/targets were present')

                    if result_post is not None:
                        processed_groups_post.append(result_post)
                    else:
                        logger.info(f'No post-QF data generated for {path}; likely no SAMs/targets were present or they'
                                    f' were all filtered out')
                else:
                    failed_inputs.append(path)

        if len(failed_inputs) == len(in_files):
            logger.critical('No input files could be read!')
            raise NoValidFilesException()
        elif len(failed_inputs) > 0:
            logger.error(f'Some input files failed because they could not be read:')
            for failed in failed_inputs:
                logger.error(f' - {failed}')

        merged_pre, len_pre = merge_groups(processed_groups_pre)
        merged_post, len_post = merge_groups(processed_groups_post)

        logger.info('Data generation complete. Will proceed to final write when backups are completed...')

        backup_pre = backup_pre_future.result()
        backup_post = backup_post_future.result()

        logger.info('Backups completed. Proceeding.')

        Path(PW_STATE_DIR).mkdir(parents=True, exist_ok=True)
        repair_file_data = dict(pre_qf_backup=backup_pre, post_qf_backup=backup_post)

        with open(ZARR_REPAIR_FILE, 'w') as fp:
            json.dump(repair_file_data, fp)

        backup_executor.shutdown()

        if merged_pre is not None:
            logger.info(f'Writing {len_pre} days of pre_qf data')

            with ProgressLogging(log_level=logging.INFO):
                zarr_writer_pre.write(
                    merged_pre,
                    attrs=dict(
                        title=cfg['output']['title']['pre_qf'],
                        quality_flag_filtered='no'
                    )
                )
        else:
            logger.info('No pre_qf data generated')

        if merged_post is not None:
            logger.info(f'Writing {len_pre} days of post_qf data')

            with ProgressLogging(log_level=logging.INFO):
                zarr_writer_post.write(
                    merged_post,
                    attrs=dict(
                        title=cfg['output']['title']['post_qf'],
                        quality_flag_filtered='yes'
                    )
                )
        else:
            logger.info('No post_qf data generated, all SAMs and target soundings on these days may have been filtered '
                        'out for bad qf')

        logger.info(f'Cleaning up temporary directory at {td}')

        try:
            os.remove(ZARR_REPAIR_FILE)
        except:
            logger.warning('Could not remove Zarr write status file')

        if backup_pre is not None and not delete_zarr_backup(
            backup_pre,
            zarr_writer_pre.store,
            zarr_writer_pre.store_params,
            verify=False
        ):
            logger.warning(f'Unable to remove backup for pre_qf zarr data at {backup_pre}')

        if backup_pre is not None and not delete_zarr_backup(
            backup_post,
            zarr_writer_post.store,
            zarr_writer_post.store_params,
            verify=False
        ):
            logger.warning(f'Unable to remove backup for post_qf zarr data at {backup_post}')


class ProcessThread(threading.Thread):
    def __init__(self, target, args, name):
        threading.Thread.__init__(self, target=target, args=args, name=name)
        self.exc = None

    def run(self) -> None:
        try:
            if self._target is not None:
                self._target(*self._args, **self._kwargs)
        except BaseException as err:
            self.exc = err
        finally:
            del self._target, self._args, self._kwargs

    def join(self, timeout=None) -> None:
        threading.Thread.join(self, timeout)

        if self.exc:
            raise self.exc


def main(cfg):
    if cfg['input']['type'] == 'queue':
        def on_message(
                channel: Channel,
                method_frame: Basic.Deliver,
                header_frame: BasicProperties,
                body: bytes):
            logger.info('Received message from input queue')
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

                thread = ProcessThread(
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

                thread.join()

                logger.info(f'Pipeline completed in {datetime.now() - pipeline_start_time}')
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                logger.debug('ACKing message')

                gc.collect()
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
                # NACK the message because the error may not be related to the input.
                # This scenario should need to be manually investigated
                channel.basic_nack(delivery_tag=method_frame.delivery_tag)
                raise
            except KeyboardInterrupt:
                channel.basic_nack(delivery_tag=method_frame.delivery_tag)
                raise
            except Exception:
                logger.error('An exception has occurred, requeueing input message')
                channel.basic_nack(delivery_tag=method_frame.delivery_tag)
                raise

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

    parser.add_argument(
        '-v',
        help='Verbose logging output',
        dest='verbose',
        action='store_true'
    )

    args, unknown = parser.parse_known_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger.setLevel(log_level)

    for handler in logging.getLogger().handlers:
        handler.setLevel(log_level)

    if len(unknown) > 0:
        logger.warning(f'Unknown commands provided: {unknown}')

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
        else:
            assert 'pre_qf' in output['naming'], 'Must specify pre_qf name (output.naming.pre_qf)'
            assert 'post_qf' in output['naming'], 'Must specify post_qf name (output.naming.post_qf)'

        if 'title' not in output:
            title = dict(
                pre_qf=output['naming']['pre_qf'].split('.zarr')[0],
                post_qf=output['naming']['post_qf'].split('.zarr')[0],
            )

            config_dict['output']['title'] = title
        else:
            assert 'pre_qf' in output['title'], 'Must specify pre_qf title (output.title.pre_qf)'
            assert 'post_qf' in output['title'], 'Must specify post_qf title (output.title.post_qf)'

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

        if 'chunking' in config_dict:
            config_dict['chunking']['config'] = (
                config_dict['chunking'].get('time', 5),
                config_dict['chunking'].get('longitude', 250),
                config_dict['chunking'].get('latitude', 250),
            )
        else:
            config_dict['chunking'] = {'config': (5, 250, 250)}

        if 'exclude-groups' not in config_dict:
            config_dict['exclude-groups'] = DEFAULT_EXCLUDE_GROUPS
    except KeyError as err:
        logger.exception(err)
        raise ValueError('Invalid configuration')

    return config_dict


if __name__ == '__main__':
    start_time = datetime.now()
    v = -1

    DEBUG_MPROF = os.getenv('DEBUG_MPROF')

    profile_logger = None
    process = None
    start_rss, start_vms = None, None

    if DEBUG_MPROF is not None:
        try:
            interval = float(DEBUG_MPROF)
            assert 0.1 <= interval <= 300
        except:
            interval = 15

        process = psutil.Process(os.getpid())

        start_rss = process.memory_info().rss
        start_vms = process.memory_info().vms

        profile_logger = logging.getLogger('debug_mprof')

        def profile_memory():
            while True:
                KB_rss = int((process.memory_info().rss - start_rss) / (1024 ** 2))
                KB_vms = int((process.memory_info().vms - start_vms) / (1024 ** 2))
                profile_logger.debug('rss_used={0:10,d} MiB vms_used={1:10,d} MiB'.format(KB_rss, KB_vms))

                sleep(interval)

        threading.Thread(
            target=profile_memory,
            daemon=True,
            name='mprof_thread'
        ).start()

    try:
        main(parse_args())
        v = 0
    except Exception as e:
        logger.critical('An unrecoverable exception has occurred')
        logger.exception(e)
        v = 1
    finally:
        cleanup_xi()

        logger.info(f'Exiting code {v}. Runtime={datetime.now() - start_time}')
        if DEBUG_MPROF is not None:
            KB_rss = int((process.memory_info().rss - start_rss) / (1024 ** 2))
            KB_vms = int((process.memory_info().vms - start_vms) / (1024 ** 2))
            profile_logger.debug('rss_used={0:10,d} MiB vms_used={1:10,d} MiB (final)'.format(KB_rss, KB_vms))
        exit(v)
