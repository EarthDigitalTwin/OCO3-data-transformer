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
import itertools
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from typing import Tuple, Optional, List, Literal

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
from sam_extract.writers import ZarrWriter, CoGWriter
from schema import Optional as Opt
from schema import Schema, Or, SchemaError
from shapely.geometry import box
from yaml import load
from yaml.scanner import ScannerError

from runconfig import RunConfig

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


logging.basicConfig(
    level=logging.TRACE,
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
            {Opt(p): Or(str, None, AWS_CRED_SCHEMA) for p in set(
                [pr for k in PROCESSORS.keys() for pr in PROCESSORS[k].keys()]
            )}
        )
    ]
})


FILES_SCHEMA = Schema([
    Or(
        str, None, AWS_CRED_SCHEMA,
        {Opt(p): Or(str, None, AWS_CRED_SCHEMA) for p in set(
                [pr for k in PROCESSORS.keys() for pr in PROCESSORS[k].keys()]
            )}
    )
])


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


def merge_groups(groups: list):
    logger.debug(f'Merging {len(groups)} interpolated groups')

    groups = [group for group in groups if group is not None]

    if len(groups) == 0:
        return None, 0

    return {
        group: xr.concat([g[group] for g in groups if group in g], dim='time').sortby('time') for group in groups[0]
    }, len(groups)


def process_inputs(in_files, cfg: RunConfig):
    logger.info(f'Interpolating {len(in_files)} L2 Lite file(s) with interpolation method '
                f'{cfg.grid_method(Processor.DEFAULT_INTERPOLATE_METHOD)}')

    def output_cfg(config: RunConfig):
        additional_params = {'verify': True, 'final': True}

        if config.output_type == 'local':
            path_root = config.output
        else:
            path_root = config.output['url']
            additional_params['region'] = config.output_aws_region
            additional_params['auth'] = config.output['auth']

        return path_root, additional_params

    with TemporaryDirectory(prefix='oco-sam-extract-', suffix='-zarr-scratch', ignore_cleanup_errors=True) as td:
        chunking: Tuple[int, int, int] = cfg.chunking

        output_root, output_kwargs = output_cfg(cfg)

        backup_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='backup_worker')

        backup_store_params = {'region': output_kwargs.get('region', 'us-west-2'), 'auth': output_kwargs.get('auth')}

        backup_pre_future: Future = backup_executor.submit(
            backup_zarr,
            str(os.path.join(output_root, cfg.pre_qf_name)),
            's3' if output_root.startswith('s3://') else 'local',
            backup_store_params,
            verify_zarr=cfg.is_global
        )

        backup_post_future: Future = backup_executor.submit(
            backup_zarr,
            str(os.path.join(output_root, cfg.post_qf_name)),
            's3' if output_root.startswith('s3://') else 'local',
            backup_store_params,
            verify_zarr=cfg.is_global
        )

        # Weird way of assigning this to avoid annoying type hint warnings in my IDE
        if cfg.is_global:
            product_type: Literal['local', 'global'] = 'global'
        else:
            product_type: Literal['local', 'global'] = 'local'

        def process_input(
                input,
                cfg: RunConfig,
                temp_dir,
        ):
            processed_data_tuples = []

            if isinstance(input, str) or (isinstance(input, dict) and 'path' in input):
                if cfg.is_global:
                    processed_data_tuples.append(PROCESSORS[product_type]['oco3'].process_input(
                        input,
                        cfg=cfg,
                        temp_dir=temp_dir,
                    ))

                    # Bit hacky, but it's better than bundling the dt with the input spec
                    dt = Processor.granule_to_dt(input if isinstance(input, str) else input['path']).replace(
                        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
                    )

                    for k in list(set([p for k in PROCESSORS.keys() for p in PROCESSORS[k].keys()])):
                        if k == 'oco3':
                            continue

                        empty_ds = PROCESSORS[product_type][k].empty_dataset(dt, cfg, temp_dir)

                        processed_data_tuples.append((
                            empty_ds,
                            empty_ds,
                            True,
                            None
                        ))
                else:
                    processed_data_tuples.append(
                        {'oco3': PROCESSORS[product_type]['oco3'].process_input(
                            input['oco3'],
                            cfg=cfg,
                            temp_dir=temp_dir,
                        )}
                    )
            elif isinstance(input, dict):
                input = {k: input[k] for k in input if input[k] is not None}

                input_keys = set(input.keys())
                output_keys = set([p for k in PROCESSORS.keys() for p in PROCESSORS[k].keys()])

                if len(input) == 0:
                    logger.critical('Empty input dictionary provided (this should not be possible)')
                    raise NoValidFilesException()

                if not input_keys.issubset(output_keys):
                    # This should also not be possible, but let's deal with it anyway
                    logger.warning(f'Ignoring some input file types that are not supported: {input_keys - output_keys}')
                    input = {k: input[k] for k in input_keys.intersection(output_keys)}

                if cfg.is_global:
                    for t in input:
                        processed_data_tuples.append(PROCESSORS[product_type][t].process_input(
                            input[t],
                            cfg=cfg,
                            temp_dir=temp_dir,
                        ))

                    # Bit hacky, but it's better than bundling the dt with the input spec
                    sample_input = input[list(input_keys)[0]]
                    sample_granule = sample_input['path'] if isinstance(sample_input, dict) else sample_input
                    dt = Processor.granule_to_dt(sample_granule).replace(
                        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
                    )

                    for t in output_keys - input_keys:
                        empty_ds = PROCESSORS[product_type][t].empty_dataset(dt, cfg, temp_dir)

                        processed_data_tuples.append((
                            empty_ds,
                            empty_ds,
                            True,
                            None
                        ))
                else:
                    processed_data_tuples.append(
                        {t: PROCESSORS[product_type][t].process_input(
                            input[t],
                            cfg=cfg,
                            temp_dir=temp_dir,
                        ) for t in input}
                    )
            else:
                raise TypeError(f'Invalid input type {type(input)}')

            def merge(groups):
                if len(groups) <= 1:
                    return groups[0]

                return {group: xr.merge([g[group] for g in groups if group in g]) for group in groups[0]}

            if cfg.is_global:
                return (
                    merge([pdt[0] for pdt in processed_data_tuples]),
                    merge([pdt[1] for pdt in processed_data_tuples]),
                    all([pdt[2] for pdt in processed_data_tuples]),
                    input
                )
            else:
                return (
                    {t: [pdt[t][0][r] for pdt in processed_data_tuples for r in range(len(pdt[t][0]))] for t in input},
                    {t: [pdt[t][1][r] for pdt in processed_data_tuples for r in range(len(pdt[t][1]))] for t in input},
                    {t: all([pdt[t][2] for pdt in processed_data_tuples]) for t in input},
                    input
                )

        process = partial(process_input, cfg=cfg, temp_dir=td)

        with ThreadPoolExecutor(max_workers=cfg.max_workers, thread_name_prefix='process_worker') as pool:
            processed_groups_pre = []
            processed_groups_post = []
            failed_inputs = []

            for result_pre, result_post, success, path in pool.map(process, in_files):
                if success:
                    if result_pre is not None:
                        if isinstance(result_pre, dict):
                            processed_groups_pre.append(result_pre)
                        else:
                            processed_groups_pre.extend(result_pre)
                    else:
                        logger.info(f'No pre-QF data generated for {path}; likely no SAMs/targets were present')

                    if result_post is not None:
                        if isinstance(result_post, dict):
                            processed_groups_post.append(result_post)
                        else:
                            processed_groups_post.extend(result_post)
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

        logger.info('Data generation complete. Will proceed to final write when backups are completed...')

        backup_pre = backup_pre_future.result()
        backup_post = backup_post_future.result()

        Path(PW_STATE_DIR).mkdir(parents=True, exist_ok=True)
        repair_file_data = dict(pre_qf_backup=backup_pre, post_qf_backup=backup_post)

        with open(ZARR_REPAIR_FILE, 'w') as fp:
            json.dump(repair_file_data, fp)

        backup_executor.shutdown()

        logger.info('Backups completed. Proceeding.')

        if cfg.is_global:
            merged_pre, len_pre = merge_groups(processed_groups_pre)
            merged_post, len_post = merge_groups(processed_groups_post)

            zarr_writer_pre = ZarrWriter(
                str(os.path.join(output_root, cfg.pre_qf_name)),
                chunking,
                overwrite=False,
                **output_kwargs
            )

            zarr_writer_post = ZarrWriter(
                str(os.path.join(output_root, cfg.post_qf_name)),
                chunking,
                overwrite=False,
                **output_kwargs
            )

            if merged_pre is not None:
                logger.info(f'Writing {len_pre} days of pre_qf data')

                with ProgressLogging(log_level=logging.INFO):
                    zarr_writer_pre.write(
                        merged_pre,
                        attrs=dict(
                            title=cfg.pre_qf_title,
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
                            title=cfg.post_qf_title,
                            quality_flag_filtered='yes'
                        )
                    )
            else:
                logger.info('No post_qf data generated, all SAMs and target soundings on these days may have been '
                            'filtered out for bad qf')
        else:
            # Collect by key: target ID -> list of groups
            # Merge all slices for each target ID to reduce net number of writes
            # Restore list of tuples structure? and iterate

            def merge_by_site(processed_groups):
                target_dict: dict = {}

                for g, t in processed_groups:
                    target_dict.setdefault(t, []).append(g)

                for t in target_dict:
                    target_dict[t] = merge_groups(target_dict[t])[0]

                return [(target_dict[t], t) for t in target_dict]

            processed_groups_pre = {
                m: list(itertools.chain(*[p[m] for p in processed_groups_pre if m in p])) for m in PROCESSORS['local']
            }

            processed_groups_post = {
                m: list(itertools.chain(*[p[m] for p in processed_groups_post if m in p])) for m in PROCESSORS['local']
            }

            processed_groups_pre = {m: merge_by_site(processed_groups_pre[m]) for m in processed_groups_pre}
            processed_groups_post = {m: merge_by_site(processed_groups_post[m]) for m in processed_groups_post}

            for m in processed_groups_pre:
                processed_groups_pre[m].sort(key=lambda x: x[1])
            for m in processed_groups_post:
                processed_groups_post[m].sort(key=lambda x: x[1])

            targets = {}

            def have_data_for_types(*types: str):
                return any(
                    [len(o.get(t, [])) > 0 for t in types for o in [processed_groups_pre, processed_groups_post]]
                )

            try:
                with open(cfg.target_file_3) as fp:
                    target_data = json.load(fp)

                    for t in ['oco3', 'oco3_sif']:
                        if have_data_for_types(t):
                            targets[t] = target_data
            except (ValueError, FileNotFoundError):
                logger.error(f'Could not load target file for OCO-3')
                if have_data_for_types('oco3', 'oco3_sif'):
                    raise

            try:
                with open(cfg.target_file_2) as fp:
                    target_data = json.load(fp)

                    for t in ['oco2']:
                        if have_data_for_types(t):
                            targets[t] = target_data
            except (ValueError, FileNotFoundError):
                logger.error(f'Could not load target file for OCO-2')
                if have_data_for_types('oco2'):
                    raise

            for m in set(list(processed_groups_pre.keys()) + list(processed_groups_post.keys())):
                logger.info(f'Writing produced outputs for {m}')

                if cfg.cog:
                    def cog_output_cfg(config: RunConfig):
                        additional_params = {'verify': True, 'final': True}

                        if config.cog_output_type == 'local':
                            path_root = config.cog_output
                        else:
                            path_root = config.cog_output['url']
                            additional_params['region'] = config.cog_output.get('region', 'us-west-2')
                            additional_params['auth'] = config.cog_output['auth']

                        return path_root, additional_params

                    cog_root, cog_kwargs = cog_output_cfg(cfg)

                    cog_kwargs['efs'] = cfg.cog_efs

                    driver_options = dict(
                        compress='lzw',
                        resampling='NEAREST',
                        overview_resampling='NEAREST',
                        overviews='AUTO',
                        level=9
                    )

                    driver_options.update(cfg.cog_options)
                    driver_options = CoGWriter.validate_options(driver_options)

                    cog_writer_pre = CoGWriter(cog_root, m, False, driver_kwargs=driver_options, **cog_kwargs)
                    cog_writer_post = CoGWriter(cog_root, m, True, driver_kwargs=driver_options, **cog_kwargs)
                else:
                    cog_writer_pre = None
                    cog_writer_post = None

                def do_write(result_tuple: tuple, qf):
                    result, target = result_tuple

                    if qf == 'pre':
                        qf_key = 'pre_qf'
                        cog_writer = cog_writer_pre
                    else:
                        qf_key = 'post_qf'
                        cog_writer = cog_writer_post

                    if result is None:
                        logger.warning(f'Target {target} present in {qf_key} results but without any data')
                        return

                    writer = ZarrWriter(
                        str(os.path.join(output_root, cfg.naming_dict[qf_key], m, f'{target}.zarr')),
                        chunking,
                        overwrite=False,
                        **output_kwargs
                    )

                    writer.write(
                        result,
                        attrs=dict(
                            title=cfg.title_dict[qf_key],
                            target_id=target,
                            target_name=targets[m][target].get('name', 'UNK'),
                            quality_flag_filtered='no' if qf == 'pre' else 'yes',
                            target_bbox=box(
                                targets[m][target]['bbox']['min_lon'],
                                targets[m][target]['bbox']['min_lat'],
                                targets[m][target]['bbox']['max_lon'],
                                targets[m][target]['bbox']['max_lat'],
                            ).wkt
                        ),
                        global_product=False,
                        mission=m
                    )

                    if cog_writer is not None:
                        cog_writer.write(
                            result,
                            target,
                            attrs=dict(
                                title=cfg.title_dict[qf_key],
                                target_id=target,
                                target_name=targets[m][target].get('name', 'UNK'),
                                quality_flag_filtered='no' if qf == 'pre' else 'yes',
                                target_bbox=box(
                                    targets[m][target]['bbox']['min_lon'],
                                    targets[m][target]['bbox']['min_lat'],
                                    targets[m][target]['bbox']['max_lon'],
                                    targets[m][target]['bbox']['max_lat'],
                                ).wkt
                            )
                        )

                    return target, targets[m][target].get('name', 'name unknown'), qf_key

                with ThreadPoolExecutor(
                        max_workers=min(cfg.max_workers, 4),
                        thread_name_prefix='write_worker'
                ) as pool:
                    futures = []

                    for r in processed_groups_pre[m]:
                        futures.append(pool.submit(do_write, r, 'pre'))

                    for r in processed_groups_post[m]:
                        futures.append(pool.submit(do_write, r, 'post'))

                    i = 1

                    for f in as_completed(futures):
                        result_tuple = f.result()

                        if result_tuple is not None:
                            tid, name, qf = result_tuple

                            logger.info(f'Finished {qf} write for {tid} ({name}) {m} progress: [{i:,}/{len(futures):,}] '
                                        f'[{i/len(futures)*100:7.3f}%]')
                        else:
                            logger.info(f'Skipped undefined target on output. {m} progress: [{i:,}/{len(futures):,}] '
                                        f'[{i/len(futures)*100:7.3f}%]')

                        i += 1

                if cog_writer_pre is not None:
                    cog_writer_pre.finish()
                    cog_writer_post.finish()

        logger.info(f'Cleaning up temporary directory at {td}')

        try:
            os.remove(ZARR_REPAIR_FILE)
        except:
            logger.warning('Could not remove Zarr write status file')

        if backup_pre is not None and not delete_zarr_backup(
            backup_pre,
            's3' if output_root.startswith('s3://') else 'local',
            backup_store_params,
            verify=False
        ):
            logger.warning(f'Unable to remove backup for pre_qf zarr data at {backup_pre}')

        if backup_pre is not None and not delete_zarr_backup(
            backup_post,
            's3' if output_root.startswith('s3://') else 'local',
            backup_store_params,
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


def main(cfg: RunConfig):
    if cfg.input_type == 'queue':
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

        queue_config = cfg.input

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
    elif cfg.input_type == 'files':
        process_inputs(cfg.input, cfg)


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
        help='Verbose logging output level. 0 for INFO, 1 for DEBUG, 2+ for TRACE',
        dest='verbose',
        action='count',
        default=0
    )

    args, unknown = parser.parse_known_args()

    verbosity = min(max(0, args.verbose), 2)

    if verbosity == 0:
        log_level = logging.INFO
    elif verbosity == 1:
        log_level = logging.DEBUG
    else:
        log_level = logging.TRACE

    logger.setLevel(log_level)

    for handler in logging.getLogger().handlers:
        handler.setLevel(log_level)

    if len(unknown) > 0:
        logger.warning(f'Unknown commands provided: {unknown}')

    try:
        run_config = RunConfig.parse_config_file(args.cfg)
    except ConfigError as err:
        logger.critical(f'Provided run config is invalid: {err!r}')
        raise

    if run_config.input_type == 'files':
        __validate_files(run_config.input)

    return run_config


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
                profile_logger.info('rss_used={0:10,d} MiB vms_used={1:10,d} MiB'.format(KB_rss, KB_vms))

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
            profile_logger.info('rss_used={0:10,d} MiB vms_used={1:10,d} MiB (final)'.format(KB_rss, KB_vms))

        exit(v)
