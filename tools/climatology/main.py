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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
from typing import List
from urllib.parse import urlparse, ParseResult, urlunparse

import boto3
import dask
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from aiobotocore.session import AioSession as S3FSSession
from botocore.exceptions import ClientError
from dask.diagnostics import ProgressBar
from s3fs import S3Map, S3FileSystem

XARRAY_8016 = tuple([int(n) for n in xr.__version__.split('.')[:3]]) >= (2023, 8, 0)
TIME_CHUNKING = (4000,)
ISO_8601_D = "%Y-%m-%d"
ISO_8601_DT = "%Y-%m-%dT%H:%M:%S%zZ"

NP_EPOCH = np.datetime64('1970-01-01T00:00:00')

MAX_CONCURRENCY = min(max(1, int(os.getenv('MAX_WORKERS', 12))), 12)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s')
logger = logging.getLogger('climatology.main')

VERSION = '1.0.0'

session = None


def get_boto_session(args=None):
    global session

    if session is not None:
        return session

    if args is None:
        return None

    logger.info('Creating global boto session')

    try:
        kwargs = dict(region_name=args.aws_region)

        if args.aws_profile:
            kwargs['profile_name'] = args.aws_profile

        session = boto3.session.Session(**kwargs)
        return session
    except:
        return None


def exists(url: ParseResult, args):
    logger.info(f'Checking if {urlunparse(url)} exists')

    if url.scheme in ['', 'file']:
        return os.path.exists(url.path)
    elif url.scheme == 's3':
        client = get_boto_session(args).client(
            's3',
            aws_access_key_id=args.aws_key,
            aws_secret_access_key=args.aws_secret,
            region_name=args.aws_region
        )

        bucket = url.netloc
        key = url.path

        # key will likely be of the form '/path/to/root'. Remove leading / and append /.zgroup
        # since head_object doesn't seem to work for directories. This will also ensure that we
        # return true iff a zarr array exists @ this path
        if key[0] == '/':
            key = key[1:]

        if key[-1] == '/':
            key = f'{key}.zgroup'
        else:
            key = f'{key}/.zgroup'

        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except client.exceptions.NoSuchKey:
            return False
        except ClientError as e:
            r = e.response
            err_code = r["Error"]["Code"]

            if err_code == '404':
                return False

            logger.error(f'An AWS error occurred: Code={err_code} Message={r["Error"]["Message"]}')
            raise
        except Exception as e:
            logger.error('Something went wrong!')
            logger.exception(e)
            raise
    else:
        raise ValueError(f'Invalid URL type: {url.scheme}')


def open_dataset(url: ParseResult, args):
    logger.info(f'Opening dataset at {urlunparse(url)}')

    if url.scheme in ['', 'file']:
        store = url.path
    else:
        if args.aws_profile:
            session = S3FSSession(profile=args.aws_profile)
            s3 = S3FileSystem(
                False,
                session=session,
                client_kwargs=dict(region_name=args.aws_region)
            )
        else:
            s3 = S3FileSystem(
                False,
                key=args.aws_key,
                secret=args.aws_secret,
                client_kwargs=dict(region_name=args.aws_region)
            )

        store = S3Map(root=urlunparse(url), s3=s3, check=False)

    return xr.open_zarr(store, mask_and_scale=True)


def main():
    args = parse_args()

    input_url = urlparse(args.input)
    output_url = urlparse(args.output)

    if not exists(input_url, args):
        raise FileNotFoundError(f'No zarr data found at {urlunparse(input_url)}')

    if not args.overwrite and exists(output_url, args):
        raise ValueError(f'Something exists already at {urlunparse(output_url)}')

    logger.info('Opening input dataset')

    input_ds = open_dataset(input_url, args)

    orig_chunk = input_ds.xco2.data.chunksize

    start_date = datetime.utcfromtimestamp(
        (input_ds.time[0].to_numpy() - NP_EPOCH) / np.timedelta64(1, 's')
    )

    end_date = datetime.utcfromtimestamp(
        (input_ds.time[-1].to_numpy() - NP_EPOCH) / np.timedelta64(1, 's')
    )

    if args.start and args.start > start_date:
        start_date = args.start

    if args.end and args.end < end_date:
        end_date = args.end

    logger.info(f'Determining date ranges between {start_date.strftime(ISO_8601_D)} and {end_date.strftime(ISO_8601_D)}')

    freq = 'M' if args.span in ['monthly', 'seasonal', 'monthly-consolidated'] else 'A'

    pd_dates_i: pd.DatetimeIndex = pd.date_range(
        start=start_date,
        end=end_date,
        freq=freq,
    )

    pd_dates: List[datetime] = [d.to_pydatetime().replace(hour=start_date.hour) for d in pd_dates_i]

    if args.span == 'seasonal':
        pd_dates = [dt.replace(day=20) for dt in list(filter(lambda d: d.month % 3 == 0, pd_dates))]

    time_slices = [
        slice(start_date, pd_dates[0])
    ]

    for i in range(1, len(pd_dates)):
        time_slices.append(
            slice(
                pd_dates[i-1] + timedelta(days=1),
                pd_dates[i]
            )
        )

    time_slices.append(slice(pd_dates[-1] + timedelta(days=1), end_date))

    logger.info(f'Will process {len(time_slices)} date ranges')

    slice_datasets = []

    with TemporaryDirectory(prefix='clim-', ignore_cleanup_errors=True) as td:
        for s in time_slices:
            sd = s.start.strftime(ISO_8601_D)

            subset = input_ds.sel(time=s)
            subset = subset.drop_vars(['target_id', 'target_type'], errors='ignore')

            xco2 = subset.xco2.mean(dim='time', skipna=True, keep_attrs=True).expand_dims()
            xco2_uncertainty = subset.xco2_uncertainty.mean(dim='time', skipna=True, keep_attrs=True)
            count: xr.DataArray = subset.xco2.count(dim='time')
            count = count.astype('int')

            slice_time = np.array([s.start], dtype='<M8[ns]')

            slice_ds = xr.Dataset(
                data_vars=dict(
                    xco2=xco2,
                    xco2_uncertainty=xco2_uncertainty,
                    valid_count=count
                ),
                coords=dict(
                    longitude=subset.coords['longitude'],
                    latitude=subset.coords['latitude'],
                )
            )

            slice_ds = slice_ds.expand_dims({"time": 1}).assign_coords({"time": slice_time})

            n_days = xr.DataArray(
                data=np.array([len(subset.time)]),
                dims=['time'],
            ).assign_coords({"time": slice_time})

            slice_ds['n_days'] = n_days

            compressor = zarr.Blosc(cname='blosclz', clevel=9)

            encoding = {var: {'compressor': compressor} for var in slice_ds.data_vars}
            encoding['valid_count']['_FillValue'] = 0

            slice_ds['time'] = slice_ds['time'].chunk(TIME_CHUNKING)

            if XARRAY_8016:
                kwargs = dict(write_empty_chunks=False)
            else:
                kwargs = {}

                for var in encoding:
                    encoding[var]['write_empty_chunks'] = False

            tmp_path = os.path.join(td, f'{sd}.zarr')

            logger.info(f'Writing slice to temp Zarr group at {tmp_path}')

            with ProgressBar():
                slice_ds.to_zarr(
                    tmp_path,
                    mode='w',
                    encoding=encoding,
                    consolidated=True,
                    **kwargs
                )

            slice_datasets.append(xr.open_zarr(tmp_path))

        if args.span == 'monthly-consolidated':
            logger.info('Consolidating months')

            consolidated_slices = []
            months = {}

            for i in range(1, 13):
                months[i] = []

            for s in slice_datasets:
                slice_time = datetime.utcfromtimestamp(
                    (s.time[0].to_numpy() - NP_EPOCH) / np.timedelta64(1, 's')
                )

                months[slice_time.month].append(s)

            for month in months:
                slices = months[month]

                if len(slices) == 0:
                    continue

                dt = datetime(1970, month, 1, 0, 0, 0)
                dt = np.array([dt], dtype='<M8[ns]')

                slices = xr.concat(slices, dim='time')

                xco2 = slices.xco2.mean(dim='time', skipna=True, keep_attrs=True).expand_dims()
                xco2_uncertainty = slices.xco2_uncertainty.mean(dim='time', skipna=True, keep_attrs=True)
                count = slices.valid_count.sum(dim='time').astype('int')
                n_days = slices.n_days.sum(dim='time')

                month_ds = xr.Dataset(
                    data_vars=dict(
                        xco2=xco2,
                        xco2_uncertainty=xco2_uncertainty,
                        valid_count=count,
                    ),
                    coords=dict(
                        longitude=slices.coords['longitude'],
                        latitude=slices.coords['latitude'],
                    )
                ).expand_dims({"time": 1}).assign_coords({"time": dt})

                month_ds['n_days'] = n_days

                consolidated_slices.append(month_ds)

            slice_datasets = consolidated_slices

        clim_ds = xr.concat(slice_datasets, dim='time').sortby('time')

        clim_ds.attrs.update(input_ds.attrs)

        if args.span == 'annual':
            comment_avg_string = 'annually'
        elif args.span == 'seasonal':
            comment_avg_string = 'seasonally'
        elif args.span == 'monthly':
            comment_avg_string = 'monthly'
        else:
            comment_avg_string = 'monthly and consolidated to a single year'

        clim_ds.attrs.update(dict(
            coverage_start=start_date.strftime(ISO_8601_DT),
            coverage_end=end_date.strftime(ISO_8601_DT),
            date_created=datetime.utcnow().strftime(ISO_8601_DT),
            source=f'Produced from averaging the {input_ds.attrs["title"]} dataset, itself '
                   f'd{input_ds.attrs["comment"][1:]}',
            title=f'{input_ds.attrs["title"]}_clim',
            comment=f'Gridded CO2 column-average data averaged {comment_avg_string}',
            averaging_span=args.span,
            source_pipeline_version=input_ds.attrs['pipeline_version'],
            climatology_tool_version=VERSION
        ))

        del clim_ds.attrs['date_updated']
        del clim_ds.attrs['pipeline_version']

        clim_ds.valid_count.attrs.update(dict(
            long_name='valid_days_per_pixel',
            comment='Number of valid (non-NaN) days that was used to compute the means of each pixel of the xco2 and '
                    'xco2_uncertainty variables',
            units='number of days'
        ))

        clim_ds.n_days.attrs.update(dict(
            long_name='number_of_source_days',
            comment='Total number of source days per span of climatology data',
            units='number of days'
        ))

        output_ext = args.output.split('.')[-1].rstrip('/')

        if output_ext == 'zarr':
            if output_url.scheme in ['', 'file']:
                store = output_url.path
            else:
                if args.aws_profile:
                    session = S3FSSession(profile=args.aws_profile)
                    s3 = S3FileSystem(
                        False,
                        session=session,
                        client_kwargs=dict(region_name=args.aws_region)
                    )
                else:
                    s3 = S3FileSystem(
                        False,
                        key=args.aws_key,
                        secret=args.aws_secret,
                        client_kwargs=dict(region_name=args.aws_region)
                    )

                store = S3Map(root=args.output, s3=s3, check=False)

            if args.rechunk:
                output_chunk = args.rechunk
            else:
                output_chunk = orig_chunk

            logger.info(f'Setting output chunks {output_chunk}')

            for var in clim_ds.data_vars:
                clim_ds[var] = clim_ds[var].chunk(output_chunk)

            compressor = zarr.Blosc(cname='blosclz', clevel=9)

            encoding = {var: {'compressor': compressor} for var in clim_ds.data_vars}
            encoding['valid_count']['_FillValue'] = 0

            clim_ds['time'] = clim_ds['time'].chunk(TIME_CHUNKING)

            if XARRAY_8016:
                kwargs = dict(write_empty_chunks=False)
            else:
                kwargs = {}

            logger.info(f'Writing Zarr array to {args.output}')

            with ProgressBar():
                clim_ds.to_zarr(
                    store,
                    mode='w',
                    encoding=encoding,
                    consolidated=True,
                    **kwargs
                )

        elif output_ext in ['nc', 'nc4']:
            comp = {"zlib": True, "complevel": 9}
            encoding = {var: comp for var in clim_ds.data_vars}
            encoding['valid_count']['_FillValue'] = 0

            if output_url.scheme in ['', 'file']:
                logger.info(f'Writing NetCDF to {args.output}')

                with ProgressBar():
                    clim_ds.to_netcdf(
                        output_url.path,
                        mode='w',
                        encoding=encoding
                    )
            else:
                with TemporaryDirectory(prefix='clim-s3-', ignore_cleanup_errors=True) as td:
                    filename = os.path.basename(args.output)
                    temp_file = os.path.join(td, filename)

                    logger.info(f'Writing NetCDF to temp file at {temp_file}')

                    with ProgressBar():
                        clim_ds.to_netcdf(
                            temp_file,
                            mode='w',
                            encoding=encoding
                        )

                    logger.info('Uploading file to S3')

                    client = get_boto_session(args).client(
                        's3',
                        aws_access_key_id=args.aws_key,
                        aws_secret_access_key=args.aws_secret,
                        region_name=args.aws_region
                    )

                    bucket = output_url.netloc
                    key = output_url.path

                    # key will likely be of the form '/path/to/root'. Remove leading / and append /.zgroup
                    # since head_object doesn't seem to work for directories. This will also ensure that we
                    # return true iff a zarr array exists @ this path
                    if key[0] == '/':
                        key = key[1:]

                    try:
                        client.upload_file(temp_file, bucket, key)
                    except ClientError as e:
                        r = e.response
                        err_code = r['Error']['Code']

                        logger.error(f'An AWS error occurred: Code={err_code}; Message={r["Error"]["Message"]}')
                        logger.debug(f'Full error message: \n {json.dumps(r, indent=4)}')

                        raise OSError('Failed to upload output file')
        else:
            raise ValueError(f'Unsupported output file type: .{output_ext}')


def parse_args():
    parser = argparse.ArgumentParser()

    def parse_iso(s):
        return datetime.strptime(s, ISO_8601_D)

    parser.add_argument(
        '-i', '--input',
        required=True,
        metavar='URL',
        help='Path or URL to input Zarr dataset. Must either be in S3 or local FS',
        dest='input'
    )

    parser.add_argument(
        '-o', '--output',
        required=True,
        metavar='URL',
        help='Path or URL to output Zarr dataset. Must either be in S3 or local FS. Raises an error if something exists'
             ' here. Use option --overwrite to go around this.',
        dest='output'
    )

    parser.add_argument(
        '--overwrite',
        required=False,
        action='store_true',
        help='Overwrite existing data if something exists at output path',
        dest='overwrite'
    )

    parser.add_argument(
        '-s', '--start-date',
        required=False,
        help='Start of date range from which climatology will be built, if not given or if given date is before the '
             'earliest date of input data, the dataset will begin building from its start date. Arg should be ISO '
             'formatted string',
        dest='start',
        type=parse_iso
    )

    parser.add_argument(
        '-e', '--end-date',
        required=False,
        help='End of date range to which climatology will be built, if not given or if given date is after the '
             'latest date of input data, the dataset will be built to its end dateArg should be ISO '
             'formatted string',
        dest='end',
        type=parse_iso
    )

    parser.add_argument(
        '--span',
        required=False,
        default='monthly',
        choices=['monthly', 'seasonal', 'annual', 'monthly-consolidated'],
        help='The span of time to average out for each step of climatology. \'monthly-consolidated\' will consolidate '
             'each month\'s data and map them to 1970-mm-01.',
        dest='span'
    )

    parser.add_argument(
        '--rechunk',
        nargs=3,
        required=False,
        dest='rechunk',
        type=int,
        help='Chunk shape for output dataset in order: time longitude latitude',
        metavar=('TIME', 'LON', 'LAT')
    )

    aws = parser.add_argument_group('AWS options')

    aws.add_argument(
        '--key',
        required=False,
        dest='aws_key',
        help='AWS Access Key ID',
    )

    aws.add_argument(
        '--secret',
        required=False,
        dest='aws_secret',
        help='AWS Secret Access Key',
    )

    aws.add_argument(
        '--profile',
        required=False,
        dest='aws_profile',
        help='AWS profile',
    )

    aws.add_argument(
        '--region',
        required=False,
        dest='aws_region',
        help='AWS region',
        default='us-west-2'
    )

    args = parser.parse_args()

    if args.rechunk:
        args.rechunk = tuple(args.rechunk)

    if args.start and args.end:
        if args.start == args.end:
            raise ValueError('Start and end times must be different')
        if args.end < args.start:
            raise ValueError('End time parameter must not be before start time')

    aws = urlparse(args.input).scheme == 's3' or urlparse(args.output).scheme == 's3'

    if aws and not ((args.aws_key and args.aws_secret) or args.aws_profile):
        raise ValueError('AWS credentials MUST be provided to read to or write from S3')

    return args


if __name__ == '__main__':
    logger.info(f'Using max concurrency: {MAX_CONCURRENCY}')

    start_time = datetime.now()

    dask.config.set(
        pool=ThreadPoolExecutor(
            max_workers=MAX_CONCURRENCY,
            thread_name_prefix='dask-worker'
        )
    )

    try:
        main()
        logger.info('Script completed successfully')
    except Exception as e:
        logger.exception(e)
        logger.info('Script completed unsuccessfully')
    except KeyboardInterrupt:
        logger.info('Script was cancelled before it could finish')
    finally:
        logger.info(f'Script exited after {datetime.now() - start_time}')
