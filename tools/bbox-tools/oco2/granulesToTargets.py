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
import os
import re
from math import sqrt
from pathlib import Path
from typing import Tuple, List

import numpy as np
import pandas as pd
import xarray as xr
from geopy.distance import geodesic
from shapely import from_wkt
from shapely.geometry import box, Point
from tqdm import tqdm

OCO2_FILENAME_PATTERN = re.compile(r'oco2_LtCO2_\d{6}_B\d+[a-zA-Z]*_\d{12}s\.nc4')
OPERATION_MODE_TARGET = 2

TARGET_INFERENCE_METHODS = {
    'all_deg': 'Compute degree distance for all points and choose most common minimum (slow)',
    'all_km': 'Compute geodesic distance for all points and choose most common minimum (very very slow)',
    'cent_deg': 'Compute degree distance for centroid of points and choose minimum',
    'cent_km': 'Compute geodesic distance for centroid of points and choose minimum'
}


def target_vertices_to_bounds(target_vertices_lat: xr.DataArray, target_vertices_lon: xr.DataArray) -> tuple:
    lats = target_vertices_lat.to_numpy().flatten()
    lons = target_vertices_lon.to_numpy().flatten()

    return np.min(lons), np.min(lats), np.max(lons), np.max(lats)


def closest_target_deg(
        targets: List[Tuple[str, str, Point]],
        lat: float,
        lon: float,
        outlier: float | None
) -> Tuple[str | None, str | None]:
    tid, tn, point = targets[0]

    min_diff = sqrt((lat - point.y) ** 2 + (lon - point.x) ** 2)
    min_target = (tid, tn)

    for tid, tn, point in targets[1:]:
        diff = sqrt((lat - point.y) ** 2 + (lon - point.x) ** 2)

        if diff < min_diff:
            min_diff = diff
            min_target = (tid, tn)

    if outlier is not None and min_diff > outlier:
        return None, None

    return min_target


def closest_target_km(
        targets: List[Tuple[str, str, Point]],
        lat: float,
        lon: float,
        outlier: float | None
) -> Tuple[str | None, str | None]:
    tid, tn, point = targets[0]

    min_diff = geodesic((lat, lon), (point.y, point.x)).km
    min_target = (tid, tn)

    for tid, tn, point in targets[1:]:
        diff = geodesic((lat, lon), (point.y, point.x)).km

        if diff < min_diff:
            min_diff = diff
            min_target = (tid, tn)

    if outlier is not None and min_diff > outlier:
        return None, None

    return min_target


def infer_target(
        targets: List[Tuple[str, str, Point]],
        method: str,
        lats: xr.DataArray, lons: xr.DataArray,
        outlier: float | None
) -> Tuple[str | None, str | None]:

    if method == 'all_deg':
        counts = {}

        for lat, lon in zip(lats.to_numpy(), lons.to_numpy()):
            nearest = closest_target_deg(targets, lat, lon, outlier)

            if nearest[0] is not None:
                if nearest not in counts:
                    counts[nearest] = 1
                else:
                    counts[nearest] += 1

        if len(counts) > 0:
            return sorted(list(counts.items()), key=lambda x: x[1], reverse=True)[0][0]
        else:
            return None, None
    elif method == 'all_km':
        counts = {}

        for lat, lon in zip(lats.to_numpy(), lons.to_numpy()):
            nearest = closest_target_km(targets, lat, lon, outlier)

            if nearest[0] is not None:
                if nearest not in counts:
                    counts[nearest] = 1
                else:
                    counts[nearest] += 1

        if len(counts) > 0:
            return sorted(list(counts.items()), key=lambda x: x[1], reverse=True)[0][0]
        else:
            return None, None
    elif method == 'cent_deg':
        cent_lat = lats.mean().item()
        cent_lon = lons.mean().item()

        return closest_target_deg(targets, cent_lat, cent_lon, outlier)
    elif method == 'cent_km':
        cent_lat = lats.mean().item()
        cent_lon = lons.mean().item()

        return closest_target_km(targets, cent_lat, cent_lon, outlier)
    else:
        raise ValueError(f'Unknown target inference method: {method}')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-d', '--dir',
        dest='input_dir',
        required=True,
        help='Directory containing input files to process'
    )

    parser.add_argument(
        '-t', '--targets',
        dest='targets',
        required=True,
        help='Path to target file as provided in oco2-targets-pub.csv'
    )

    parser.add_argument(
        '-m', '--method',
        dest='method',
        default='cent_km',
        choices=TARGET_INFERENCE_METHODS.keys(),
        help=f'Method to infer the target for each region of target data. Default: all_km . Accepted values: '
             f'{" ".join([f"{m}: {d}" for m, d in TARGET_INFERENCE_METHODS.items()])}'
    )

    parser.add_argument(
        '-o', '--output',
        dest='output',
        default=os.path.join(os.getcwd(), 'oco2-target-bounding-boxes.csv'),
        help='Output file path. Defaults to oco2-target-bounding-boxes.csv in the current working directory.'
    )

    parser.add_argument(
        '--overwrite',
        action='store_true',
        dest='overwrite',
        help='Overwrite output file if it exists.'
    )

    def positive_float_or_none(value):
        if value is None:
            return None

        try:
            val = float(value)
        except:
            raise ValueError('Could not parse to float')

        if val <= 0.0:
            raise ValueError(f'Value {value} must be greater than zero')

        return val

    parser.add_argument(
        '--outlier',
        required=False,
        default=None,
        type=positive_float_or_none,
        dest='outlier',
        help='Maximum distance from target center (based on evaluation defined by -m) over which a region will be '
             'excluded. Omit to include all regions.'
    )

    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        raise FileNotFoundError('Input directory does not exist')

    outfile: str = args.output

    if not outfile.endswith('.csv'):
        outfile = outfile + '.csv'

    excluded_outfile = outfile[:-4] + '.excluded.csv'

    if not args.overwrite and os.path.exists(outfile):
        raise FileExistsError(f'Output file {outfile} already exists')

    if args.outlier is not None and (not args.overwrite and os.path.exists(excluded_outfile)):
        raise FileExistsError(f'Output file {excluded_outfile} already exists')

    Path(os.path.dirname(outfile)).mkdir(parents=True, exist_ok=True)

    inputs = [f.path for f in os.scandir(args.input_dir) if re.match(OCO2_FILENAME_PATTERN, f.name)]
    inputs.sort()

    if len(inputs) == 0:
        raise ValueError('No files found in input directory matching OCO-2 Lite file naming pattern')

    if not os.path.isfile(args.targets):
        raise FileNotFoundError(f'Target file {args.targets} does not exist')

    target_df = pd.read_csv(args.targets)

    targets: List[Tuple[str, str, Point]] = []

    for row in target_df.itertuples(index=False):
        targets.append((row[0], row[1], from_wkt(row[2])))

    dfd = {
        'Granule': [],
        'Inferred Target ID': [],
        'Inferred Target Name': [],
        'Region Start Index': [],
        'Region End Index': [],
        'Operation Mode': [],
        'min_lon': [],
        'min_lat': [],
        'max_lon': [],
        'max_lat': [],
        'Bounding Box WKT': [],
    }

    dfd_excluded = {
        'Granule': [],
        'Inferred Target ID': [],
        'Inferred Target Name': [],
        'Region Start Index': [],
        'Region End Index': [],
        'Operation Mode': [],
        'min_lon': [],
        'min_lat': [],
        'max_lon': [],
        'max_lat': [],
        'Bounding Box WKT': [],
    }

    for granule in tqdm(inputs):
        ds_main = xr.open_dataset(granule, mask_and_scale=False, engine='h5netcdf').load()
        ds_sounding = xr.open_dataset(granule, mask_and_scale=False, engine='h5netcdf', group='Sounding').load()

        mode_array = ds_sounding['operation_mode']

        v_lats_array = ds_main['vertex_latitude']
        v_lons_array = ds_main['vertex_longitude']

        lats_array = ds_main['latitude']
        lons_array = ds_main['longitude']

        i = None
        in_region = False
        start = None

        for i, mode in enumerate(mode_array.to_numpy()):
            if mode.item() == OPERATION_MODE_TARGET:
                if not in_region:
                    in_region = True
                    start = i

            if mode.item() != OPERATION_MODE_TARGET:
                if in_region:
                    region_lats = lats_array.isel(sounding_id=slice(start, i))
                    region_lons = lons_array.isel(sounding_id=slice(start, i))

                    region_vlats = v_lats_array.isel(sounding_id=slice(start, i))
                    region_vlons = v_lons_array.isel(sounding_id=slice(start, i))

                    min_lon, min_lat, max_lon, max_lat = target_vertices_to_bounds(region_vlats, region_vlons)

                    tid, tn = infer_target(targets, args.method, region_lats, region_lons, args.outlier)

                    add_dfd = dfd if tid is not None else dfd_excluded

                    add_dfd['Granule'].append(os.path.basename(granule))
                    add_dfd['Inferred Target ID'].append(tid)
                    add_dfd['Inferred Target Name'].append(tn)
                    add_dfd['Region Start Index'].append(start)
                    add_dfd['Region End Index'].append(i)
                    add_dfd['Operation Mode'].append(OPERATION_MODE_TARGET)
                    add_dfd['min_lon'].append(min_lon)
                    add_dfd['min_lat'].append(min_lat)
                    add_dfd['max_lon'].append(max_lon)
                    add_dfd['max_lat'].append(max_lat)
                    add_dfd['Bounding Box WKT'].append(
                        box(min_lon, min_lat, max_lon, max_lat).wkt
                    )

                    in_region = False

        if in_region:
            region_lats = lats_array.isel(sounding_id=slice(start, i))
            region_lons = lons_array.isel(sounding_id=slice(start, i))

            region_vlats = v_lats_array.isel(sounding_id=slice(start, i))
            region_vlons = v_lons_array.isel(sounding_id=slice(start, i))

            min_lon, min_lat, max_lon, max_lat = target_vertices_to_bounds(region_vlats, region_vlons)

            tid, tn = infer_target(targets, args.method, region_lats, region_lons, args.outlier)

            add_dfd = dfd if tid is not None else dfd_excluded

            add_dfd['Granule'].append(os.path.basename(granule))
            add_dfd['Inferred Target ID'].append(tid)
            add_dfd['Inferred Target Name'].append(tn)
            add_dfd['Region Start Index'].append(start)
            add_dfd['Region End Index'].append(i)
            add_dfd['Operation Mode'].append(OPERATION_MODE_TARGET)
            add_dfd['min_lon'].append(min_lon)
            add_dfd['min_lat'].append(min_lat)
            add_dfd['max_lon'].append(max_lon)
            add_dfd['max_lat'].append(max_lat)
            add_dfd['Bounding Box WKT'].append(
                box(min_lon, min_lat, max_lon, max_lat).wkt
            )

    df = pd.DataFrame(dfd)
    df_excluded = pd.DataFrame(dfd_excluded)

    df.to_csv(outfile, index=False)
    df_excluded.to_csv(excluded_outfile, index=False)


if __name__ == '__main__':
    main()
