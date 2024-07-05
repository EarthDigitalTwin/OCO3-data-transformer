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
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import box
from tqdm import tqdm

OCO3_FILENAME_PATTERN = re.compile(r'oco3_LtCO2_\d{6}_B\d+[a-zA-Z]*_\d{12}s\.nc4')
MISSING = ['missing', 'Missing']
EXCLUDE_IDS_DEFAULT = ['fossil0240']

# Two target IDs that seem to have been set initially with the longitude mistakenly east instead of west.
# Currently, the exclusion criterion for these two targets is iff lons > 0. If this changes from a new target
# being incorrectly entered, the logic will have to be updated, but it can stay really simple for now
SPECIAL_CASES = [
    'volcano0036',
    'val001'
]

OPERATION_MODE_SAM = 4
OPERATION_MODE_TARGET = 2


def sam_vertices_to_bounds(sam_vertices_lat: xr.DataArray, sam_vertices_lon: xr.DataArray) -> tuple:
    lats = sam_vertices_lat.to_numpy().flatten()
    lons = sam_vertices_lon.to_numpy().flatten()

    return np.min(lons), np.min(lats), np.max(lons), np.max(lats)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-d', '--dir',
        dest='input_dir',
        required=True,
        help='Directory containing input files to process'
    )

    parser.add_argument(
        '-o', '--output',
        dest='output',
        default=os.path.join(os.getcwd(), 'oco3-sam-bounding-boxes.csv'),
        help='Output file path. Defaults to oco3-sam-bounding-boxes.csv in the current working directory.'
    )

    parser.add_argument(
        '--overwrite',
        action='store_true',
        dest='overwrite',
        help='Overwrite output file if it exists.'
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

    if not args.overwrite and os.path.exists(excluded_outfile):
        raise FileExistsError(f'Output file {excluded_outfile} already exists')

    Path(os.path.dirname(outfile)).mkdir(parents=True, exist_ok=True)

    inputs = [f.path for f in os.scandir(args.input_dir) if re.match(OCO3_FILENAME_PATTERN, f.name)]
    inputs.sort()

    if len(inputs) == 0:
        raise ValueError('No files found in input directory matching OCO-3 Lite file naming pattern')

    dfd = {
        'Granule': [],
        'Target ID': [],
        'Target Name': [],
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
        'Target ID': [],
        'Target Name': [],
        'Region Start Index': [],
        'Region End Index': [],
        'Operation Mode': [],
        'min_lon': [],
        'min_lat': [],
        'max_lon': [],
        'max_lat': [],
        'Bounding Box WKT': [],
        'Reason for Exclusion': [],
    }

    for granule in tqdm(inputs):
        ds_main = xr.open_dataset(granule, mask_and_scale=False, engine='h5netcdf').load()
        ds_sounding = xr.open_dataset(granule, mask_and_scale=False, engine='h5netcdf', group='Sounding').load()

        mode_array = ds_sounding['operation_mode']
        target_array = ds_sounding['target_id']
        target_name_array = ds_sounding['target_name']

        v_lats_array = ds_main['vertex_latitude']
        v_lons_array = ds_main['vertex_longitude']

        i = None
        in_region = False
        start = None
        target_id = None
        target_name = None

        for i, (mode, target, name) in enumerate(zip(
                mode_array.to_numpy(),
                target_array.to_numpy(),
                target_name_array.to_numpy()
        )):
            if mode.item() == OPERATION_MODE_SAM:
                if not in_region:
                    in_region = True
                    target_id = target
                    target_name = name
                    start = i
                elif target != target_id:
                    min_lon, min_lat, max_lon, max_lat = sam_vertices_to_bounds(
                        v_lats_array.isel(sounding_id=slice(start, i)),
                        v_lons_array.isel(sounding_id=slice(start, i))
                    )

                    if target_id in MISSING or target_id in EXCLUDE_IDS_DEFAULT:
                        add_dfd = dfd_excluded
                        reason = 'Target fully excluded'
                    elif target_id in SPECIAL_CASES and min_lon > 0:
                        add_dfd = dfd_excluded
                        reason = 'Incorrectly configured SAM capture. Actual lon = desired lon * -1'
                    else:
                        add_dfd = dfd
                        reason = None

                    add_dfd['Granule'].append(os.path.basename(granule))
                    add_dfd['Target ID'].append(target_id)
                    add_dfd['Target Name'].append(target_name)
                    add_dfd['Region Start Index'].append(start)
                    add_dfd['Region End Index'].append(i)
                    add_dfd['Operation Mode'].append(OPERATION_MODE_SAM)
                    add_dfd['min_lon'].append(min_lon)
                    add_dfd['min_lat'].append(min_lat)
                    add_dfd['max_lon'].append(max_lon)
                    add_dfd['max_lat'].append(max_lat)
                    add_dfd['Bounding Box WKT'].append(
                        box(min_lon, min_lat, max_lon, max_lat).wkt
                    )

                    if reason is not None:
                        add_dfd['Reason for Exclusion'] .append(reason)

                    target_id = target
                    target_name = name
                    start = i

            if mode.item() != OPERATION_MODE_SAM:
                if in_region:
                    min_lon, min_lat, max_lon, max_lat = sam_vertices_to_bounds(
                        v_lats_array.isel(sounding_id=slice(start, i)),
                        v_lons_array.isel(sounding_id=slice(start, i))
                    )

                    if target_id in MISSING or target_id in EXCLUDE_IDS_DEFAULT:
                        add_dfd = dfd_excluded
                        reason = 'Target fully excluded'
                    elif target_id in SPECIAL_CASES and min_lon > 0:
                        add_dfd = dfd_excluded
                        reason = 'Incorrectly configured SAM capture. Actual lon = desired lon * -1'
                    else:
                        add_dfd = dfd
                        reason = None

                    add_dfd['Granule'].append(os.path.basename(granule))
                    add_dfd['Target ID'].append(target_id)
                    add_dfd['Target Name'].append(target_name)
                    add_dfd['Region Start Index'].append(start)
                    add_dfd['Region End Index'].append(i)
                    add_dfd['Operation Mode'].append(OPERATION_MODE_SAM)
                    add_dfd['min_lon'].append(min_lon)
                    add_dfd['min_lat'].append(min_lat)
                    add_dfd['max_lon'].append(max_lon)
                    add_dfd['max_lat'].append(max_lat)
                    add_dfd['Bounding Box WKT'].append(
                        box(min_lon, min_lat, max_lon, max_lat).wkt
                    )

                    if reason is not None:
                        add_dfd['Reason for Exclusion'] .append(reason)

                    target_id = None
                    target_name = None
                    in_region = False
                    reason = None

        if in_region:
            min_lon, min_lat, max_lon, max_lat = sam_vertices_to_bounds(
                v_lats_array.isel(sounding_id=slice(start, i+1)),
                v_lons_array.isel(sounding_id=slice(start, i+1))
            )

            if target_id in MISSING or target_id in EXCLUDE_IDS_DEFAULT:
                add_dfd = dfd_excluded
                reason = 'Target fully excluded'
            elif target_id in SPECIAL_CASES and min_lon > 0:
                add_dfd = dfd_excluded
                reason = 'Incorrectly configured SAM capture. Actual lon = desired lon * -1'
            else:
                add_dfd = dfd
                reason = None

            add_dfd['Granule'].append(os.path.basename(granule))
            add_dfd['Target ID'].append(target_id)
            add_dfd['Target Name'].append(target_name)
            add_dfd['Region Start Index'].append(start)
            add_dfd['Region End Index'].append(i+1)
            add_dfd['Operation Mode'].append(OPERATION_MODE_SAM)
            add_dfd['min_lon'].append(min_lon)
            add_dfd['min_lat'].append(min_lat)
            add_dfd['max_lon'].append(max_lon)
            add_dfd['max_lat'].append(max_lat)
            add_dfd['Bounding Box WKT'].append(
                box(min_lon, min_lat, max_lon, max_lat).wkt
            )

            if reason is not None:
                add_dfd['Reason for Exclusion'].append(reason)

        i = None
        in_region = False
        start = None
        target_id = None
        target_name = None

        for i, (mode, target, name) in enumerate(zip(
                mode_array.to_numpy(),
                target_array.to_numpy(),
                target_name_array.to_numpy()
        )):
            if mode.item() == OPERATION_MODE_TARGET:
                if not in_region:
                    in_region = True
                    target_id = target
                    target_name = name
                    start = i
                elif target != target_id:
                    min_lon, min_lat, max_lon, max_lat = sam_vertices_to_bounds(
                        v_lats_array.isel(sounding_id=slice(start, i)),
                        v_lons_array.isel(sounding_id=slice(start, i))
                    )

                    if target_id in MISSING or target_id in EXCLUDE_IDS_DEFAULT:
                        add_dfd = dfd_excluded
                        reason = 'Target fully excluded'
                    elif target_id in SPECIAL_CASES and min_lon > 0:
                        add_dfd = dfd_excluded
                        reason = 'Incorrectly configured SAM capture. Actual lon = desired lon * -1'
                    else:
                        add_dfd = dfd
                        reason = None

                    add_dfd['Granule'].append(os.path.basename(granule))
                    add_dfd['Target ID'].append(target_id)
                    add_dfd['Target Name'].append(target_name)
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

                    if reason is not None:
                        add_dfd['Reason for Exclusion'] .append(reason)

                    target_id = target
                    target_name = name
                    start = i

            if mode.item() != OPERATION_MODE_TARGET:
                if in_region:
                    min_lon, min_lat, max_lon, max_lat = sam_vertices_to_bounds(
                        v_lats_array.isel(sounding_id=slice(start, i)),
                        v_lons_array.isel(sounding_id=slice(start, i))
                    )

                    if target_id in MISSING or target_id in EXCLUDE_IDS_DEFAULT:
                        add_dfd = dfd_excluded
                        reason = 'Target fully excluded'
                    elif target_id in SPECIAL_CASES and min_lon > 0:
                        add_dfd = dfd_excluded
                        reason = 'Incorrectly configured SAM capture. Actual lon = desired lon * -1'
                    else:
                        add_dfd = dfd
                        reason = None

                    add_dfd['Granule'].append(os.path.basename(granule))
                    add_dfd['Target ID'].append(target_id)
                    add_dfd['Target Name'].append(target_name)
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

                    if reason is not None:
                        add_dfd['Reason for Exclusion'] .append(reason)

                    target_id = None
                    target_name = None
                    in_region = False

        if in_region:
            min_lon, min_lat, max_lon, max_lat = sam_vertices_to_bounds(
                v_lats_array.isel(sounding_id=slice(start, i+1)),
                v_lons_array.isel(sounding_id=slice(start, i+1))
            )

            if target_id in MISSING or target_id in EXCLUDE_IDS_DEFAULT:
                add_dfd = dfd_excluded
                reason = 'Target fully excluded'
            elif target_id in SPECIAL_CASES and min_lon > 0:
                add_dfd = dfd_excluded
                reason = 'Incorrectly configured SAM capture. Actual lon = desired lon * -1'
            else:
                add_dfd = dfd
                reason = None

            add_dfd['Granule'].append(os.path.basename(granule))
            add_dfd['Target ID'].append(target_id)
            add_dfd['Target Name'].append(target_name)
            add_dfd['Region Start Index'].append(start)
            add_dfd['Region End Index'].append(i+1)
            add_dfd['Operation Mode'].append(OPERATION_MODE_TARGET)
            add_dfd['min_lon'].append(min_lon)
            add_dfd['min_lat'].append(min_lat)
            add_dfd['max_lon'].append(max_lon)
            add_dfd['max_lat'].append(max_lat)
            add_dfd['Bounding Box WKT'].append(
                box(min_lon, min_lat, max_lon, max_lat).wkt
            )

            if reason is not None:
                add_dfd['Reason for Exclusion'] .append(reason)

    df = pd.DataFrame(dfd)
    df_excluded = pd.DataFrame(dfd_excluded)

    df.to_csv(outfile, index=False)
    df_excluded.to_csv(excluded_outfile, index=False)


if __name__ == '__main__':
    main()
