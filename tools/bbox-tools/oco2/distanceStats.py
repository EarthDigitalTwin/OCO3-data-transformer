# Copyright 2025 California Institute of Technology (Caltech)
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
import os
import re
from math import sqrt
from pathlib import Path

import pandas as pd
import xarray as xr
from geopy.distance import geodesic
from shapely import wkt
from tqdm import tqdm

OPERATION_MODE_TARGET = 2
OCO2_FILENAME_PATTERN = re.compile(r'oco2_LtCO2_\d{6}_B\d+[a-zA-Z]*_\d{12}s\.nc4')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-d', '--dir',
        dest='input_dir',
        required=True,
        help='Directory containing input files to process'
    )

    parser.add_argument(
        '-e', '--extracted-data',
        dest='extracted_targets',
        required=True,
        help='Path to file containing Target captures (generated by granulesToTargets.py)'
    )

    parser.add_argument(
        '-t', '--targets',
        dest='target_file',
        required=True,
        help='Path to file containing target definitions. (provided in oco2-targets-pub.csv)'
    )

    parser.add_argument(
        '-o', '--output',
        dest='output',
        default=os.path.join(os.getcwd(), 'oco2-target-distances.json'),
        help='Output file path. Defaults to oco2-target-distances.json in the current working directory.'
    )

    parser.add_argument(
        '--overwrite',
        action='store_true',
        dest='overwrite',
        help='Overwrite output file if it exists.'
    )

    parser.add_argument(
        '--diff',
        dest='diff_method',
        choices=['abs', 'latlon'],
        default='latlon',
        help='Method to compute differences between sounding and target centroid. latlon (default) is the max abs '
             'difference in lat and lon (max(|lon_c - lon_s|, |lat_c - lat_s|). abs is the absolute distance between '
             'the centroids.'
    )

    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        raise FileNotFoundError('Input directory does not exist')

    if not os.path.isfile(args.extracted_targets):
        raise FileNotFoundError('Extracted data file does not exist')

    if not os.path.isfile(args.target_file):
        raise FileNotFoundError('Target file does not exist')

    outfile: str = args.output

    if not outfile.endswith('.json'):
        outfile = outfile + '.json'

    if not args.overwrite and os.path.exists(outfile):
        raise FileExistsError(f'Output file {outfile} already exists')

    Path(os.path.dirname(outfile)).mkdir(parents=True, exist_ok=True)

    inputs = [f.path for f in os.scandir(args.input_dir) if re.match(OCO2_FILENAME_PATTERN, f.name)]
    inputs.sort()

    if len(inputs) == 0:
        raise ValueError('No files found in input directory matching OCO-2 Lite file naming pattern')

    df = pd.read_csv(args.extracted_targets)[['Granule', 'Inferred Target ID', 'Region Start Index',
                                              'Region End Index', 'Operation Mode']].rename(
        {
            'Inferred Target ID': 'tid',
            'Region Start Index': 'start_i',
            'Region End Index': 'end_i',
            'Operation Mode': 'mode'
        },
        axis='columns'
    )

    target_df = pd.read_csv(args.target_file, index_col='Target_ID')[['Center_WKT']].rename(
        {
            'Center_WKT': 'point',
        },
        axis='columns'
    )

    stats = {}

    for granule in tqdm(inputs):
        granule_sams = df.loc[df['Granule'] == os.path.basename(granule)]

        ds = xr.open_dataset(granule, mask_and_scale=False, engine='h5netcdf').load()

        for row in granule_sams.itertuples():
            target_id = row.tid
            start_i = row.start_i
            end_i = row.end_i

            if target_id not in target_df.index:
                tqdm.write(f'Skipping sam for target ID {target_id} because it\'s not in the targets file')
                continue

            center_wkt = target_df.loc[target_id].values[0]
            center = wkt.loads(center_wkt)
            center_lat, center_lon = center.y, center.x

            stats.setdefault(target_id, dict(target_mode_km=[], target_mode_deg=[]))

            km_key = 'target_mode_km'
            deg_key = 'target_mode_deg'

            subset = ds.isel(sounding_id=slice(start_i, end_i))

            for lat, lon in zip(subset.latitude.values, subset.longitude.values):
                if args.diff_method == 'abs':
                    diff_deg = sqrt(((center_lat - lat) ** 2) + ((center_lon - lon) ** 2))
                    diff_km = geodesic((lat, lon), (center_lat, center_lon)).km
                else:
                    diff_deg = max(abs(center_lat - lat), abs(center_lon - lon))
                    diff_km = max(
                        geodesic((lat, lon), (center_lat, lon)).km,
                        geodesic((lat, lon), (lat, center_lon)).km,
                    )

                stats[target_id][km_key].append(float(diff_km))
                stats[target_id][deg_key].append(float(diff_deg))

    with open(outfile, 'w') as fp:
        json.dump(stats, fp, sort_keys=True)


if __name__ == '__main__':
    main()
