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
from pathlib import Path

import pandas as pd
import xarray as xr
from shapely.geometry import Polygon, box
from tqdm import tqdm

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
        help='Path to file containing SAM and Target captures (generated by granulesToTargets.py)'
    )

    parser.add_argument(
        '-t', '--targets',
        dest='target_file',
        required=True,
        help='Target bbox JSON file (generated by targetsToJson.py)'
    )

    parser.add_argument(
        '-o', '--output',
        dest='output',
        default=os.path.join(os.getcwd(), 'oco2-target-stats.xlsx'),
        help='Output file path. Defaults to oco2-target-stats.xlsx in the current working directory. If a .csv '
             'extension is given, a CSV file will be produced.'
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

    if not os.path.isfile(args.extracted_targets):
        raise FileNotFoundError('Extracted data file does not exist')

    if not os.path.isfile(args.target_file):
        raise FileNotFoundError('Target file does not exist')

    outfile: str = args.output

    if not (outfile.endswith('.xlsx') or outfile.endswith('.csv')):
        outfile = outfile + '.xlsx'

    if not args.overwrite and os.path.exists(outfile):
        raise FileExistsError(f'Output file {outfile} already exists')

    Path(os.path.dirname(outfile)).mkdir(parents=True, exist_ok=True)

    inputs = [f.path for f in os.scandir(args.input_dir) if re.match(OCO2_FILENAME_PATTERN, f.name)]
    inputs.sort()

    if len(inputs) == 0:
        raise ValueError('No files found in input directory matching OCO-2 Lite file naming pattern')

    extracted_data_df = pd.read_csv(args.extracted_targets)[['Granule', 'Inferred Target ID', 'Inferred Target Name',
                                                             'Region Start Index', 'Region End Index',
                                                             'Operation Mode']].rename(
        {
            'Inferred Target ID': 'tid',
            'Inferred Target Name': 'tn',
            'Region Start Index': 'start_i',
            'Region End Index': 'end_i',
            'Operation Mode': 'mode'
        },
        axis='columns'
    )

    with open(args.target_file) as fp:
        target_dict = json.load(fp)

    df_data = {
        'Granule': [],
        'Target ID': [],
        'Target Name': [],
        'Region Start Index': [],
        'Region End Index': [],
        'Operation Mode': [],
        'Soundings': [],
        'Good Soundings': [],
        'Soundings in Bounds': [],
        'Soundings on Bounds': [],
        'Soundings out of Bounds': [],
        'Good Soundings in Bounds': [],
        'Good Soundings on Bounds': [],
        'Good Soundings out of Bounds': [],
    }

    for granule in tqdm(inputs):
        granule_sams = extracted_data_df.loc[extracted_data_df['Granule'] == os.path.basename(granule)]

        ds = xr.open_dataset(granule, mask_and_scale=False, engine='h5netcdf').load()

        for row in tqdm(granule_sams.itertuples(), total=len(granule_sams), leave=False):
            target_id = row.tid
            target_name = row.tn
            start_i = row.start_i
            end_i = row.end_i
            mode = row.mode

            if target_id not in target_dict:
                tqdm.write(f'Skipping sam for target ID {target_id} because it\'s not in the targets file')
                continue

            target_bbox = box(
                target_dict[target_id]['bbox']['min_lon'],
                target_dict[target_id]['bbox']['min_lat'],
                target_dict[target_id]['bbox']['max_lon'],
                target_dict[target_id]['bbox']['max_lat']
            )

            subset = ds.isel(sounding_id=slice(start_i, end_i))

            n_soundings = len(subset.sounding_id)
            n_soundings_in = 0
            n_soundings_on = 0
            n_soundings_out = 0

            n_good_soundings = 0
            n_good_soundings_in = 0
            n_good_soundings_on = 0
            n_good_soundings_out = 0

            for lats, lons, qf in zip(
                    subset.vertex_latitude.values,
                    subset.vertex_longitude.values,
                    subset.xco2_quality_flag.values,
            ):
                vertices = [(lons[i].item(), lats[i].item()) for i in range(len(lats))]
                vertices.append((lons[0].item(), lats[0].item()))
                p = Polygon(vertices)

                good = qf == 0

                if good:
                    n_good_soundings += 1

                if target_bbox.contains(p):
                    n_soundings_in += 1
                    if good:
                        n_good_soundings_in += 1
                elif target_bbox.intersects(p):
                    n_soundings_on += 1
                    if good:
                        n_good_soundings_on += 1
                else:
                    n_soundings_out += 1
                    if good:
                        n_good_soundings_out += 1

            df_data['Granule'].append(os.path.basename(granule))
            df_data['Target ID'].append(target_id)
            df_data['Target Name'].append(target_name)
            df_data['Region Start Index'].append(start_i)
            df_data['Region End Index'].append(end_i)
            df_data['Operation Mode'].append(mode)
            df_data['Soundings'].append(n_soundings)
            df_data['Good Soundings'].append(n_good_soundings)
            df_data['Soundings in Bounds'].append(n_soundings_in)
            df_data['Soundings on Bounds'].append(n_soundings_on)
            df_data['Soundings out of Bounds'].append(n_soundings_out)
            df_data['Good Soundings in Bounds'].append(n_good_soundings_in)
            df_data['Good Soundings on Bounds'].append(n_good_soundings_on)
            df_data['Good Soundings out of Bounds'].append(n_good_soundings_out)

    df = pd.DataFrame(df_data)

    # If user specified Excel output, aggregate the data in certain levels
    if outfile.endswith('.xlsx'):
        tid_agg_df = df.replace(to_replace={'Target Name': 'none'}, value=None).groupby('Target ID').agg({
            'Target Name': 'first',
            'Soundings': 'sum',
            'Good Soundings': 'sum',
            'Soundings in Bounds': 'sum',
            'Soundings on Bounds': 'sum',
            'Soundings out of Bounds': 'sum',
            'Good Soundings in Bounds': 'sum',
            'Good Soundings on Bounds': 'sum',
            'Good Soundings out of Bounds': 'sum'
        })

        tid_agg_df.loc['all'] = tid_agg_df.sum()
        tid_agg_df.loc['all', 'Target Name'] = "Sum of all targets"
        tid_agg_df = tid_agg_df.reset_index(['Target ID'])

        targets_df_dict = {
            'Target ID': [],
            'Target Name': [],
            'Centroid WKT': [],
            'min_lon': [],
            'min_lat': [],
            'max_lon': [],
            'max_lat': [],
            'Bounding Box WKT': [],
        }

        for tid in target_dict:
            targets_df_dict['Target ID'].append(tid)
            targets_df_dict['Target Name'].append(target_dict[tid]['name'])
            targets_df_dict['Centroid WKT'].append(target_dict[tid]['centroid_wkt'])
            bbox = target_dict[tid]['bbox']
            targets_df_dict['min_lon'].append(bbox['min_lon'])
            targets_df_dict['min_lat'].append(bbox['min_lat'])
            targets_df_dict['max_lon'].append(bbox['max_lon'])
            targets_df_dict['max_lat'].append(bbox['max_lat'])
            targets_df_dict['Bounding Box WKT'].append(box(bbox['min_lon'], bbox['min_lat'],
                                                           bbox['max_lon'], bbox['max_lat']).wkt)

        targets_df = pd.DataFrame(targets_df_dict)

        dfs = {
            'Individual Sam Stats': df,
            'Per-Target ID & Total Stats': tid_agg_df,
            'Target Bounding Box Definitions': targets_df
        }

        with pd.ExcelWriter(outfile, engine='xlsxwriter') as writer:
            from xlsxwriter.worksheet import Worksheet
            from xlsxwriter.workbook import Workbook

            workbook: Workbook = writer.book

            number_format = workbook.add_format({
                'num_format': '#,##0'
            })

            highlight_format = workbook.add_format()
            highlight_format.set_pattern(1)
            highlight_format.set_bg_color('yellow')

            for sheet, df in dfs.items():
                df.to_excel(writer, sheet_name=sheet, index=False, freeze_panes=(1, 0))
                worksheet: Worksheet = writer.sheets[sheet]
                worksheet.set_zoom(150)

                for i, col in enumerate(df):
                    series = df[col]
                    col_width = max((
                        series.astype(str).map(len).max(),
                        len(str(series.name))
                    )) + 2

                    if sheet != 'Target Bounding Box Definitions' and series.name not in [
                        'Granule', 'Target ID', 'Target Name', 'Target Type'
                    ]:
                        worksheet.set_column(i, i, col_width, number_format)
                    else:
                        worksheet.set_column(i, i, col_width)

                if sheet == 'Per-Target ID & Total Stats':
                    for i, tid in enumerate(dfs[sheet]['Target ID'], 1):
                        if tid == 'all':
                            worksheet.write_string(i, 0, tid, highlight_format)
    else:
        df.to_csv(outfile, index=False)


if __name__ == '__main__':
    main()
