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


import pandas as pd
import argparse
from shapely.geometry import box
import os
from pathlib import Path

try:
    import geopandas
    has_gp = True
except ImportError:
    has_gp = False


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-i', '--input',
        dest='input',
        required=True,
        help='Input .csv file. Should be the output of the granulesToTargets.py script'
    )

    parser.add_argument(
        '-o', '--output',
        dest='output',
        default=os.path.join(os.getcwd(), 'oco2-targets-aggregated-bounding-boxes.csv'),
        help='Output file path. Defaults to oco2-targets-aggregated-bounding-boxes.csv in the current working '
             'directory.'
    )

    parser.add_argument(
        '--overwrite',
        action='store_true',
        dest='overwrite',
        help='Overwrite output file if it exists.'
    )

    parser.add_argument(
        '--no-geo',
        action='store_false',
        dest='geo',
        help='Do not try to produce a GeoPackage of the computed bounding boxes'
    )

    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError('Input file does not exist')

    outfile: str = args.output

    if not outfile.endswith('.csv'):
        outfile = outfile + '.csv'

    if not args.overwrite and os.path.exists(outfile):
        raise FileExistsError(f'Output file {outfile} already exists')

    Path(os.path.dirname(outfile)).mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.input)  # .replace(to_replace={'Target Name': 'none'}, value=None)
    agg_df = df.groupby('Inferred Target ID').agg(
        {'Inferred Target Name': 'first', 'min_lon': 'min', 'min_lat': 'min', 'max_lon': 'max', 'max_lat': 'max'})

    def to_wkt(row):
        return box(row['min_lon'], row['min_lat'], row['max_lon'], row['max_lat']).wkt

    agg_df['Bounding Box WKT'] = agg_df.apply(to_wkt, axis=1)

    agg_df.to_csv(outfile)

    if args.geo and has_gp:
        try:
            geo_out = outfile[:-4] + '.gpkg'

            if not args.overwrite and os.path.exists(geo_out):
                raise FileExistsError(f'Output file {geo_out} already exists')

            agg_df['Bounding Box WKT'] = geopandas.GeoSeries.from_wkt(agg_df['Bounding Box WKT'])
            gdf = geopandas.GeoDataFrame(agg_df, geometry='Bounding Box WKT', crs="EPSG:4326")

            gdf.to_file(geo_out)
        except Exception as e:
            print(f'Unable to produce gpkg due to error: {e}')
    elif args.geo and not has_gp:
        print('Cannot create gpkg as geopandas dependency is not installed')


if __name__ == '__main__':
    main()
