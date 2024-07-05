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
import json
import os
from pathlib import Path

import pandas as pd
from pyproj import Geod
from shapely import wkt
from shapely.geometry import box

try:
    import geopandas
    has_gp = True
except ImportError:
    has_gp = False

GEOD = Geod(ellps='WGS84')


def point_to_bbox_km(point_wkt, dist_km):
    distance = dist_km * 1000
    point = wkt.loads(point_wkt)
    lat, lon = point.y, point.x
    max_lat = GEOD.fwd(lon, lat, 0, distance)[1]
    max_lon = GEOD.fwd(lon, lat, 90, distance)[0]
    min_lat = GEOD.fwd(lon, lat, 180, distance)[1]
    min_lon = GEOD.fwd(lon, lat, 270, distance)[0]
    return dict(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat)


def point_to_bbox_deg(point_wkt, dist_deg):
    point = wkt.loads(point_wkt)
    lat, lon = point.y, point.x
    return dict(min_lon=lon - dist_deg, min_lat=lat - dist_deg, max_lon=lon + dist_deg, max_lat=lat + dist_deg)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        dest='target_file',
        help='Path to file containing target definitions. Expected format: clasp_report.csv '
             '(https://oco3car.jpl.nasa.gov/api/report/clasp)'
    )

    def offset(s):
        v = float(s)

        if v <= 0:
            raise ValueError('--offset must be > 0')

        return v

    parser.add_argument(
        dest='offset',
        type=offset,
        help='Offset from centroid for bboxes. Must be > 0.'
    )

    parser.add_argument(
        '--unit',
        dest='unit',
        choices=['km', 'deg'],
        default='km',
        help='Units of the value of --offset. Boxes are computed by target centroid +/- offset in --units. Default: km'
    )

    parser.add_argument(
        '-o', '--output',
        dest='output',
        default=os.path.join(os.getcwd(), 'targets.json'),
        help='Output file path. Defaults to targets.json in the current working directory.'
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

    if not os.path.isfile(args.target_file):
        raise FileNotFoundError('Target file does not exist')

    outfile: str = args.output

    if not outfile.endswith('.json'):
        outfile = outfile + '.json'

    if not args.overwrite and os.path.exists(outfile):
        raise FileExistsError(f'Output file {outfile} already exists')

    Path(os.path.dirname(outfile)).mkdir(parents=True, exist_ok=True)

    target_df = pd.read_csv(args.target_file)[[
        'Target ID', 'Target Name', 'Site Center WKT'
    ]].rename(
        {
            'Target ID': 'tid',
            'Target Name': 'tn',
            'Site Center WKT': 'point',
        },
        axis='columns'
    )

    target_dict = {}
    fun = point_to_bbox_deg if args.unit == 'deg' else point_to_bbox_km

    for row in target_df.itertuples():
        tid = row.tid
        tn = row.tn
        center_wkt = row.point

        target_dict[tid] = dict(
            id=tid,
            name=tn,
            centroid_wkt=center_wkt,
            bbox=fun(center_wkt, args.offset)
        )

    with open(outfile, 'w') as fp:
        json.dump(target_dict, fp, sort_keys=True, indent=4)

    if args.geo and has_gp:
        try:
            geo_out = outfile.removesuffix('.json') + '.gpkg'

            if not args.overwrite and os.path.exists(geo_out):
                raise FileExistsError(f'Output file {geo_out} already exists')

            df_dict = {
                'Target ID': [],
                'Target Name': [],
                'Centroid WKT': [],
                'Bounding Box WKT': [],
            }

            for tid in target_dict:
                df_dict['Target ID'].append(tid)
                df_dict['Target Name'].append(target_dict[tid]['name'])
                df_dict['Centroid WKT'].append(target_dict[tid]['centroid_wkt'])
                bbox = target_dict[tid]['bbox']
                df_dict['Bounding Box WKT'].append(box(bbox['min_lon'], bbox['min_lat'],
                                                       bbox['max_lon'], bbox['max_lat']).wkt)

            df = pd.DataFrame(df_dict)

            df['Bounding Box WKT'] = geopandas.GeoSeries.from_wkt(df['Bounding Box WKT'])
            gdf = geopandas.GeoDataFrame(df, geometry='Bounding Box WKT')

            gdf.to_file(geo_out)
        except Exception as e:
            print(f'Unable to produce gpkg due to error: {e}')
    elif args.geo and not has_gp:
        print('Cannot create gpkg as geopandas dependency is not installed')


if __name__ == '__main__':
    main()
