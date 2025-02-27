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
import subprocess

from io import TextIOWrapper
from os.path import abspath

SAFE_VARS = [
    'input_granule_limit',
    'output_chunk_time',
    'output_chunk_lat',
    'output_chunk_lon',
    'output_grid_resolution',
    'schedule_frequency',
    'disable_schedule',
]


def tfvars_to_json(tfvars_path):
    print(f'Parsing variables from {tfvars_path}')

    cmd = 'go run main.go'.split(' ')
    cmd.extend([abspath(tfvars_path), '-'])

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        cwd=abspath('tfvars2json')
    )

    ret = p.wait()

    if ret != 0:
        print('subprocess failed')
        exit(ret)

    buf = TextIOWrapper(p.stdout, encoding='utf-8')

    return json.load(buf)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'f1',
        metavar='tfvars_file_1',
        help='First .tfvars file',
    )

    parser.add_argument(
        'f2',
        metavar='tfvars_file_2',
        help='Second .tfvars file',
    )

    args = parser.parse_args()

    v1 = tfvars_to_json(args.f1)
    v2 = tfvars_to_json(args.f2)

    problems = []

    for var in list(set(list(v1.keys()) + list(v2.keys()))):
        if var in SAFE_VARS:
            continue

        if v1.get(var) == v2.get(var):
            problems.append(var)

    if len(problems) > 0:
        print('Invalid terraform variable configuration!\nThe following variables have the same value for global '
              'and target-focused deployments:\n  - ' + '\n  - '.join(sorted(problems)))
        # print('\n  - '.join(problems))
        exit(1)
    else:
        print('Terraform variable configuration is valid!')
        exit(0)
