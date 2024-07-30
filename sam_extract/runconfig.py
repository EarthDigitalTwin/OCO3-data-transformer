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


import logging
import warnings
from copy import deepcopy
from typing import Literal

import yamale
from pkg_resources import resource_filename
from sam_extract.exceptions import ConfigError

logger = logging.getLogger(__name__)
warnings.simplefilter('always', DeprecationWarning)


class RunConfig(object):
    def __init__(self, path: str, config_dict: dict):
        self.__path = path
        self.__dict = config_dict

    @staticmethod
    def parse_config_file(file_path: str):
        schema_file = resource_filename('sam_extract', 'schema/run-config-schema.yaml')

        yaml_schema = yamale.make_schema(schema_file)
        config_dict = yamale.make_data(file_path)

        yamale.validate(yaml_schema, config_dict)

        config_dict = config_dict[0][0]

        output = config_dict['output']
        input_section = config_dict['input']

        def s3_or_local(d: dict, path: str) -> Literal['s3', 'local']:
            if 's3' not in d and 'local' not in d:
                raise ConfigError(f'Config section "{path}" needs either "s3" or "local" sections')
            if 's3' in d and 'local' in d:
                raise ConfigError(f'Config section "{path}" cannot have both "s3" and "local" sections')

            return 's3' if 's3' in d else 'local'

        output['type'] = s3_or_local(output, 'output')

        if 'cog' in output:
            output['cog']['output']['type'] = s3_or_local(output['cog']['output'], 'output.cog.output')

        if 'files' not in input_section and 'queue' not in input_section:
            raise ConfigError(f'Config section "input" needs either "files" or "queue" sections')
        if 'files' in input_section and 'queue' in input_section:
            raise ConfigError(f'Config section "input" cannot have both "files" and "queue" sections')

        input_section['type'] = 'files' if 'files' in input_section else 'queue'

        config_dict['drop-dims'] = [
            (dim['group'], dim['name']) for dim in config_dict.get('drop-dims', [])
        ]

        config_dict['chunking']['config'] = (
            config_dict['chunking']['time'],
            config_dict['chunking']['longitude'],
            config_dict['chunking']['latitude']
        )

        if 'title' not in output:
            config_dict['output']['title'] = dict(
                pre_qf=output['naming']['pre_qf'].split('.zarr')[0],
                post_qf=output['naming']['post_qf'].split('.zarr')[0],
            )

        config = RunConfig(file_path, config_dict)

        return config

    def __getitem__(self, item):
        warnings.warn(
            '__getitem__ is deprecated, use property attributes instead',
            DeprecationWarning,
            stacklevel=2
        )
        #
        # logger.warning('Shouldn\'t be here :(')

        if item in self.__dict:
            return deepcopy(self.__dict)[item]
        else:
            raise KeyError(f'"{item}" is not defined in the config')

    def get(self, item, default=None):
        warnings.warn(
            'get is deprecated, use property attributes instead',
            DeprecationWarning,
            stacklevel=2
        )

        return deepcopy(self.__dict).get(item, default)

    @property
    def input_type(self):
        return self.__dict['input']['type']

    @property
    def input(self):
        if self.input_type == 'files':
            return self.__dict['input']['files']
        else:
            return self.__dict['input']['queue']

    @property
    def output_type(self):
        return self.__dict['output']['type']

    @property
    def output(self):
        if self.output_type == 'local':
            return self.__dict['output']['local']
        else:
            return self.__dict['output']['s3']

    @property
    def output_aws_region(self):
        if self.output_type == 's3':
            return self.__dict['output']['s3'].get('region', 'us-west-2')
        else:
            return None

    @property
    def grid(self):
        return self.__dict['grid']

    def grid_method(self, default):
        return self.grid.get('method', default)

    @property
    def exclude_vars(self):
        return self.__dict['drop-dims']

    @property
    def exclude_groups(self):
        return self.__dict.get('exclude-groups', [])

    @property
    def chunking(self):
        return self.__dict['chunking']['config']

    @property
    def pre_qf_name(self):
        return self.__dict['output']['naming']['pre_qf']

    @property
    def post_qf_name(self):
        return self.__dict['output']['naming']['post_qf']

    @property
    def pre_qf_title(self):
        return self.__dict['output']['title']['pre_qf']

    @property
    def post_qf_title(self):
        return self.__dict['output']['title']['post_qf']

    @property
    def is_global(self):
        return self.__dict['output'].get('global', True)

    @property
    def cog(self):
        return 'cog' in self.__dict['output']

    @property
    def cog_output_type(self):
        if self.cog:
            return 'local' if 'local' in self.__dict['output']['cog']['output'] else 's3'
        else:
            return None

    @property
    def cog_output(self):
        if self.cog:
            if self.cog_output_type == 'local':
                return self.__dict['output']['cog']['output']['local']
            else:
                return self.__dict['output']['cog']['output']['s3']
        else:
            return None

    @property
    def cog_efs(self):
        if self.cog:
            return self.__dict['output']['cog'].get('efs', False)
        else:
            return False

    @property
    def cog_options(self):
        return self.__dict['output'].get('cog', {}).get('options', {})

    @property
    def naming_dict(self):
        return self.__dict['output']['naming']

    @property
    def title_dict(self):
        return self.__dict['output']['title']

    @property
    def max_workers(self):
        return self.__dict['max-workers']

    @property
    def mask_scaling(self):
        return self.__dict.get('mask-scaling', 1.00)

    @property
    def target_file(self):
        return self.__dict.get('target-file', None)



