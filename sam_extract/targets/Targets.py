import json

import importlib.resources as pkg_resources

from sam_extract import targets

class Targets:
    targets = None

    @classmethod
    def __read(cls):
        input_file = (pkg_resources.files(targets) / 'targets.json')

        with input_file.open('rt') as f:
            target_list = json.load(f)['targets']

        locations = []

        for type in target_list:
            for location in type['locations']:
                location['type'] = type['type']
                locations.append(location)

        return locations

    @classmethod
    def get_default_target_list(cls):
        if not cls.targets:
            cls.targets = cls.__read()

        return cls.targets
