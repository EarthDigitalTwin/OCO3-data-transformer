from math import sqrt, ceil

import xarray as xr
import zarr

from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from sam_extract.targets import Targets

import argparse


def main(cfg):
    print(cfg)


def parse_args():
    parser = argparse.ArgumentParser()

    input_config = parser.add_argument_group('Config file', 'Use a config yaml file')

    local = parser.add_argument_group('Single input file', 'Process only a single input file with default '
                                                           'settings')

    local.add_argument('-f', '--input-file',
                       help='Path to local OCO-3 lite NetCDF file to process',
                       metavar='FILE',
                       dest='in_file')

    local.add_argument('-o', '--output',
                       help='Path to directory to store output zarr arrays',
                       metavar='DIR',
                       dest='out')

    input_config.add_argument('-i', help='Configuration yaml file', metavar='YAML', dest='cfg')

    args = parser.parse_args()

    if args.cfg and (args.out or args.in_file):
        raise ValueError('Must specify either single input file (+ output dir) or provide a config yaml file. Not both')
    elif not args.cfg and not all((args.out, args.in_file)):
        raise ValueError('If providing single input file, both input file and output dir must be provided')

    if args.cfg:
        with open(args.cfg) as f:
            cfg_yml = load(f, Loader=Loader)

        config_dict = {}

        try:
            output = cfg_yml['output']
            inp = cfg_yml['input']

            if output['local']['path']:
                config_dict['output'] = {'local': output['local']}
            elif output['s3']['url']:
                if not output['s3']['region']:
                    output['s3']['region'] = 'us-west-2'

                config_dict['output'] = {'s3': output['s3']}
            else:
                raise ValueError('No output params configured')

            if not any((inp['single-file'], inp['queue']['url'])):
                raise ValueError('No input params configured')

            if inp['queue']['url'] and not inp['queue']['region']:
                inp['queue']['region'] = 'us-west-2'

            if inp['single-file']:
                config_dict['input']['single-file'] = inp['single-file']
            else:
                config_dict['input'] = {'queue': inp['queue']}

            config_dict['drop-dims'] = [(dim['group'], dim['name']) for dim in cfg_yml['drop-dims']]

            locations = []

            for type in cfg_yml['targets']:
                for location in type['locations']:
                    location['type'] = type['type']
                    locations.append(location)

            config_dict['targets'] = locations

        except KeyError:
            pass # TODO error




    else:
        # TODO Eventually I'll enable & implement single use S3

        config_dict = {
            'output': {
                'local': {
                    'path': args.out
                }
            },
            'input': {
                'single-file': args.in_file
            },
            'drop-dims': [
                # for now leave this empty
            ],
            'targets': Targets.get_default_target_list()
        }

    return config_dict


if __name__ == '__main__':
    main(parse_args())
    # print(Targets.get_default_target_list())