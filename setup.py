from setuptools import setup, find_packages
from subprocess import check_call, CalledProcessError

try:
    check_call(['conda', 'install', '-yc', 'conda-forge', '--file', 'conda-requirements.txt'])
except (CalledProcessError, IOError) as e:
    raise EnvironmentError("Error installing conda packages", e)

setup(
    name='oco3_sam_zarr',
    version='0.5.0',
    url='https://github.jpl.nasa.gov/rileykk/oco-sam-extract',
    author='Riley Kuttruff',
    author_email='riley.k.kuttruff@jpl.nasa.gov',
    description='Extract SAMs from OCO-3 data and store them as Zarr either locally or in S3',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests", "scripts"]),
    # test_suite='tests',
    platforms='any',
    python_requires='>=3.9',
    include_package_data=True,
)
