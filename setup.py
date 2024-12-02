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

from setuptools import setup, find_packages

setup(
    name='oco3_sam_zarr',
    version='2024.12.02',
    url='https://github.com/EarthDigitalTwin/OCO3-data-transformer',
    author='Riley Kuttruff',
    author_email='riley.k.kuttruff@jpl.nasa.gov',
    description='Extract SAMs from OCO-3 data and store them as Zarr either locally or in S3',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests", "scripts"]),
    platforms='any',
    python_requires='>=3.9',
    include_package_data=True,
    package_data=dict(sam_extract=['schema/run-config-schema.yaml']),
)
