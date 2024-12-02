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

FROM condaforge/mambaforge:24.9.2-0

# Copy requirements list & install them
COPY conda-requirements.txt ./conda-requirements.txt
RUN mamba install -yc conda-forge --file conda-requirements.txt && conda clean -ay

# Copy program data & setup scrip
COPY sam_extract /sam_extract
COPY setup.py /setup.py

# Install package
RUN python3 setup.py install clean && conda clean -afy

# Include repair tool
COPY tools/repair /tools/repair

# Include sync tool
COPY tools/s3Sync /tools/s3Sync

# Run
ENTRYPOINT ["tini", "-g", "--"]
