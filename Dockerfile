# Copyright 2023 California Institute of Technology (Caltech)
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

FROM continuumio/miniconda3:23.5.2-0

# Install mamba because conda solve time is abysmal
RUN conda install -n base --override-channels -c conda-forge -c defaults mamba=1.5.0=py311h3072747_0 libmamba=1.5.0=h658169a_0 libmambapy=1.5.0=py311h527f279_0 libarchive=3.6.2=h039dbb9_1 && conda clean -afy


# Copy requirements list & install them
COPY conda-requirements.txt ./conda-requirements.txt
RUN mamba install -yc conda-forge --file conda-requirements.txt && mamba clean -afy

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
