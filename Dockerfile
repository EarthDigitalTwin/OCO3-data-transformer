FROM continuumio/miniconda3:23.5.2-0

# Install mamba because conda solve time is abysmal
RUN conda install -n base --override-channels -c conda-forge -c defaults mamba libarchive && conda clean -afy

# Copy requirements list & install them
COPY conda-requirements.txt ./conda-requirements.txt
RUN mamba install -yc conda-forge --file conda-requirements.txt && mamba clean -afy

# Copy program data & setup scrip
COPY sam_extract /sam_extract
COPY setup.py /setup.py

# Install package
RUN python3 setup.py install clean && conda clean -afy

# TEMPORARY: Until pydata/xarray#8016 is merged, install the branch it's based off of

COPY xarray-dev-install.sh /xarray-dev-install.sh
RUN /xarray-dev-install.sh

###

# Run
ENTRYPOINT ["tini", "-g", "--"]
