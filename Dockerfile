FROM continuumio/miniconda3:23.5.2-0

COPY conda-requirements.txt ./conda-requirements.txt

RUN conda install -n base --override-channels -c conda-forge -c defaults mamba libarchive && conda clean -afy

RUN mamba install -yc conda-forge --file conda-requirements.txt && mamba clean -afy

COPY sam_extract /sam_extract
COPY setup.py /setup.py

RUN python3 setup.py install clean && conda clean -afy

ENTRYPOINT ["tini", "-g", "--"]
