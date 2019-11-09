FROM conda/miniconda3

# this builds nci/ndr-execution, should be run as so:
# docker run -it --rm -v `cwd`:/var/workspace nci/ndr-execution
# the version of this dockerfile is the one designed to run with the current
# commit of this repository. There is otherwise no other versioning.

USER root
RUN apt-get update \
&& apt-get install -y \
    build-essential \
    git \
    libspatialindex-c4v5 \
    mercurial \
&& rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-c"]
RUN conda create -y --name py37 python=3.7 && conda clean -a -y
RUN conda run -v -n py37 conda install -c conda-forge gdal=2.4.1
RUN conda run -v -n py37 pip install --no-cache-dir \
    cython \
    ecoshard==0.3.1 \
    pygeoprocessing==1.9.0 \
    pytest \
    pytest-cov \
    mock \
    numpy \
    retrying \
    rtree \
    scipy \
    setuptools-scm \
    shapely \
    sympy \
    taskgraph==0.8.5 && conda clean -a -y

RUN conda init bash && echo "source activate py37" > ~/.bashrc
WORKDIR /var/workspace
ENTRYPOINT /bin/bash -i execute.sh
