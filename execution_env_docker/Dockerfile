FROM conda/miniconda3

# this builds nci/ndr-execution, should be run in the same directory as so:
# docker run -it --rm -v `cwd`:/var/workspace nci/ndr-execution
# the [TAG] is defined as the largest number in VERSION

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
    taskgraph && conda clean -a -y

RUN conda init bash && echo "source activate py37" > ~/.bashrc
WORKDIR /var/workspace
ENTRYPOINT /bin/bash -ic execute.sh
