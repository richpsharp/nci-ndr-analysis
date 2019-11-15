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
RUN conda create -y --name py37 python=3.7; conda clean -a -y
RUN conda init bash; echo "source activate py37" > ~/.bashrc
RUN /bin/bash -c "source ~/.bashrc; conda info; conda install -c conda-forge gdal=2.4.1"
COPY requirements.txt .
RUN /bin/bash -c "source ~/.bashrc; conda activate py37; \
    pip install --no-cache-dir -r requirements.txt; conda clean -a -y"
RUN /bin/bash -c "source ~/.bashrc; \
    pip install --no-dependencies --ignore-installed --no-cache-dir \
    git+git://github.com/natcap/ecoshard.git@3b92d5925e8502f7536f1d51ba6fc404687c2926; \
    conda clean -a -y"
RUN /bin/bash -c "source ~/.bashrc; \
    pip install --no-cache-dir git+git://github.com/richpsharp/InSPRING.git@553e63902dc46614d27fabb9b46da0e84291fe27; \
    conda clean -a -y"

WORKDIR /var/workspace
ENTRYPOINT ["/bin/bash", "-i", "execute.sh"]
CMD []
