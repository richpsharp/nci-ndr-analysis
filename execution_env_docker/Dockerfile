FROM conda/miniconda3

# this builds nci/ndr-execution, should be run as so:
# docker run -it --rm -v `cwd`:/var/workspace nci/ndr-execution
# the version of this dockerfile is the one designed to run with the current
# commit of this repository. There is otherwise no other versioning.

USER root
RUN apt-get update \
    && apt-get install -y \
        build-essential \
        curl \
        unzip \
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
    pip install --no-cache-dir git+git://github.com/richpsharp/InSPRING.git@6bad8cdbe3f568fba2762746a107394b3fa799d8; \
    conda clean -a -y"

WORKDIR /home/root
RUN curl "https://d1vvhvl2y92vvt.cloudfront.net/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip

ARG AWS_ACCESS_KEY
ARG AWS_SECRET_KEY

RUN /usr/local/bin/aws configure set aws_access_key_id $AWS_ACCESS_KEY && \
    /usr/local/bin/aws configure set aws_secret_access_key $AWS_SECRET_KEY && \
    /usr/local/bin/aws configure set default.region us-west-1

RUN /usr/local/envs/py37/bin/pip install gsutil

WORKDIR /var/workspace
ENTRYPOINT ["/bin/bash", "-i", "execute.sh"]
CMD []
