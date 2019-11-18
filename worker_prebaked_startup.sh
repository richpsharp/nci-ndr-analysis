#!/bin/bash
cd /root/nci-ndr-analysis
git pull
docker run --rm -p 8888:8888 -v `pwd`:/var/workspace natcap/nci-ndr-execution:1 nci_ndr_watershed_worker.py 8888 > docker_log.txt
