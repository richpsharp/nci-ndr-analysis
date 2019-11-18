#!/bin/bash
yum update -y
amazon-linux-extras install docker -y
service docker start
systemctl enable docker
usermod -a -G docker ec2-user
yum install git -y
git clone https://github.com/richpsharp/nci-ndr-analysis.git /root/nci-ndr-analysis
cd /root/nci-ndr-analysis
docker run --rm -p 8888:8888 -v `pwd`:/var/workspace natcap/nci-ndr-execution:1 nci_ndr_watershed_worker.py 8888 > docker_log.txt
