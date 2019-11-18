#!/bin/bash
yum update -y
amazon-linux-extras install docker -y
service docker start
usermod -a -G docker ec2-user
yum install git -y
mkfs -t ext4 /dev/xvdb
mkdir /usr/local/workspace
mount /dev/xvdb /usr/local/workspace
echo "/dev/xvdb /usr/local/workspace ext4  defaults,nofail  0  2" >> /etc/fstab
git clone https://github.com/richpsharp/nci-ndr-analysis.git /usr/local/workspace/nci-ndr-analysis
docker run --rm -p 8080:8080 -v `pwd`:/var/workspace natcap/nci-ndr-execution:1 nci_ndr_watershed_manager.py 8080 10.0.1.57 > docker_log.txt
