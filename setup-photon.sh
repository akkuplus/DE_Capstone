#! /usr/bin/env bash

sudo mkdir -p /home/hadoop/Photon
sudo chmod -R 777 /home/hadoop/Photon
cd /home/hadoop/Photon

aws s3 cp s3://f3de543c84/Photon/ . --recursive
#cat *tar.* | tar -xvf - -i
#tar -xvf photon_data.tar