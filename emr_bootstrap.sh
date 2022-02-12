#!/bin/bash -xe

sudo aws s3 cp s3://f3de543c84/Capstone_Project/ ./ --recursive
sudo aws s3 cp s3://f3de543c84/Capstone_Project/ /home/hadoop/ --recursive

sudo amazon-linux-extras enable corretto8
sudo yum install java-1.8.0-amazon-corretto -y