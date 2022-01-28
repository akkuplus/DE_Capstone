#!/bin/bash -xe

sudo amazon-linux-extras enable corretto8
sudo yum install java-1.8.0-amazon-corretto -y

sudo yum –y install python3
sudo yum –y install python3-pip

sudo pip3 install -U pyarrow pyspark~=2.4.3 requests~=2.26.0 s3fs~=2021.10.1 pandas~=1.3.3 numpy~=1.21.2 scipy~=1.7.2 scikit-learn~=1.0.1 folium~=0.12.1.post1 plotly~=5.4.0 Flask~=2.0.2