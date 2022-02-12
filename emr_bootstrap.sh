#!/bin/bash -xe


sudo aws s3 cp s3://f3de543c84/Capstone_Project/ ./ --recursive
sudo aws s3 cp s3://f3de543c84/Capstone_Project/ /home/hadoop/ --recursive

sudo amazon-linux-extras enable corretto8
sudo yum install java-1.8.0-amazon-corretto -y


#sudo yum -y install git


# sudo yum -y install python3
# sudo yum -y install python3-pip
sudo python3 -m pip install requests~=2.26.0 s3fs~=2021.10.1 pandas~=1.3.3 numpy~=1.21.2 scipy~=1.7.2 scikit-learn~=1.0.1 folium~=0.12.1.post1 plotly~=5.4.0 Flask~=2.0.2
#sudo pip3 install -U requests~=2.26.0 s3fs~=2021.10.1 pandas~=1.3.3 numpy~=1.21.2 scipy~=1.7.2 scikit-learn~=1.0.1 folium~=0.12.1.post1 plotly~=5.4.0 Flask~=2.0.2


#sudo curl -OL https://github.com/komoot/photon/releases/download/0.3.5/photon-0.3.5.jar
#sudo curl --insecure --output photon-db-latest.tar.bz2 https://download1.graphhopper.com/public/extracts/by-country-code/de/photon-db-de-latest.tar.bz2  | pbzip2 -cd | tar x
#sudo java -jar photon-0.3.5.jar


#sudo docker cp . jupyterhub:/home/hadoop
#sudo docker restart jupyterhub
