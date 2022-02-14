#! /usr/bin/env bash

# Directories
sudo chmod -R 777 /home/hadoop
sudo chmod -R 777 /tmp
sudo chmod -R 777 /home/hadoop/s3a

# https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421
# https://www.educative.io/edpresso/how-to-set-up-a-spark-environment

# Environment variables
sudo export SPARK_HOME="/usr/lib/spark"
echo $SPARK_HOME

sudo export PATH="$SPARK_HOME/bin:$PATH"
sudo export PATH="/home/hadoop/.local/bin:$PATH"
echo $PATH

sudo export PYSPARK_PYTHON="/usr/lib/spark/python"
echo $PYSPARK_PYTHON

sudo export PYTHONPATH="/usr/lib/spark/python:/home/hadoop/.local/lib/python3.7/site-packages:$PYTHONPATH"
echo $PYTHONPATH

sudo export PYSPARK_DRIVER_PYTHON="/home/hadoop/.local/bin/jupyter"
echo $PYSPARK_DRIVER_PYTHON

sudo export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser"
echo $PYSPARK_DRIVER_PYTHON_OPTS
echo ""

sleep 2s

# Pip
pip3 install --user py4j  jupyter ipython requests~=2.26.0 s3fs~=2021.10.1 pandas~=1.3.3 numpy~=1.21.2 scipy~=1.7.2 scikit-learn~=1.0.1 folium~=0.12.1.post1 plotly~=5.4.0 Flask~=2.0.2
