#! /usr/bin/env bash
#sudo yum -y install python3
#sudo yum -y install python3-pip

echo $PYTHONPATH
echo $PYSPARK_DRIVER_PYTHON
echo $PYSPARK_DRIVER_PYTHON_OPTS

pyspark --conf spark.local.dir=/home/hadoop