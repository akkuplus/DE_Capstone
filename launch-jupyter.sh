#! /usr/bin/env bash
echo $PYTHONPATH
echo $PYSPARK_DRIVER_PYTHON
echo $PYSPARK_DRIVER_PYTHON_OPTS

pyspark --conf spark.local.dir=/home/hadoop