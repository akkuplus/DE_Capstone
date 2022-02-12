#! /usr/bin/env bash
export PYTHONPATH="/home/hadoop/.local/lib/python3.7/site-packages:$PYTHONPATH"
export PYTHONPATH="/usr/lib/spark/python:$PYTHONPATH"
export PYSPARK_DRIVER_PYTHON=/home/hadoop/.local/bin/jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser'
echo $PYTHONPATH
echo $PYSPARK_DRIVER_PYTHON
echo $PYSPARK_DRIVER_PYTHON_OPTS
pyspark
