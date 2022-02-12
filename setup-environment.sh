#! /usr/bin/env bash

# https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421
# https://www.educative.io/edpresso/how-to-set-up-a-spark-environment

export SPARK_HOME=/usr/lib/spark
export PATH="$SPARK_HOME/bin:$PATH"
export PATH="/home/hadoop/.local/bin:$PATH"
export PYSPARK_PYTHON=/usr/lib/spark/python


pip install --user py4j requests~=2.26.0 s3fs~=2021.10.1 pandas~=1.3.3 numpy~=1.21.2 scipy~=1.7.2 scikit-learn~=1.0.1 folium~=0.12.1.post1 plotly~=5.4.0 Flask~=2.0.2 jupyter ipython seaborn


export PYTHONPATH="/usr/lib/spark/python:/home/hadoop/.local/lib/python3.7/site-packages:$PYTHONPATH"
export PYSPARK_DRIVER_PYTHON=/home/hadoop/.local/bin/jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser'
