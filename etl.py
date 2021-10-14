import os
import pandas as pd
import pyspark.sql.functions as F

import model.helper as help
import model.process_inbound as p_in



def main():
    """
    Implement main ETL functionality.
    """

    spark = help.create_spark_session()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    help.logger.debug("Created Spark session")

    input_path = os.path.join(os.getcwd(), "data")  # Alternative: input_data = "s3a://udacity-dend/"
    help.logger.debug(f"Set input_data to {input_path}")

    output_path = os.path.join(os.getcwd(), "data")
    # Alternative: #output_path = config['DATA']['OUTPUT_PATH']  # point to S3
    help.logger.debug(f"Set output_data to {output_path}")

    p_in.get_tables_from_raw_data(spark, input_path, output_path)


if __name__ == "__main__":

    main()
