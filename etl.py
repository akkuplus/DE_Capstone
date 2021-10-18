import os

import model.helper as helper
import model.process_inbound as process_inbound
import model.process_outbound as process_outbound


def main():
    """
    Implement main ETL functionality.
    """

    # GET Spark session
    spark = helper.create_spark_session()

    # SET input and output path
    #input_path = os.path.join(os.getcwd(), "data")
    input_path = helper.get_config_or_default("DataLake", "DATA_LAKE_INPUT_PATH")
    helper.logger.debug(f"Set input_data_path to {input_path}")

    #output_path = os.path.join(os.getcwd(), "data")
    output_path = helper.get_config_or_default("DataLake", "DATA_LAKE_OUTPUT_PATH")
    helper.logger.debug(f"Set output_data_path to {output_path}")

    if True:
        # GET Rental data
        process_inbound.process_immoscout_data(spark, input_path, output_path)

        # GET Station data
        process_inbound.process_station_data(spark, input_path, output_path)

        # GET mappings municipal code, zip code, coordinates
        process_inbound.process_mappings(spark, input_path, output_path)

    process_outbound.main()
    helper.logger.debug("Finished ETL\n")


if __name__ == "__main__":
    main()
