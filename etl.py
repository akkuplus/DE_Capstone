import os

import model.helper as help
import model.process_inbound as process_inbound


def main():
    """
    Implement main ETL functionality.
    """

    spark = help.create_spark_session()

    help.logger.debug("Created Spark session")

    input_path = os.path.join(os.getcwd(), "data")
    help.logger.debug(f"Set input_data to {input_path}")
    output_path = os.path.join(os.getcwd(), "data")
    help.logger.debug(f"Set output_data to {output_path}")

    # Rental data
    process_inbound.process_immoscout_data(spark, input_path, output_path)

    # Station data
    process_inbound.process_station_data(spark, input_path, output_path)

    # Mappings municipal code, zip code, coordinates
    process_inbound.process_mappings(spark, input_path, output_path)


if __name__ == "__main__":

    main()
