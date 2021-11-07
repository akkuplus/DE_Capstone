import os

import etl.helper as helper
import etl.process_inbound as process_inbound
import etl.process_outbound as process_outbound


def main():
    """
    Implement main ETL functionality.
    """

    spark, input_path, output_path = helper.get_setup(do_test_s3_access=False)

    # GET working mode
    work_mode = helper.get_config_or_default("General", "work_mode").split(",")

    if "process_immoscout_data" in work_mode:
        # GET Rental data
        process_inbound.process_immoscout_data(spark, input_path, output_path)

    if "process_station_data" in work_mode:
        # GET Station data
        process_inbound.process_station_data(spark, input_path, output_path)

    if "process_mappings" in work_mode:
        # GET mappings municipal code, zip code, coordinates
        process_inbound.process_mappings(spark, input_path, output_path)

    if "process_outbound" in work_mode:

        # ADD zip code to Station data
        table_stations_with_zip = process_outbound.step1(spark, input_path)

        # GET Key Table holding joined data of Rental and Station
        KT_rental_with_station = process_outbound.step2(spark, input_path, output_path,
                                                        table_stations_with_zip=table_stations_with_zip)

        # GET data of Rental location with matched Station
        process_outbound.step_last(spark, input_path, output_path,
                               KT_rental_with_station=KT_rental_with_station)

        helper.logger.debug("Created data mart")

    helper.logger.debug("Finished ETL\n")


if __name__ == "__main__":
    pass
    main()
