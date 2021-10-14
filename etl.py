import os
import pandas as pd
import pyspark.sql.functions as F

from model import helper


def process_immoscout_data(spark, input_path, output_path):
    """
    Process the apartment rental data files in "immoscout" subdirectory
     and derive tables.

    :param spark: SparkSession
    :param input_path: Project path for Input data. Assuming subdirectory "immoscout" holds data within csv file.
    :param output_path: Project path for Output data.
    """

    # GET filepath to song data file
    input_location = os.path.join(input_path, "immoscout", "immo_data.csv")
    assert os.path.isfile(input_location), f"Data source {input_location} does not exist"

    # LOAD Rental data
    RENTAL_SCHEMA = helper.get_schemas('RENTAL')
    df_rental = spark.read \
        .schema(RENTAL_SCHEMA) \
        .option("header", True) \
        .option("multiLine", True) \
        .option("ignoreLeadingWhiteSpace", True) \
        .option("escape", "\\") \
        .option("quote", "\"") \
        .option("delimiter", ",") \
        .option("sep", ",") \
        .option("mode", "PERMISSIVE") \
        .csv(input_location)

    # SHOW example
    print("\nShowing sample of table_rental:")
    df_rental.show(10, truncate=True)
    helper.logger.debug("Imported raw data to table_rental")

    # PERSIST data to parquet
    output_location = os.path.join(output_path, "table_rental.parquet")
    df_rental.write.mode("overwrite").parquet(output_location)
    helper.logger.debug("Exported table_rental to parquet")

    ##### LOCATION data
    # EXTRACT columns to create table
    table_rental_location = df_rental\
        .select(["scoutId", "date", "regio1", "regio2", "regio3",
                 "geo_bln", "geo_krs", "geo_plz", "street", "streetPlain", "houseNumber"])\
        .drop_duplicates()\
        .dropna(subset='scoutId')

    # SHOW example
    print("\nShowing sample of table_rental_location:")
    table_rental_location.show(10, truncate=False)

    # PERSIST to parquet
    output_location = os.path.join(output_path, "table_rental_location.parquet")
    table_rental_location.write.mode('overwrite').parquet(output_location)
    helper.logger.debug("Exported table_rental_location as parquet")
    del table_rental_location

    ##### PRICE_and_COST data
    # EXTRACT columns to create table
    table_rental_price_and_cost = df_rental\
        .select(["scoutId", "date", "serviceCharge", "newlyConst", "pricetrend",
                "totalRent", "baseRent", "baseRentRange", "heatingCosts", "electricityBasePrice",
                 "electricityKwhPrice"])\
        .drop_duplicates()\
        .dropna(subset='scoutId')

    # SHOW example
    print("\nShowing sample of table_rental_price_and_cost:")
    table_rental_price_and_cost.show(10, truncate=False)

    # PERSIST to parquet
    output_location = os.path.join(output_path, "table_rental_price_and_cost.parquet")
    table_rental_price_and_cost.write.mode('overwrite').parquet(output_location)
    helper.logger.debug("Exported table_rental_price_and_cost as parquet")
    del table_rental_price_and_cost

    ##### FEATURE data
    # EXTRACT columns to create table
    table_rental_feature = df_rental\
        .select(["scoutId", "date", "heatingType",
                 "telekomTvOffer", "telekomHybridUploadSpeed",
                "telekomUploadSpeed", "balcony", "picturecount",
                "yearConstructed", "yearConstructedRange", "noParkSpaces",
                "firingTypes", "hasKitchen", "cellar",
                "livingSpace", "condition", "interiorQual",
                "petsAllowed", "lift", "typeOfFlat",
                "noRooms", "thermalChar", "floor",
                "numberOfFloors", "noRoomsRange", "garden",
                "livingSpaceRange",
                "energyEfficiencyClass", "lastRefurbish"])\
        .drop_duplicates()\
        .dropna(subset='scoutId')
    del df_rental

    # SHOW examples
    print("\nShowing sample of table_rental_feature:")
    table_rental_feature.show(10, truncate=False)

    # PERSIST to parquet
    output_location = os.path.join(output_path, "table_rental_feature.parquet")
    table_rental_feature.write.mode('overwrite').parquet(output_location)
    helper.logger.debug("Exported table_rental_feature as parquet")

    return


def process_station_data(spark, input_path, output_path):
    """
    Process the XXX data files in "XXX" subdirectory.
    Therewith derive tables XXX.

    :param spark: SparkSession
    :param input_path: Project path for Input data. Assuming subdirectory "XXX" holds song data within jsons files.
    :param output_path: Project path for Ouput data.
    """

    # GET filepath to data
    input_location = os.path.join(input_path, "opendata-oepnv", "zHV_aktuell_csv.2021-09-17.csv")
    assert os.path.isfile(input_location), f"Data source {input_location} does not exist"

    # LOAD data
    STATION_SCHEMA = helper.get_schemas("STATION")
    # Alternative with XML: df = spark.read.format('xml').options(rowTag='book').load(input_location)

    df_stations = spark.read \
        .schema(STATION_SCHEMA) \
        .option("header", True) \
        .option("multiLine", False) \
        .option("ignoreLeadingWhiteSpace", True) \
        .option("quote", "\"") \
        .option("delimiter", ";") \
        .option("sep", ";") \
        .option("mode", "PERMISSIVE") \
        .csv(input_location)

    # ADAPT data for Latitude and Longitude
    df_stations = df_stations.withColumn('Latitude', F.regexp_replace('Latitude', ',', '.'))
    df_stations = df_stations.withColumn('Latitude', df_stations['Latitude'].cast("float"))
    df_stations = df_stations.withColumn('Longitude', F.regexp_replace('Longitude', ',', '.'))
    df_stations = df_stations.withColumn('Longitude', df_stations['Longitude'].cast("float"))

    # SHOW example
    print("\nShowing sample of df_stations in city of Leipzig:")
    df_stations.where(F.col("MunicipalityCode") == "14713000").show(10, truncate=True)
    helper.logger.debug("Imported Station Data")

    # STORE data to parquet
    name = "table_stations"
    output_location = os.path.join(output_path, f"{name}.parquet")
    df_stations.write.mode("overwrite").parquet(output_location)
    helper.logger.debug(f"Exported {name} to parquet")

    # FIND all (major) stations that have a municipality code
    table_stations_Gemeinden = df_stations.where(df_stations["MunicipalityCode"] != "00000000")

    # ANALYSIS of filtered stations
    if False:
        # The resulting set of stations having a municipality code (!= "00000000") is equivalent
        # to a filter to the column "Type" to value "S"
        # -> ALTERNATIVE: table_stations_Gemeinden = table_station.where(table_station["Type"] == "S")
        # CHECK identity of columns DHID and Parent since the subset show all (major) stations within a municipality
        table_stations_Gemeinden = table_stations_Gemeinden.withColumn("isEqual_DHID_Parent",
            table_stations_Gemeinden.DHID == table_stations_Gemeinden.Parent)
        table_stations_Gemeinden.count()  # Get the count of all stations with munipicality code
        table_stations_Gemeinden.agg(F.count(F.when(F.col("isEqual_DHID_Parent"), 1))).show()  # Get the count equal values
        # of DHID and Parent. -> The counts are equal, and each row has identic values in columns DHID and Parent.
        # -> This subset of stations contains major stations.

    # STORE data of (major) stations having municipality code
    df_stations_in_municipal = table_stations_Gemeinden.select(["SeqNo", "Type", "DHID", "Parent", "Name", "Latitude",
                                                                "Longitude", "MunicipalityCode", "Municipality"])
    name = "table_stations_in_municipals"
    output_location = os.path.join(output_path, f"{name}.parquet")
    df_stations_in_municipal.write.mode("overwrite").parquet(output_location)
    assert os.path.exists(output_location), f"Dataset {output_location} does not exist."
    helper.logger.debug(f"Exported {name} as parquet")

    return


def process_mapping_municipality_to_zip(input_path, output_path):
    """
    Process the XXX data files in "XXX" subdirectory.
    Therewith derive table XXX.

    :param input_path: Project path for Input data. Assuming subdirectory "XXX" holds song data within jsons files.
    :param output_path: Project path for Ouput data.
    """

    # EXTRACT mapping
    column_types = {"AGS": str, "Bezeichnung": str, "PLZ": str}
    data_location = os.path.join(input_path, "destatis-Gemeindeschluessel", "AuszugGV3QAktuell.xlsx")
    os.path.exists(data_location)  # sheet "Extract" holds an extract of sheet "Onlineprodukt_Gemeinden"
    dfpd_mapping_municipal_codes = pd.read_excel(data_location, engine="openpyxl",
                                     sheet_name="Extract", names=column_types.keys(),
                                     header=0, dtype=column_types)

    output_location = os.path.join(output_path, "table_Mapping_municipal_code_to_zip.parquet")
    dfpd_mapping_municipal_codes.to_parquet(output_location, compression="snappy")
    helper.logger.debug("Exported table_Mapping_municipal_code_to_zip as parquet")

    return


def process_mapping_zip_2_coord(spark, input_path, output_path):
    """
    Process the XXX data files in "XXX" subdirectory.
    Therewith derive table XXX.

    :param input_path: Project path for Input data. Assuming subdirectory "XXX" holds song data within jsons files.
    :param output_path: Project path for Ouput data.
    """

    # EXTRACT mapping
    column_types = {"PLZ": str, "lat": float, "lng": float}
    data_location = os.path.join(input_path, "WZBSocialScienceCenter_plz_geocoord", "plz_geocoord.txt")
    assert os.path.exists(data_location)
    table_Mapping_zip_2_coor = spark.read \
        .option("header", True) \
        .option("multiLine", False) \
        .option("ignoreLeadingWhiteSpace", True) \
        .option("sep", ",") \
        .option("mode", "PERMISSIVE") \
        .csv(data_location)

    output_location = os.path.join(output_path, "table_mapping_zip_2_coor.parquet")
    table_Mapping_zip_2_coor.write.parquet(output_location)
    helper.logger.debug("Exported table_Mapping_zip_coor as parquet")

    return


def infer_tables_from_raw_data(spark, input_path, output_path):

    data_location = os.path.join(output_path, "table_rental.parquet")
    if True: #> not (os.path.exists(data_location)):
        process_immoscout_data(spark, input_path, output_path)
    assert os.path.exists(data_location), f"Dataset {data_location} does not exist"
    helper.logger.debug(f"Dataset {data_location} exists.")

    data_location = os.path.join(output_path, "table_stations.parquet")
    if True: #not(os.path.exists(data_location)):
        process_station_data(spark, input_path, output_path)
    assert os.path.exists(data_location), f"Dataset {data_location} does not exist"
    helper.logger.debug(f"Dataset {data_location} exists.")

    data_location = os.path.join(output_path, "table_Mapping_municipal_code_to_zip.parquet")
    if True: #not(os.path.exists(data_location)):
        process_mapping_municipality_to_zip(input_path, output_path)
    assert os.path.exists(data_location), f"Dataset {data_location} does not exist"
    helper.logger.debug(f"Dataset {data_location} exists.")

    return


def main():
    """
    Implement main ETL functionality.
    """

    spark = helper.create_spark_session()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    helper.logger.debug("Created Spark session")

    input_path = os.path.join(os.getcwd(), "data")  # Alternative: input_data = "s3a://udacity-dend/"
    helper.logger.debug(f"Set input_data to {input_path}")

    output_path = os.path.join(os.getcwd(), "data")
    # Alternative: #output_path = config['DATA']['OUTPUT_PATH']  # point to S3
    helper.logger.debug(f"Set output_data to {output_path}")

    infer_tables_from_raw_data(spark, input_path, output_path)


if __name__ == "__main__":
    main()
