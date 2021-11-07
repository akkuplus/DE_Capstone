import os
import pandas as pd
import pyspark.sql.functions as F

from etl import helper


def process_immoscout_data(spark, input_path, output_path):
    """
    Process the file containing data for apartment rentals in the given path,
    derive tables and write these tables as parquet.

    :param spark:       Apache Spark session.
    :param input_path:  Path for Input data.
    :param output_path: Path for Output data.
    """

    try:
        # GET filepath to data
        input_location = os.path.join(input_path, "immo_data.csv")
        data_object = helper.get_s3_object(input_location)
        helper.logger.info(f"Found object {input_location}: {data_object}")

        # LOAD Rental data
        helper.logger.info(f"Reading {input_location} into Pandas Dataframe ...")
        column_types = {'geo_plz': 'str', 'telekomHybridUploadSpeed': 'str', 'noParkSpaces': 'str', 'houseNumber': 'str',
                        'noRooms': 'str', 'thermalChar': 'str', 'floor': 'str', 'telekomUploadSpeed': 'str'}
        pdDF_rental = pd.io.parsers.read_csv(data_object, dtype=column_types)

        assert set(pdDF_rental.columns).issuperset(column_types), "Error while reading. Columns are not consistent."
        helper.logger.info(f"Successfully read {input_location} into Pandas Dataframe")

    except Exception as ex:
        helper.logger.error(f"Error while reading {input_location}. Reason: \n{ex}")
        raise

    # DROP unrelevant columns
    pdDF_rental.drop(columns=["description", "facilities"], inplace=True)
    pdDF_rental_raw = pdDF_rental.copy()

    # DATA CURATION
    # HANDLE incomplete zipcode
    regex_pattern = "(\d){5}"
    bool_index_four_digits = ~pdDF_rental["geo_plz"].str.match(regex_pattern)

    # Probleme - not all zipcodes have five digits
    pdDF_rental.loc[ bool_index_four_digits, ["geo_plz", "geo_bln", "geo_krs"]]\
        .sort_values(by=["geo_krs","geo_bln","geo_plz"])

    # 1. Add leading zero to zipcode
    bool_index_geo_krs = pdDF_rental["geo_krs"].str.contains("Bautzen_Kreis")
    bool_index = bool_index_four_digits & bool_index_geo_krs
    pdDF_rental.loc[ bool_index, ["geo_plz"]] = pdDF_rental.loc[ bool_index, ["geo_plz"]].apply(lambda row: "0"+row)

    bool_index_geo_krs = pdDF_rental["geo_krs"].str.contains("Vogtlandkreis")
    bool_index = bool_index_four_digits & bool_index_geo_krs
    pdDF_rental.loc[ bool_index, ["geo_plz"]] = pdDF_rental.loc[ bool_index, ["geo_plz"]].apply(lambda row: "0"+row)

    bool_index_geo_krs = pdDF_rental["geo_krs"].str.contains("Zwickau")
    bool_index = bool_index_four_digits & bool_index_geo_krs
    pdDF_rental.loc[ bool_index, ["geo_plz"]] = pdDF_rental.loc[ bool_index, ["geo_plz"]].apply(lambda row: "0"+row)

    # 2. Add trailing 'X' to zipcode
    # Later, we just use the leading decimals
    bool_index_geo_krs = pdDF_rental["geo_bln"].str.contains("Niedersachsen")
    bool_index = bool_index_four_digits & bool_index_geo_krs
    pdDF_rental.loc[bool_index, ["geo_plz"]] = pdDF_rental.loc[bool_index, ["geo_plz"]].apply(lambda row: row + "X")

    bool_index_geo_krs = pdDF_rental["geo_bln"].str.contains("Nordrhein_Westfalen")
    bool_index = bool_index_four_digits & bool_index_geo_krs
    pdDF_rental.loc[bool_index, ["geo_plz"]] = pdDF_rental.loc[bool_index, ["geo_plz"]].apply(lambda row: row + "X")

    bool_index_geo_krs = pdDF_rental["geo_bln"].str.contains("Baden_Württemberg")
    bool_index = bool_index_four_digits & bool_index_geo_krs
    pdDF_rental.loc[bool_index, ["geo_plz"]] = pdDF_rental.loc[bool_index, ["geo_plz"]].apply(lambda row: row + "X")

    bool_index_geo_krs = pdDF_rental["geo_krs"].str.contains("Plauen")
    bool_index = bool_index_four_digits & bool_index_geo_krs
    pdDF_rental.loc[bool_index, ["geo_plz"]] = pdDF_rental.loc[bool_index, ["geo_plz"]].apply(lambda row: row + "X")

    bool_index_geo_krs = pdDF_rental["geo_krs"].str.contains("Dortmund")
    bool_index = bool_index_four_digits & bool_index_geo_krs
    pdDF_rental.loc[bool_index, ["geo_plz"]] = pdDF_rental.loc[bool_index, ["geo_plz"]].apply(lambda row: row + "X")

    # GET Spark dataframe
    RENTAL_SCHEMA = helper.get_schemas('RENTAL')
    assert set(pdDF_rental.columns) == set(RENTAL_SCHEMA.names), f"Columns of Dataframe and schema do not match:\n " \
        f"{set(RENTAL_SCHEMA.names).symmetric_difference(set(pdDF_rental.columns))}"
    df_rental = spark.createDataFrame(pdDF_rental, schema=RENTAL_SCHEMA)
    #helper.logger.info("Created Spark DataFrame Rental")

    # EXTRACT LOCATION table to parquet
    columns = ["scoutId", "date", "regio1", "regio2", "regio3",
               "geo_bln", "geo_krs", "geo_plz", "street", "streetPlain", "houseNumber"]
    table_name = "table_rental_location"
    helper.extract_to_table(base_table=df_rental, columns=columns,
                            table_name=table_name, output_path=output_path, single_partition=True)
    helper.logger.info(f"Extracted parts of Rental data to table {table_name}")

    # EXTRACT PRICE_and_COST table to parquet
    columns = ["scoutId", "date", "serviceCharge", "newlyConst", "pricetrend",
               "totalRent", "baseRent", "baseRentRange", "heatingCosts", "electricityBasePrice",
               "electricityKwhPrice"]
    table_name = "table_rental_price_and_cost"
    helper.extract_to_table(base_table=df_rental, columns=columns,
                            table_name=table_name, output_path=output_path, single_partition=True)
    helper.logger.info(f"Extracted parts of Rental data to table {table_name}")

    # EXTRACT FEATURE table to parquet
    columns = ["scoutId", "date", "heatingType", "telekomTvOffer", "telekomHybridUploadSpeed",
               "telekomUploadSpeed", "balcony", "yearConstructed", "yearConstructedRange",
               "noParkSpaces", "firingTypes", "hasKitchen", "cellar", "livingSpace", "condition", "interiorQual",
               "petsAllowed", "lift", "typeOfFlat", "noRooms", "thermalChar", "floor", "numberOfFloors",
               "noRoomsRange", "garden", "livingSpaceRange", "energyEfficiencyClass", "lastRefurbish"]
    table_name = "table_rental_feature"
    helper.extract_to_table(base_table=df_rental, columns=columns,
                            table_name=table_name, output_path=output_path, single_partition=True)
    helper.logger.info(f"Extracted parts of Rental data to table {table_name}")

    return pdDF_rental_raw


def process_station_data(spark, input_path, output_path):
    """
    Process the file containing data for stations of public transfer in the given path,
    derive tables and write these tables as parquet.

    :param spark:       Apache Spark session.
    :param input_path:  Path for Input data.
    :param output_path: Path for Output data.
    """

    # GET filepath to data
    input_location = os.path.join(input_path, "zHV_aktuell_csv.2021-09-17.csv")
    assert helper.s3_object_exists(input_location), f"Data source {input_location} does not exist"
    helper.logger.info(f"Found object {input_location}")

    # LOAD data
    STATION_SCHEMA = helper.get_schemas("STATION")
    helper.logger.info(f"Reading {input_location} into Spark Dataframe ...")

    df_stations_raw = spark.read \
        .schema(STATION_SCHEMA) \
        .option("header", True) \
        .option("multiLine", False) \
        .option("ignoreLeadingWhiteSpace", True) \
        .option("quote", "\"") \
        .option("delimiter", ";") \
        .option("sep", ";") \
        .option("mode", "PERMISSIVE") \
        .csv(input_location) \
        .cache()

    helper.logger.info(f"Successfully read {input_location} into Spark Dataframe")

    # DATA CURATION
    # TRANSFORM values of latitude and longitude
    df_stations = df_stations_raw
    df_stations = df_stations.withColumn('Latitude', F.regexp_replace('Latitude', ',', '.'))
    df_stations = df_stations.withColumn('Longitude', F.regexp_replace('Longitude', ',', '.'))
    helper.logger.debug("Replaced set '.' as decimal separator for columns 'Latitude' and 'Longtitude'")

    df_stations = df_stations.withColumn('Latitude', df_stations['Latitude'].cast("float"))
    df_stations = df_stations.withColumn('Longitude', df_stations['Longitude'].cast("float"))
    helper.logger.debug("Cast columns 'Latitude' and 'Longtitude' to float")
    helper.logger.info("Imported Station data")

    # FIND all (major) stations that have a municipality code
    table_stations_Gemeinden = df_stations.where(df_stations["MunicipalityCode"] != "00000000")
    helper.logger.debug("Filtered Station data to MunicipalityCode unequal 00000000")

    # EXTRACT table to parquet
    columns = ["SeqNo", "Type", "DHID", "Parent", "Name", "Latitude", "Longitude", "MunicipalityCode", "Municipality"]
    helper.extract_to_table(base_table=table_stations_Gemeinden, columns=columns,
                            table_name="table_majorstations_in_municipals", output_path=output_path,
                            single_partition=True)
    df_stations.unpersist()
    del df_stations

    return df_stations_raw


def process_mappings(spark, input_path, output_path):
    """
    Process the files containing data for mappings (municipality code to zip code, and zip code to coordinates)
    in the given path, derive tables and write these tables as parquet.

    :param spark:       Apache Spark session.
    :param input_path:  Path for Input data.
    :param output_path: Path for Output data.
    """

    # 1. IMPORT mapping municipality code to zip code (from Statistische Bundesamt)
    # GET filepath to data
    input_location = os.path.join(input_path, "AuszugGV3QAktuell.xlsx")
    assert helper.s3_object_exists(input_location), f"Data source {input_location} does not exist"

    # LOAD data
    schema = helper.get_schemas("MAPPING_MUNICIPAL_ZIP")

    df_mapping_municipal_to_zip = spark.read.format("com.crealytics.spark.excel") \
        .option("useHeader", "true") \
        .option("Schema", schema) \
        .option("dataAddress", f"'Extract'!A1") \
        .load(input_location)
    df_mapping_municipal_to_zip.cache()

    #df_mapping_municipal_to_zip = df_mapping_municipal_to_zip.withColumn("Partition_PLZ",
    #     df_mapping_municipal_to_zip.PLZ.substr(1, 1)).cache()

    # EXTRACT table to parquet
    columns = df_mapping_municipal_to_zip.columns
    helper.extract_to_table(base_table=df_mapping_municipal_to_zip, columns=columns,
                            table_name="table_mapping_municipal_to_zip", output_path=output_path,
                            single_partition=True)
    df_mapping_municipal_to_zip.unpersist()
    del df_mapping_municipal_to_zip

    # 2. IMPORT mapping zipcode to geo coordinates
    # GET filepath to data
    input_location = os.path.join(input_path, "plz_geocoord.txt")
    assert helper.s3_object_exists(input_location), f"Data source {input_location} does not exist"

    # LOAD data
    schema = helper.get_schemas("MAPPING_ZIP_COOR")
    table_mapping_zip_to_coor = spark.read \
        .option("header", True) \
        .option("Schema", schema) \
        .option("multiLine", False) \
        .option("ignoreLeadingWhiteSpace", True) \
        .option("sep", ",") \
        .option("mode", "PERMISSIVE") \
        .csv(input_location).cache()

    # EXTRACT table to parquet
    columns = table_mapping_zip_to_coor.columns
    helper.extract_to_table(base_table=table_mapping_zip_to_coor, columns=columns,
                            table_name="table_mapping_zip_to_coor", output_path=output_path,
                            single_partition=True)
    table_mapping_zip_to_coor.unpersist()
    del table_mapping_zip_to_coor

    return
