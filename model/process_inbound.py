import os
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

from model import helper, analyze


def process_immoscout_data(spark, input_path, output_path):
    """
    Process the file containing data for apartment rentals in the given path,
    derive tables and write these tables as parquet.

    :param spark:       Apache Spark session.
    :param input_path:  Path for Input data.
    :param output_path: Path for Ouput data.
    """

    # GET filepath to data
    input_location = os.path.join(input_path, "src_immoscout", "immo_data.csv")
    assert os.path.isfile(input_location), f"Data source {input_location} does not exist"

    # LOAD Rental data
    column_types = {'geo_plz': 'str', 'telekomHybridUploadSpeed': 'str', 'noParkSpaces': 'str', 'houseNumber': 'str',
                    'noRooms': 'str', 'thermalChar': 'str', 'floor': 'str', 'telekomUploadSpeed': 'str'}
    pdDF_rental = pd.io.parsers.read_csv(input_location, dtype=column_types)

    # ANALYZE data
    regex_pattern = "(\d){5}"
    bool_index_four_digits = ~pdDF_rental["geo_plz"].str.match(regex_pattern)
    pdDF_rental.loc[ bool_index_four_digits, ["geo_plz", "geo_bln", "geo_krs"]]  # which zipcodes having not five digits
    #analyze.show_stats1(spark, pdDF_rental)  # show (intermediate) results

    bool_index_saxony_in_state = pdDF_rental["geo_bln"].str.contains("Sachsen")
    pdDF_rental.loc[bool_index_saxony_in_state, ["geo_plz", "geo_bln", "geo_krs"]]  # zipcodes state name contains Saxony

    # FILL incomplete zipcode

    # 1. Add leading zero to zipcode, this sets correct zipcodes
    bool_index = bool_index_four_digits & bool_index_saxony_in_state
    pdDF_rental.loc[ bool_index, ["geo_plz"]] = pdDF_rental.loc[ bool_index, ["geo_plz"]].apply(lambda row: "0"+row)
    pdDF_rental.loc[ bool_index, ["geo_plz", "geo_bln", "geo_krs"]]  # show

    # 2. Add trailing 'X' to zipcode.
    # This can lead to incorrect zipcodes. Later, we just use the leading two decimals!
    bool_index = bool_index_four_digits & ~bool_index_saxony_in_state
    pdDF_rental.loc[bool_index, ["geo_plz"]] = pdDF_rental.loc[bool_index, ["geo_plz"]].apply(lambda row: row + "X")
    pdDF_rental.loc[bool_index, ["geo_plz", "geo_bln", "geo_krs", "regio1", "regio2", "regio3"]]  # show
    # E.g., 'Neuenheim-Heidelberg' has correct zipcode 69120 not 69110
    # see https://www.plz-suche.org/de/ortssuche?ort=Neuenheim&submit=+

    # DROP unrelevant columns
    pdDF_rental.drop(columns=["description", "facilities"], inplace=True)

    # GET Spark dataframe
    RENTAL_SCHEMA = helper.get_schemas('RENTAL')
    assert set(pdDF_rental.columns) == set(RENTAL_SCHEMA.names), f"Columns of Dataframe and schema do not match:\n " \
        f"{set(RENTAL_SCHEMA.names).symmetric_difference(set(pdDF_rental.columns))}"
    df_rental = spark.createDataFrame(pdDF_rental, schema=RENTAL_SCHEMA)

    # EXTRACT LOCATION table to parquet
    columns = ["scoutId", "date", "regio1", "regio2", "regio3",
                 "geo_bln", "geo_krs", "geo_plz", "street", "streetPlain", "houseNumber"]
    helper.extract_to_table(base_table=df_rental, columns=columns, table_name="table_rental_location", output_path=output_path)

    # EXTRACT PRICE_and_COST table to parquet
    columns=["scoutId", "date", "serviceCharge", "newlyConst", "pricetrend",
                "totalRent", "baseRent", "baseRentRange", "heatingCosts", "electricityBasePrice",
                 "electricityKwhPrice"]
    helper.extract_to_table(base_table=df_rental, columns=columns, table_name="table_rental_price_and_cost", output_path=output_path)

    # EXTRACT FEATURE table to parquet
    columns = ["scoutId", "date", "heatingType", "telekomTvOffer", "telekomHybridUploadSpeed",
               "telekomUploadSpeed", "balcony", "picturecount", "yearConstructed", "yearConstructedRange",
               "noParkSpaces", "firingTypes", "hasKitchen", "cellar", "livingSpace", "condition", "interiorQual",
               "petsAllowed", "lift", "typeOfFlat", "noRooms", "thermalChar", "floor", "numberOfFloors",
               "noRoomsRange", "garden", "livingSpaceRange", "energyEfficiencyClass", "lastRefurbish"]
    helper.extract_to_table(base_table=df_rental, columns=columns, table_name="table_rental_feature", output_path=output_path)

    return


def process_station_data(spark, input_path, output_path):
    """
    Process the file containing data for stations of public transfer in the given path,
    derive tables and write these tables as parquet.

    :param spark:       Apache Spark session.
    :param input_path:  Path for Input data.
    :param output_path: Path for Ouput data.
    """

    # GET filepath to data
    input_location = os.path.join(input_path, "src_opendata-oepnv", "zHV_aktuell_csv.2021-09-17.csv")
    assert os.path.isfile(input_location), f"Data source {input_location} does not exist"

    # LOAD data
    STATION_SCHEMA = helper.get_schemas("STATION")
    # Alternative with XML, since csv is less complex

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

    # Transform values of latitude and longitude
    df_stations = df_stations.withColumn('Latitude', F.regexp_replace('Latitude', ',', '.'))
    df_stations = df_stations.withColumn('Latitude', df_stations['Latitude'].cast("float"))
    df_stations = df_stations.withColumn('Longitude', F.regexp_replace('Longitude', ',', '.'))
    df_stations = df_stations.withColumn('Longitude', df_stations['Longitude'].cast("float"))
    helper.logger.debug("Imported Station data")

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

    # EXTRACT table to parquet
    columns = ["SeqNo", "Type", "DHID", "Parent", "Name", "Latitude", "Longitude", "MunicipalityCode", "Municipality"]
    helper.extract_to_table(base_table=table_stations_Gemeinden, columns=columns,
                            table_name="table_majorstations_in_municipals", output_path=output_path)

    return


def process_mappings(spark, input_path, output_path):
    """
    Process the files containing data for mappings (municipality code to zip code, and zip code to coordinates)
    in the given path, derive tables and write these tables as parquet.

    :param spark:       Apache Spark session.
    :param input_path:  Path for Input data.
    :param output_path: Path for Ouput data.
    """

    # 1. IMPORT mapping municipality code -> zip code
    # GET filepath to data
    input_location = os.path.join(input_path, "src_destatis-Gemeindeschluessel", "AuszugGV3QAktuell.xlsx")
    assert os.path.isfile(input_location), f"Data source {input_location} does not exist"

    # LOAD data
    schema = helper.get_schemas("MAPPING")

    df_mapping_municipal_to_zip = spark.read.format("com.crealytics.spark.excel") \
        .option("useHeader", "true") \
        .option("Schema", schema) \
        .option("dataAddress", f"'Extract'!A1") \
        .load(input_location)

    # EXTRACT table to parquet
    columns = df_mapping_municipal_to_zip.columns
    helper.extract_to_table(base_table=df_mapping_municipal_to_zip, columns=columns,
                            table_name="table_mapping_municipal_to_zip", output_path=output_path)

    # 2. IMPORT mapping zipcode to geo coordinates
    # GET filepath to data
    input_location = os.path.join(input_path, "src_WZBSocialScienceCenter_plz_geocoord", "plz_geocoord.txt")
    assert os.path.isfile(input_location), f"Data source {input_location} does not exist"

    # LOAD data
    table_mapping_zip_to_coor = spark.read \
        .option("header", True) \
        .option("multiLine", False) \
        .option("ignoreLeadingWhiteSpace", True) \
        .option("sep", ",") \
        .option("mode", "PERMISSIVE") \
        .csv(input_location)

    # EXTRACT table to parquet
    columns = table_mapping_zip_to_coor.columns
    helper.extract_to_table(base_table=table_mapping_zip_to_coor, columns=columns,
                            table_name="table_mapping_zip_to_coor", output_path=output_path)

    return
