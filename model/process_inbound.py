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


    # GET filepath to data
    input_location = os.path.join(input_path, "immoscout", "immo_data.csv")
    assert os.path.isfile(input_location), f"Data source {input_location} does not exist"


    # LOAD Rental data
    pdDF_rental =  pd.io.parsers.read_csv(input_location, dtype={'geo_plz': 'str',
                                                                 'telekomHybridUploadSpeed': 'str',
                                                                 'noParkSpaces': 'str',
                                                                 'houseNumber': 'str',
                                                                 'noRooms': 'str',
                                                                 'thermalChar': 'str',
                                                                 'floor': 'str',
                                                                 'telekomUploadSpeed': 'str'})



    # ANALYZE data
    regex_pattern = "(\d){5}"
    bool_index = pdDF_rental["geo_plz"].str.match(regex_pattern)
    bool_index_four_digits = ~pdDF_rental["geo_plz"].str.match(regex_pattern)
    pdDF_rental.loc[ bool_index_four_digits, ["geo_plz", "geo_bln", "geo_krs"]]  # which zipcodes having not five digits
    bool_index_saxony_in_state = pdDF_rental["geo_bln"].str.contains("Sachsen")
    pdDF_rental.loc[bool_index_saxony_in_state, ["geo_plz", "geo_bln", "geo_krs"]]  # zipcodes having a leading zero


    # FILL incomplete zipcode
    # 1. Add leading zero to zipcode, this sets correct zipcodes
    bool_index = bool_index_four_digits & bool_index_saxony_in_state
    pdDF_rental.loc[ bool_index, ["geo_plz"]] = pdDF_rental.loc[ bool_index, ["geo_plz"]].apply(lambda row: "0"+row)  # add a leading zero
    pdDF_rental.loc[ bool_index, ["geo_plz", "geo_bln", "geo_krs"]]
    # 2. Add trailing 'X' to zipcode. This can lead to incorrect zipcodes. Later we use just the leading two decimals!
    bool_index = bool_index_four_digits & ~bool_index_saxony_in_state
    pdDF_rental.loc[bool_index, ["geo_plz"]] = pdDF_rental.loc[bool_index, ["geo_plz"]].apply(lambda row: row + "X")  # add a leading zero
    pdDF_rental.loc[bool_index, ["geo_plz", "geo_bln", "geo_krs", "regio1", "regio2", "regio3"]]
    # E.g., Neuenheim-Heidelberg has zipcode 69120 not the 69110, see https://www.plz-suche.org/de/ortssuche?ort=Neuenheim&submit=+


    # SET relevant columns
    pdDF_rental.drop(columns=["description", "facilities",
                              #"houseNumber","petsAllowed"
                              ], inplace=True)


    # GET Spark datafraem
    RENTAL_SCHEMA = helper.get_schemas('RENTAL')
    assert set(pdDF_rental.columns) == set(RENTAL_SCHEMA.names), f"Columns of Dataframe and schema do not match:\n " \
        f"{set(RENTAL_SCHEMA.names).symmetric_difference(set(pdDF_rental.columns))}"
    df_rental = spark.createDataFrame(pdDF_rental, schema=RENTAL_SCHEMA)


    # EXTRACT to table
    columns = df_rental.columns
    helper.extract_to_table(base_table=df_rental, columns=columns, table_name="table_rental", output_path=output_path)

    """
    # SHOW example
    print("\nShowing sample of table_rental:")
    df_rental.show(10, truncate=False)
    helper.logger.debug("Imported raw data to table_rental")

    # PERSIST data to parquet
    output_location = os.path.join(output_path, "table_rental.parquet")
    df_rental.write.mode('overwrite').parquet(output_location)
    helper.logger.debug("Exported table_rental to parquet")

    """

    # LOCATION data
    columns = ["scoutId", "date", "regio1", "regio2", "regio3",
                 "geo_bln", "geo_krs", "geo_plz", "street", "streetPlain", "houseNumber"]
    helper.extract_to_table(base_table=df_rental, columns=columns, table_name="table_rental_location", output_path=output_path)


    # PRICE_and_COST data
    columns=["scoutId", "date", "serviceCharge", "newlyConst", "pricetrend",
                "totalRent", "baseRent", "baseRentRange", "heatingCosts", "electricityBasePrice",
                 "electricityKwhPrice"]
    helper.extract_to_table(base_table=df_rental, columns=columns, table_name="table_rental_price_and_cost", output_path=output_path)


    # FEATURE data
    columns = ["scoutId", "date", "heatingType",
                 "telekomTvOffer", "telekomHybridUploadSpeed",
                "telekomUploadSpeed", "balcony", "picturecount",
                "yearConstructed", "yearConstructedRange", "noParkSpaces",
                "firingTypes", "hasKitchen", "cellar",
                "livingSpace", "condition", "interiorQual",
                "petsAllowed", "lift", "typeOfFlat",
                "noRooms", "thermalChar", "floor",
                "numberOfFloors", "noRoomsRange", "garden",
                "livingSpaceRange",
                "energyEfficiencyClass", "lastRefurbish"]
    helper.extract_to_table(base_table=df_rental, columns=columns, table_name="table_rental_feature", output_path=output_path)


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


def get_tables_from_raw_data(spark, input_path, output_path):

    # Rental data
    data_location = os.path.join(output_path, "table_rental.parquet")
    if True: #> not (os.path.exists(data_location)):
        process_immoscout_data(spark, input_path, output_path)
    assert os.path.exists(data_location), f"Dataset {data_location} does not exist"
    helper.logger.debug(f"Dataset {data_location} exists.")

    # Station data
    data_location = os.path.join(output_path, "table_stations.parquet")
    if True: #not(os.path.exists(data_location)):
        process_station_data(spark, input_path, output_path)
    assert os.path.exists(data_location), f"Dataset {data_location} does not exist"
    helper.logger.debug(f"Dataset {data_location} exists.")

    # Mapping municipal code to zip code
    data_location = os.path.join(output_path, "table_Mapping_municipal_code_to_zip.parquet")
    if True: #not(os.path.exists(data_location)):
        process_mapping_municipality_to_zip(input_path, output_path)
    assert os.path.exists(data_location), f"Dataset {data_location} does not exist"
    helper.logger.debug(f"Dataset {data_location} exists.")

    return