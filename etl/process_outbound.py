import folium
import numpy as np
import os
import pandas as pd
from pyspark import StorageLevel
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import StructType, StructField, StringType
from scipy import spatial
import requests
import time
import webbrowser

import etl.helper as helper


def query_coordinates(spark, input_path, output_path, url="http://localhost:2322/api"):
    """
    Query coordinates of rental offers using address information and Photon geocoding service and save as parquet.

    :param spark:       Apache Spark session.
    :param input_path:  Path for Input data.
    :param output_path: Path for Output data.
    :param url:         URL of photon service.
    """

    # PREPARE table_rental_location
    helper.logger.info(f"Preparing table rental location for geocoding...")
    table_rental_location = prepare_table_rental_location(spark, input_path)

    assert helper.check_photon_service(url), f"No response from Photon on {url}"

    # GET and SAVE coordinates
    _query_coordinates(spark, url, table_rental_location, output_path)

    return


def prepare_table_rental_location(spark, input_path):
    """
    Process the files containing data for mappings (municipality code to zip code, and zip code to coordinates)
    in the given path, derive tables and write these tables as parquet.

    :param spark:       Apache Spark session.
    :param input_path:  Path for Output data.
    """
    # LOAD rental data
    table_name = "table_rental_location"
    table_rental_location = helper.create_df_from_parquet(spark, table_name, input_path)

    # PREPARE data for geocoding
    for column in table_rental_location.columns:
        table_rental_location = table_rental_location\
            .withColumn(column, F.when(F.isnan(F.col(column)), None).otherwise(F.col(column)))
    table_rental_location.show()

    column = "streetPlain"
    table_rental_location = table_rental_location \
        .select(["scoutId", "streetPlain", "houseNumber", "geo_plz", "geo_krs", "geo_bln"]) \
        .where(~F.col(column).contains('None')
               & ~F.col(column).contains('NULL')
               & ~F.col(column).contains('no_information')
               & ~(F.col(column) == '')
               & ~F.col(column).isNull()
               & ~F.isnan(column)) \
        .withColumn("_id", F.monotonically_increasing_id()) \
        .withColumn('streetPlain', F.regexp_replace('streetPlain', '_', ' ')) \
        .withColumn('houseNumber', F.regexp_replace('houseNumber', '-', ' ')) \
        .withColumn('houseNumber', F.regexp_replace('houseNumber', '[^0-9]', ' ')) \
        .withColumn('geo_krs', F.regexp_replace('geo_krs', '_Kreis', '')) \
        .withColumn('geo_krs', F.regexp_replace('geo_krs', '_', ' ')) \
        .withColumn('geo_bln', F.regexp_replace('geo_bln', '_', ' ')) \
        .withColumn('address_string', F.concat(F.col('streetPlain'), F.lit(' '),
                                               F.col('houseNumber'), F.lit(', '),
                                               F.col('geo_plz'), F.lit(', '),
                                               F.col('geo_krs'), F.lit(', '),
                                               F.col('geo_bln'))) \
        .cache()

    return table_rental_location


def _query_coordinates(spark, url, table_rental_location, output_path, idx=None):
    """
    Process the files containing data for mappings (municipality code to zip code, and zip code to coordinates)
    in the given path, derive tables and write these tables as parquet.

    :param spark:                   Apache Spark session.
    :param url:                     URL of Photon geocoding service.
    :param table_rental_location    Tables with rental offers and addresses.
    :param output_path:             Path for Output data.
    """

    # SETUP for geocoding
    schema = T.StructType([
        T.StructField("Lat", T.DoubleType(), False),
        T.StructField("Lng", T.DoubleType(), False)])
    UDF_geocode = F.udf(helper.geocode_coordinates, schema)
    # See: https://www.mikulskibartosz.name/derive-multiple-columns-from-single-column-in-pyspark/
    # See: https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/

    # SPLIT, maybe parallelize -> query separate photon instances
    if not idx:
        idx = table_rental_location.approxQuantile("_id", [0.5], 0.00001)[0]

    start_time = time.time()
    df_temp = table_rental_location.where(F.col("_id") <= idx)
    df = df_temp.withColumn('extractedCoords',
                            UDF_geocode(F.col("address_string"), F.lit(url)))  # duration: around one hour for one half

    # See: https://www.mikulskibartosz.name/derive-multiple-columns-from-single-column-in-pyspark/
    # See: https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/
    df = df.withColumn("lat", F.col("extractedCoords.Lat")) \
        .withColumn("lng", F.col("extractedCoords.Lng")) \
        .drop(F.col("extractedCoords"))
    helper.extract_to_table(base_table=df, output_path=output_path, single_partition=True,
                            table_name="table_rental_location_partA", columns=df.columns)
    end_time = time.time()
    helper.logger.info(f"Duration Querying PartA: {end_time - start_time}")

    start_time = time.time()
    df_temp = table_rental_location.where(F.col("_id") > idx)
    df = df_temp.withColumn('extractedCoords',
                            UDF_geocode(F.col("address_string"), F.lit(url)))  # last around one hour for second half

    # See: https://www.mikulskibartosz.name/derive-multiple-columns-from-single-column-in-pyspark/
    # See: https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/
    df = df.withColumn("lat", F.col("extractedCoords.Lat")) \
        .withColumn("lng", F.col("extractedCoords.Lng")) \
        .drop(F.col("extractedCoords"))
    helper.extract_to_table(base_table=df, output_path=output_path, single_partition=True,
                            table_name="table_rental_location_partB", columns=df.columns)
    end_time = time.time()
    helper.logger.info(f"Duration Querying PartB: {end_time - start_time}")

    return


def load_table_rental_locations_with_coordindates(spark, input_path):
    """
    Load and return table rental location with previously queried coordinates.

    :param spark:       Apache Spark session.
    :param input_path:  Path for Output data.
    :return table:      Spark dataframe of rental offers with coordinates.
    """
    # COMBINE combine parts of table_rental_location with coords
    table_name = "table_rental_location_partA.parquet"
    # input_local_path = "C:\Python\_Working\DatEng_Capstone"
    table_rental_location_a = helper.create_df_from_parquet(spark, table_name, input_path)

    table_name = "table_rental_location_partB.parquet"
    # input_local_path = "C:\Python\_Working\DatEng_Capstone"
    table_rental_location_b = helper.create_df_from_parquet(spark, table_name, input_path)

    table_rental_location_coords = table_rental_location_a.union(table_rental_location_b) \
        .withColumn("zip_group", F.col('geo_plz').substr(1, 3)) \
        .cache()
    del table_rental_location_a, table_rental_location_b

    return table_rental_location_coords


def _join_parent_and_child(table_stations_with_zip):
    """
    Join child rows on parent rows in table stations with zip.

    In the table, a lot of rows point to a parent. Parents holds information, so join children on these dependent rows.
    Parents have identical values in the columns DHID and Parent, and children have different values in both columns.
    """

    # split table_stations_with_zip into parents and childs. Parents have information regarding Municipality and ZIP,
    # childs have not.

    source_count = table_stations_with_zip.count()

    childs = table_stations_with_zip \
        .where(table_stations_with_zip["MunicipalityCode"] == "00000000") \
        .withColumnRenamed("Parent", "Parent_in_Child") \
        .withColumnRenamed("DHID", "DHID_in_Child") \
        .drop(*["Municipality", "MunicipalityCode", "MunicipalityCode_ZIP", "name_ZIP", "PLZ"])
    childs_count = childs.count()

    parents = table_stations_with_zip\
        .where(table_stations_with_zip["MunicipalityCode"] != "00000000") \
        .drop(*["SeqNo", "Type", "Name", "Latitude", "Longitude"]) \
        .withColumnRenamed("DHID", "DHID_in_Parent") \
        .withColumnRenamed("Parent", "Parent_in_Parent") \
        .withColumn('ZIP_Group_Station', F.col('PLZ').substr(1, 3))
    parents_count = parents.count()

    # JOIN parent on child to fill missing information
    cond = [childs.Parent_in_Child == parents.DHID_in_Parent]  # <==> where Child.Parent == Parent.DHID
    childs = childs.join(parents, cond, "left")
    childs = childs \
        .drop(*["DHID_in_Parent", "Parent_in_Parent"]) \
        .withColumnRenamed("DHID_in_CHild", "DHID") \
        .withColumnRenamed("Parent_in_Child", "Parent")

    try:
        table_stations_with_zip2 = table_stations_with_zip\
            .where(table_stations_with_zip["MunicipalityCode"] != "00000000")\
            .withColumn('ZIP_Group_Station', F.col('PLZ').substr(1, 3))\
            .union(childs)
        table_stations_with_zip2\
            .withColumn('ZIP_Group_Station', F.col('PLZ').substr(1, 3))\
            .cache()
        joined_count = table_stations_with_zip2.count()
        assert source_count == joined_count, f"Error transforming table_stations_with_zip. " \
                                             f"Count of rows of source and result differ."
    except Exception as ex:
        helper.logger.error(f"Error while union of parent and children of table_stations_with_zip. Reason:{ex}")
        raise

    return table_stations_with_zip2


def add_zipcode_to_stations(spark, input_path, output_path):
    """
    Add zipcode to station data using 'MunicipalityCode' and name the first three numbers of
    zipcode as 'ZIP_Group_Station'.

    :param spark:                       Apache Spark session.
    :param input_path:                  Path for Input data.
    :param output_path:                 Path for Output data.
    """

    # LOAD preprocessed STATION data
    # old: name = "table_majorstations_in_municipals"
    name = "table_stations"
    table_stations_in_municipals = helper.create_df_from_parquet(spark, name, input_path)

    # LOAD preprocessed MAPPING data (municipality to zip code)
    name = "table_mapping_municipal_to_zip"
    table_mapping_municipal_ZIP = helper.create_df_from_parquet(spark, name, input_path) \
        .withColumnRenamed("name", "name_ZIP")\
        .withColumnRenamed("MunicipalityCode", "MunicipalityCode_ZIP")

    # JOIN data of Station and Mapping (on municipality code)
    cond = [table_stations_in_municipals.MunicipalityCode == table_mapping_municipal_ZIP.MunicipalityCode_ZIP]
    table_stations_with_zip = table_stations_in_municipals\
        .join(table_mapping_municipal_ZIP, cond, "left")
    columns = table_stations_with_zip.columns
    helper.extract_to_table(base_table=table_stations_with_zip, columns=columns,
                            table_name="table_stations_with_zip", output_path=output_path,
                            single_partition=True)

    # AUSWERTUNG:
    # table_stations_with_zip2.where(F.col("PLZ").isNull()).count() Out[210]: 110141
    # table_stations_with_zip2.count() Out[211]: 511898
    # -> ca 1/5 hat keine PLZ
    # table_stations_with_zip2 = _join_parent_and_child(table_stations_with_zip)


    #GET stations without a zipcode -> collect in df
    file_name = "table_stations_with_zip"
    table_stations_with_zip = helper.create_df_from_parquet(spark, file_name, input_path)
    table_stations_with_zip = table_stations_with_zip.replace('', None)
    df = table_stations_with_zip.where(F.col("PLZ").isNull()).select("SeqNo", "Latitude", "Longitude")

    # SETUP Photon: Reverse geocode a coordinate
    assert helper.check_photon_service(), f"No response from Photon"
    url = "http://localhost:2322/reverse"
    UDF_geocode = F.udf(helper.geocode_zipcodes, T.StringType())
    file_name = "temp_filled_zipcodes.parquet"
    data_location = os.path.join(output_path, file_name)

    # QUERY zipcodes - first run
    df = df.withColumn('Q_PLZ', UDF_geocode(F.col("Latitude"), F.col("Longitude")))  # duration: half hour
    df = df.repartition(1)
    df.write.mode("overwrite").parquet(data_location)

    # QUERY zipcodes - rerun the query for remaining unknown zipcodes, because of some invalid connection
    # (Some Socket numbers are not unique)

    del df
    df = helper.create_df_from_parquet(spark, file_name, input_path=input_path).replace('', None, 'Q_PLZ')
    df = df.where(F.col("Q_PLZ").isNull())
    df = df.withColumn('Q_PLZ', UDF_geocode(F.col("Latitude"), F.col("Longitude"))) \
        .drop_duplicates().repartition(1)
    file_name = "temp_filled_zipcodes2.parquet"
    data_location = os.path.join(output_path, file_name)
    df.write.mode("append").parquet(data_location)

    del df
    df = helper.create_df_from_parquet(spark, file_name, input_path=input_path).replace('', None, 'Q_PLZ')
    df = df.where(F.col("Q_PLZ").isNull())
    df = df.withColumn('Q_PLZ', UDF_geocode(F.col("Latitude"), F.col("Longitude"))) \
        .drop_duplicates().repartition(1)
    file_name = "temp_filled_zipcodes3.parquet"
    data_location = os.path.join(output_path, file_name)
    df.write.mode("append").parquet(data_location)

    del df
    df = helper.create_df_from_parquet(spark, file_name, input_path=input_path).replace('', None, 'Q_PLZ')
    df = df.where(F.col("Q_PLZ").isNull())
    df = df.withColumn('Q_PLZ', UDF_geocode(F.col("Latitude"), F.col("Longitude"))) \
        .drop_duplicates().repartition(1)
    file_name = "temp_filled_zipcodes4.parquet"
    data_location = os.path.join(output_path, file_name)
    df.write.mode("append").parquet(data_location)

    del df
    file_name = "temp_filled_zipcodes4.parquet"
    df4 = helper.create_df_from_parquet(spark, file_name, input_path=input_path).where(F.col("Q_PLZ").isNotNull())
    file_name = "temp_filled_zipcodes3.parquet"
    df3 = helper.create_df_from_parquet(spark, file_name, input_path=input_path).where(F.col("Q_PLZ").isNotNull())
    file_name = "temp_filled_zipcodes2.parquet"
    df2 = helper.create_df_from_parquet(spark, file_name, input_path=input_path).where(F.col("Q_PLZ").isNotNull())
    file_name = "temp_filled_zipcodes.parquet"
    df = helper.create_df_from_parquet(spark, file_name, input_path=input_path).where(F.col("Q_PLZ").isNotNull())
    df = df.union(df2).union(df3).union(df4).drop_duplicates()
    df.show()
    df.where(F.col("Q_PLZ").isNull()).count()

    # COUNT of rows with and without a zipcode -> ACTUAL counts vary depending on your photon results!
    df.show(1)
    df.count()                              # count of all rows: Out[56]: 151090
    df.where(F.col("Q_PLZ").isNotNull()).count()  # count of rows with a zipcode: Out[29]: 103453
    df.where(F.col("Q_PLZ") != "").count()  # count of rows with a zipcode: Out[29]: 103453
    df.where(F.col("Q_PLZ").isNull()).count()  # count of rows without a zipcode: Out[30]: 47637
    df.where(F.col("Q_PLZ") == "").count()  # count of rows without a zipcode: Out[30]: 47637

    # COMBINE source and derivate -> add column Q_PLZ to source
    table_stations_with_zip3 = table_stations_with_zip.join(df.drop("Latitude", "Longitude"), ["SeqNo"], "left")
    table_stations_with_zip3.count()
    table_stations_with_zip3.show()

    table_stations_with_zip3.where(F.col("PLZ").isNull()).count()  # >>>    Out[65]: 110141
    table_stations_with_zip3.where(F.col("Q_PLZ").isNull()).count()   # >>>    Out[68]: 58

    # DROP empty column PLZ, and rename column Q_plz to PLZ
    UDF_combine = F.udf(helper.combine, T.StringType())
    table_stations_with_zip4 = table_stations_with_zip3 \
        .withColumn('C_PLZ', UDF_combine(F.col("PLZ"), F.col("Q_PLZ"))) \
        .drop("PLZ", "Q_PLZ") \
        .withColumnRenamed("C_PLZ", "PLZ") \
        .withColumn('ZIP_Group_Station', F.col('PLZ').substr(1, 3))

    # SHOW some stats
    table_stations_with_zip4.show()
    table_stations_with_zip4.count()
    table_stations_with_zip4.where(F.col("PLZ").isNull()).count()
    table_stations_with_zip4.where(F.col("PLZ").isNotNull()).count()
    table_stations_with_zip4.where(F.col("PLZ").isNotNull()).show()

    columns = table_stations_with_zip4.columns
    helper.extract_to_table(base_table=table_stations_with_zip4, columns=columns,
                            table_name="table_stations_with_zip_final", output_path=output_path,
                            single_partition=True)

    # CLEAN data and extract stations with a zip code -> function clean_queried_stations
    # Appending data leads to pairs of rows having identical SeqNo. Those pairs have rows without a zipcode AND
    # rows with a zipcode. From those pairs, reduce rows without zipcode and keep those with a zip code!

    return


"""
def _query_zipcodes(spark, url, table_stations_with_zip, output_path, idx=None):
    """"""
    Process the files containing data for mappings (municipality code to zip code, and zip code to coordinates)
    in the given path, derive tables and write these tables as parquet.

    :param spark:                   Apache Spark session.
    :param url:                     URL of Photon geocoding service.
    :param table_stations_with_zip    Tables with rental offers and addresses.
    :param output_path:             Path for Output data.
    """"""

    # SETUP for geocoding
    schema = T.StructType([
        T.StructField("Lat", T.DoubleType(), False),
        T.StructField("Lng", T.DoubleType(), False)])
    UDF_geocode = F.udf(helper.geocode_zipcodes, schema)
    # See: https://www.mikulskibartosz.name/derive-multiple-columns-from-single-column-in-pyspark/
    # See: https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/

    # SPLIT, maybe parallelize -> query separate photon instances
    if not idx:
        idx = table_stations_with_zip.approxQuantile("_id", [0.5], 0.00001)[0]

    start_time = time.time()
    df_temp = table_stations_with_zip.where(F.col("_id") <= idx)
    df = df_temp.withColumn('extractedCoords',
                            UDF_geocode(F.col("address_string"), F.lit(url)))  # last around one hour for first half

    # See: https://www.mikulskibartosz.name/derive-multiple-columns-from-single-column-in-pyspark/
    # See: https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/
    df = df.withColumn("lat", F.col("extractedCoords.Lat")) \
        .withColumn("lng", F.col("extractedCoords.Lng")) \
        .drop(F.col("extractedCoords"))
    helper.extract_to_table(base_table=df, output_path=output_path, single_partition=True,
                            table_name="table_rental_location_partA", columns=df.columns)
    end_time = time.time()
    helper.logger.info(f"Duration Querying PartA: {end_time - start_time}")

    start_time = time.time()
    df_temp = table_rental_location.where(F.col("_id") > idx)
    df = df_temp.withColumn('extractedCoords',
                            UDF_geocode(F.col("address_string"), F.lit(url)))  # last around one hour for second half

    # See: https://www.mikulskibartosz.name/derive-multiple-columns-from-single-column-in-pyspark/
    # See: https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/
    df = df.withColumn("lat", F.col("extractedCoords.Lat")) \
        .withColumn("lng", F.col("extractedCoords.Lng")) \
        .drop(F.col("extractedCoords"))
    helper.extract_to_table(base_table=df, output_path=output_path, single_partition=True,
                            table_name="table_rental_location_partB", columns=df.columns)
    end_time = time.time()
    helper.logger.info(f"Duration Querying PartB: {end_time - start_time}")

    return
"""


def group_stations_by_zipcode(spark, input_path, output_path):
    """
    Group/partition stations by first three numbers of the zip codes.

    :param spark:                       Apache Spark session.
    :param input_path:                  Path for Input data.
    :param output_path:                 Path for Output data.
    """

    # GET table stations, that holds shorted zipcode and relevant facts
    name = "table_stations_with_zip_final"
    table_stations_with_zip = helper.create_df_from_parquet(spark, name, input_path)

    # GET table rentals, that holds zip code
    name = "table_rental_location"
    required_columns = ["geo_plz"]  # drop scoutId and just handle all distinct zip_code groups
    table_rental_location = helper.create_df_from_parquet(spark, name, input_path) \
        .select(required_columns) \
        .withColumn('ZIP_Group_Rental', F.col('geo_plz').substr(1, 3)) \
        .drop("geo_plz") \
        .distinct()
    table_rental_location.show()

    # Statistic: Check total counts of zip group:
    # table_rental_location.select("ZIP_Group_Rental").distinct().count() >>> Out[94]: 733
    # table_stations_grouped.select("ZIP_Group_Station").distinct().count() >>> Out[95]: 547

    # JOIN Rental and Station tables on zip-group
    # left join keeps the zip group of the rentals and adds all stations with matching zip group
    cond = [table_rental_location.ZIP_Group_Rental == table_stations_with_zip.ZIP_Group_Station]
    table_stations_grouped = table_rental_location \
        .join(table_stations_with_zip, cond, "left") \
        .repartition(1) \
        .cache()


    # PERSIST to parquet
    table_name = "table_stations_grouped"
    columns = table_stations_grouped.columns
    helper.extract_to_table(base_table=table_stations_grouped, columns=columns,
                            single_partition=True, table_name=table_name, output_path=output_path,
                            show_example=False)

    return


def query_nearest_stations(spark,
                           input_path,
                           scoutId=None,
                           table_rental_location_coords=None,
                           table_stations_grouped=None):
    """
    Query stations next to given rental location providing scoutId.

    :param spark:                           Apache Spark session.
    :param input_path:                      Path for Input data.
    :param scoutId:                         scoutId of offered rental.
    :param table_rental_location_coords:    Spark dataframe of rental offers with coordinates.
    """

    """
    Strategie:
    je scoutId mit Werten lat und lng aus der table_rental_location_coords:
    - gehe in table_KT_rental_with_station und filtere darin auf jeweilige scoutId -> erstelle gruppe je scoutId
    - bestimme nächste Nachbarn von (lat, lng):
        - erstelle / schätze kd baum für gruppe in table_KT_rental_with_station
        - durchsuche kd baum für (lat, lng) -> es werden entsprechend anzahl kleinster nachbarn mehrere indizes ausgegeben
        - für jeweiligen index in gruppe ermittle Parent_Id Latitude Longitude Name
    """

    # TODO: partitioniere table_rental_location_coords und table_KT_rental_with_station nach zip_group
    # TODO: exkludiere zip_group mit (PLZ gleich null) in table_KT_rental_with_station

    # LOAD table_stations_grouped
    if not table_stations_grouped:
        table_name = "table_stations_grouped"
        helper.logger.info("Loading table_stations_grouped...")
        table_stations_grouped = helper.create_df_from_parquet(spark, table_name, input_path)
    assert table_stations_grouped, f"Dataframe table_stations_grouped is NOT loaded."

    # LOAD table table_rental_location_coords
    if not table_rental_location_coords:
        helper.logger.info("Loading table_rental_location_coords...")
        table_rental_location_coords = load_table_rental_locations_with_coordindates(spark, input_path)
    assert table_rental_location_coords, f"Dataframe table_rental_location_coords is NOT loaded."

    if not scoutId:
        scoutId = 115209583  # rental offer in Leipzig, Nordstr.
        helper.logger.info("Found no valid scoutId. Using example scoutId={scoutId}".format(scoutId))

    # GET single rental coordinates via scoutId
    row = table_rental_location_coords.where(F.col("scoutId").isin(scoutId)).collect()
    assert len(row) == 1, f"Cant find single valid scoutId {scoutId} in rental offers (table_rental_location_coords)"
    row = row[0]
    helper.logger.info(f"Found valid scoutId {scoutId}. Querying nearest stations...")

    # PREPARE for kd tree
    df_stations_for_zip_groups = table_stations_grouped \
        .where(( F.col("ZIP_Group_Rental") == row["zip_group"]))\
        .select(*["Latitude", "Longitude", "Parent",
                  "DHID", "Name"]) \
        .dropDuplicates() \
        .toPandas()\
        .dropna()

    if (df_stations_for_zip_groups["Latitude"].count() >0) \
            & (df_stations_for_zip_groups["Longitude"].count()>0):
        stations_in_zipgroups = df_stations_for_zip_groups.loc[:, ["Latitude", "Longitude"]].values.tolist()

        # QUERY kd tree to find nearest neighbours
        # See https://kanoki.org/2020/08/05/find-nearest-neighbor-using-kd-tree/
        tree = spatial.KDTree(stations_in_zipgroups)
        start_location = (row["lng"], row["lat"], row["address_string"])
        idxs = tree.query((row["lng"], row["lat"]), 10)
        print(f"\nRental location:\n{start_location}\n")
        print(f"Nearest Stations:\n")
        nearest_stations = df_stations_for_zip_groups.iloc[idxs[1], :]
        nearest_stations = nearest_stations.dropna()
        print(f"{nearest_stations}\n")

        # TODO: check nearest_stations
        if nearest_stations.empty:
            helper.logger.debug("Invalid nearest_stations")
            # Ist DF korrekt für weiterverarbeitung?
            # nearest_stations = df.columns = ['Latitude', 'Longitude', 'Parent', 'DHID', 'Name']

    return (row, nearest_stations)


def show_nearest_station(spark, output_path, scoutId, scout_apartment_infos, nearest_stations):

    """
    scout_apartment_infos, nearest_stations = query_nearest_stations(
        spark,
        input_path=output_path,
        scoutId=scoutId,
        table_rental_location_coords=table_rental_location_coords,
        table_stations_grouped=table_stations_grouped)
    """

    m = folium.Map(location=[scout_apartment_infos["lng"], scout_apartment_infos["lat"]], zoom_start=15)
    folium.CircleMarker(location=(scout_apartment_infos["lng"], scout_apartment_infos["lat"]),
                        tooltip=scout_apartment_infos.address_string,
                        color="red",
                        fill=True
                        ).add_to(m)

    for station in nearest_stations.itertuples():
        folium.CircleMarker(location=(station.Latitude, station.Longitude),
                            popup=station.Name, tooltip=station.Name).add_to(m)
        m.save("index.html")

    # Show HTMl in Webbrowser via Python: https://stackoverflow.com/a/21437460
    path = os.path.abspath('temp.html')
    url = 'file://' + path

    with open(path, 'w') as f:
        f.write(m._repr_html_())
    webbrowser.open(url)

    return m, url