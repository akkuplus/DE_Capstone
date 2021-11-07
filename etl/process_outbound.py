import os
from math import sin

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import StructType, StructField, StringType

import etl.helper as helper


def main(spark, input_path, output_path):

    # Strategy:
    # apartment location  -> identify stations within a certain surrounding
    # -> find like 10 nearest stations

    # Q1) Which tables contain what data?
    location = os.path.join(input_path, "table_rental_location.parquet")
    table_rental_location = spark.read.parquet(location)
    print(f"\n\ntable_rental_location\n{table_rental_location.show(2, truncate=True)}")
    """
    +---------+-----+-------------------+--------------------+------------------+-------------------+--------------------+-------+--------------------+--------------------+-----------+
    |  scoutId| date|             regio1|              regio2|            regio3|            geo_bln|             geo_krs|geo_plz|              street|         streetPlain|houseNumber|
    +---------+-----+-------------------+--------------------+------------------+-------------------+--------------------+-------+--------------------+--------------------+-----------+
    |111372798|May19|            Sachsen|             Leipzig|          Plagwitz|            Sachsen|             Leipzig|  04229|Karl - Heine - St...|Karl_-_Heine_-_St...|          9|
    |114400812|Feb20|            Sachsen| Mittelsachsen_Kreis|            Döbeln|            Sachsen| Mittelsachsen_Kreis|  04720|    Burgstra&szlig;e|          Burgstraße|         11|
    """
    # how many nulls in columns, like zip code and streetPlain?
    # See https://sparkbyexamples.com/pyspark/pyspark-find-count-of-null-none-nan-values/
    #table_rental_location = table_rental_location.drop(F.col("is_na"))
    df_temp = table_rental_location.select(
        [F.count(
            F.when(F.col(c).contains('None') | F.col(c).contains('NULL')
                   | (F.col(c) == '' )  | F.col(c).isNull() | F.isnan(c), c
                   )
        ).alias(c) for c in table_rental_location.columns
        ])
    print(f"\nTotal of missing values\n{df_temp.show()}")
    del df_temp

    colums =["geo_plz"]
    expr = table_rental_location.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in colums])
    print(f"\nTotal of missing zip codes\n{expr.show()}")

    """
    addresses = table_rental_location.select(["geo_bln","geo_krs","geo_plz","streetPlain"]).drop_duplicates()
    >>> addresses.count()
        97250
    >>> addresses.groupby(["geo_bln"]).count().orderBy(["count"]).show()
        |             geo_bln|count|
        +--------------------+-----+
        |            Saarland|  505|
        |              Bremen| 1021|
        |             Hamburg| 1517|
        |Mecklenburg_Vorpo...| 2416|
        |           Thüringen| 2853|
        |         Brandenburg| 2935|
        |  Schleswig_Holstein| 3060|
        |     Rheinland_Pfalz| 3675|
        |              Berlin| 3905|
        |      Sachsen_Anhalt| 5312|
        |              Hessen| 6554|
        |   Baden_Württemberg| 7003|
        |       Niedersachsen| 7404|
        |              Bayern| 9632|
        |             Sachsen|12224|
        | Nordrhein_Westfalen|27234|
        +--------------------+-----+
    >>> table_rental_location.select(["geo_bln","geo_krs","streetPlain"]).drop_duplicates().count()    
        85991
    """


    print("\n\ntable_majorstations_in_municipals.parquet")
    location = os.path.join(input_path, "table_majorstations_in_municipals.parquet")
    table_majorstations_in_municipals = spark.read.parquet(location)
    table_majorstations_in_municipals.show(5)
    """
    +-----+----+--------------+--------------+--------------------+---------+---------+----------------+--------------------+
    |SeqNo|Type|          DHID|        Parent|                Name| Latitude|Longitude|MunicipalityCode|        Municipality|
    +-----+----+--------------+--------------+--------------------+---------+---------+----------------+--------------------+
    |  638|   S|de:07334:35017|de:07334:35017|Germersheim Schla...|49.222374| 8.376188|        07334007|         Germersheim|
    | 1896|   S| de:08111:2478| de:08111:2478|Stuttgart Zuffenh...|48.823223| 9.185597|        08111000|           Stuttgart|
    """


    print("\n\ntable_mapping_municipal_to_zip")
    location = os.path.join(input_path, "table_mapping_municipal_to_zip.parquet")
    table_mapping_municipal_to_zip = spark.read.parquet(location)
    table_mapping_municipal_to_zip.show(5)
    """
    +--------+---------------+-----+
    |     AGS|    Bezeichnung|  PLZ|
    +--------+---------------+-----+
    |01053024|    Düchelsdorf|23847|
    |01054079|     Löwenstedt|25864|
    """


    # Q2) combine which tables and how?
    # APPROACH 1)
    # ==> combine table_rental_location and table_majorstations_in_municipals:
    # match table_majorstations_in_municipals(MunicipalityCode) -> via zipcode -> with table_rental_location(geo_plz)

    # STATIONS data
    name = "table_majorstations_in_municipals"
    data_location = os.path.join(input_path, f"{name}.parquet")
    assert os.path.exists(data_location)
    table_stations_in_municipals = spark.read.parquet(data_location)
    table_stations_in_municipals.show(5, truncate=True)

    # Mapping data municipality AND zip
    name = "table_mapping_municipal_to_zip"
    data_location = os.path.join(input_path, f"{name}.parquet")
    assert os.path.exists(data_location)
    table_mapping_municipal_ZIP = spark.read.parquet(data_location)
    print("\n\ntable_mapping_municipal_to_zip")
    table_mapping_municipal_ZIP.show(5, truncate=True)

    # JOIN stations and zip on municipality
    cond = [table_stations_in_municipals.MunicipalityCode == table_mapping_municipal_ZIP.AGS]
    table_stations_with_zip = table_stations_in_municipals.join(table_mapping_municipal_ZIP, cond, "left")
    table_stations_with_zip.show(5, truncate=True)
    del table_mapping_municipal_ZIP

    colums =["PLZ"]
    print("Missing values of zipcode")
    print(f"Total of rows:{table_stations_with_zip.select(colums).count()}")
    print(f"Total rows missing a zipcode:{table_stations_with_zip.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in colums]).collect()}")
    # -> we have 971 missing PLZ -> continue with most stations

    # JOIN Rental and Station on zipcode
    location = os.path.join(input_path, "table_rental_location.parquet")
    table_rental_location = spark.read.parquet(location)

    table_KT_rental_location = table_rental_location.select(["scoutId","geo_plz"])
    table_KT_stations_with_zip = table_stations_with_zip.select(["PLZ", "Parent"])
    cond = [table_KT_rental_location.geo_plz == table_KT_stations_with_zip.PLZ]
    table_KT_rentals_with_stations = table_KT_rental_location.join(table_KT_stations_with_zip, cond, "left")
    table_KT_rentals_with_stations = table_KT_rentals_with_stations.withColumnRenamed("Parent","Parent_Id")
    table_KT_rentals_with_stations.show(5)
    print(f"\n\ntable_KT_rentals_with_stations\n{table_KT_rentals_with_stations.take(5)}")
    del table_mapping_municipal_to_zip, table_rental_location, table_stations_with_zip


    columns = "PLZ"
    print(f"Total of rows: {table_KT_rentals_with_stations.select('PLZ').count()}")
    print(f"Total of individual zip codes: {table_KT_rentals_with_stations.select('PLZ').distinct().count()}")
    print(f"Total of missing zip codes: {table_KT_rentals_with_stations.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in colums]).take(1)}")

    table_KT_rentals_with_stations.show(5)
    columns = table_KT_rentals_with_stations.columns
    helper.extract_to_table(base_table=table_KT_rentals_with_stations, columns=columns,
                            table_name="table_KT_rentals_with_stations", output_path=output_path)

    columns = "PLZ"
    table_KT_rentals_with_stations.select("geo_plz","PLZ", F.col(columns).isNull() ).show()  # OK
    table_KT_rentals_with_stations.where(F.col(columns).isNull()).select("geo_plz","PLZ").distinct().count() # ok
    table_KT_rentals_with_stations.where(F.col(columns).isNull()).select("scoutId","geo_plz","PLZ").show()
    # -> no matching on PLZ for 2501 values of zipcode -> geocode

    # Save incomplete rows
    table_rental_missing_station_and_zip = table_KT_rentals_with_stations.where(F.col(columns).isNull()).select("scoutId")
    table_name = "table_rental_missing_station_and_zip"
    output_location = os.path.join(output_path, f"{table_name}.parquet")
    table_rental_missing_station_and_zip.write.mode('overwrite').parquet(output_location)
    del table_rental_missing_station_and_zip

    # Save complete rows
    table_KT_rentals_with_stations = table_KT_rentals_with_stations.where(F.col(columns).isNotNull())

    table_name = "table_KT_rentals_with_stations"
    output_location = os.path.join(output_path, f"{table_name}.parquet")
    table_KT_rentals_with_stations.write.mode('overwrite').parquet(output_location)
    helper.logger.debug(f"Exported {table_name} as parquet")


    # Resulting table schema>
    # scountId|DHID|Parent|Name|Latitude|Longitude
    cond = [table_KT_rentals_with_stations.Parent_Id == table_stations_in_municipals.Parent]
    table_ST_rental_with_stations = table_KT_rentals_with_stations\
        .join(table_stations_in_municipals, cond, "left")\
        .select(["scoutId","DHID","Parent","Name","Latitude","Longitude"])
    table_ST_rental_with_stations.show(5)
    del table_KT_rentals_with_stations, table_stations_in_municipals

    # PERSIST to parquet
    table_name = "table_ST_rentals_with_stations"
    output_location = os.path.join(output_path, f"{table_name}.parquet")
    table_ST_rental_with_stations.write.mode('overwrite').parquet(output_location)
    helper.logger.debug(f"Exported {table_name} as parquet")



    #table_ST_rental_with_stations.orderBy("scoutId").show(5)
    #print(f"Total rows in apartments with stations: {table_ST_rental_with_stations.count()}")

    helper.logger.debug("Created data mart")


def step1(spark, input_path):
    """
    Add zipcode to station data, since station data only columns 'Latitude', 'Longitude', 'MunicipalityCode'.

    :param spark:                       Apache Spark session.
    :param input_path:                  Path for Input data.
    :return table_stations_with_zip:    Station data having matched zip code.
    """

    # LOAD preprocessed STATION data
    name = "table_majorstations_in_municipals"
    table_stations_in_municipals = helper.create_df_from_parquet(spark, name, input_path)

    # LOAD preprocessed MAPPING data (municipality to zip code)
    name = "table_mapping_municipal_to_zip"
    table_mapping_municipal_ZIP = helper.create_df_from_parquet(spark, name, input_path)

    # JOIN data of Station and Mapping on municipality code
    cond = [table_stations_in_municipals.MunicipalityCode == table_mapping_municipal_ZIP.AGS]
    table_stations_with_zip = table_stations_in_municipals.join(table_mapping_municipal_ZIP, cond, "left")

    # See: analyze.show_missing_munitipality_code(table_stations_with_zip)

    return table_stations_with_zip


def step2(spark, input_path, output_path, table_stations_with_zip):
    """
    Get Key Table holding joined data of Rental and Station based in some zip code.

    :param spark:                       Apache Spark session.
    :param input_path:                  Path for Input data.
    :param output_path:                 Path for Output data.
    :param table_stations_with_zip:     Rental data having a Station as result of step1.
    :return KT_rental_with_station:     Key Table of Rental matched with Station data represents the IDs of both tables.
    """

    # GET Key Tables that just hold relevant keys (KT = Key Table)
    required_columns = ["PLZ", "Parent"]
    assert set(required_columns).issubset(table_stations_with_zip.columns), f"Missing columns {required_columns} not in table"
    KT_stations_with_zip = table_stations_with_zip.select(required_columns).withColumnRenamed("Parent", "Parent_Id")

    name = "table_rental_location"
    required_columns = ["scoutId", "geo_plz"]
    table_rental_location = helper.create_df_from_parquet(spark, name, input_path)
    KT_rental_location = table_rental_location.select(required_columns)

    # JOIN Rental and Station tables on 'PLZ' (== zip code)
    cond = [KT_rental_location.geo_plz == KT_stations_with_zip.PLZ]
    KT_rental_with_station = KT_rental_location\
        .join(KT_stations_with_zip, cond, "left")

    # EXTRACT Rental WITHOUT a matching Station / zip code. Just save the 'scoutID'
    table_rental_missing_station_and_zip = KT_rental_with_station \
        .where(KT_rental_with_station.PLZ.isNull())\
        .select("scoutId")
    table_name = "table_rental_missing_station_and_zip"
    columns = table_rental_missing_station_and_zip.columns
    helper.extract_to_table(base_table=table_rental_missing_station_and_zip, columns=columns,
                            table_name=table_name, output_path=output_path, show_example=False,
                            single_partition=True)

    # EXTRACT Rental with Station WITH a matching Station / zip code. Save the keys of this matching.
    KT_rental_with_station = KT_rental_with_station\
        .where(KT_rental_with_station.PLZ.isNotNull())\
        .select(["scoutId", "geo_plz", "PLZ", "Parent_Id"])
    table_name = "KT_rental_with_station"
    columns = KT_rental_with_station.columns
    helper.extract_to_table(base_table=KT_rental_with_station, columns=columns,
                            table_name=table_name, output_path=output_path, show_example=False,
                            single_partition=True)

    return KT_rental_with_station


def step3(spark, input_path, output_path, KT_rental_with_station):
    """
    Combine data of Rental location with matched Station.
    Resulting table schema:
    ['scoutId', 'date', 'regio1', 'regio2', 'regio3', 'street', 'streetPlain', 'houseNumber', 'PLZ', 'Parent_Id']

    :param spark:                       Apache Spark session.
    :param input_path:                  Path for Input data.
    :param output_path:                 Path for Output data.
    :param table_stations_with_zip:     Rental data having a Station as result of step1.
    :return KT_rental_with_station:     Key Table of Rental matched with Station data represents the IDs of both tables.
    """

    # required: KT_rental_with_station, table_stations_in_municipals
    required_columns = ["PLZ", "Parent_Id", "scoutId"]
    assert set(required_columns).issubset(KT_rental_with_station.columns), f"Missing columns {required_columns} not in table"
    KT_rental_with_station = KT_rental_with_station.withColumnRenamed("scoutId", "scoutId_2")

    name = "table_rental_location"
    table_rental_location = helper.create_df_from_parquet(spark, name, input_path)

    cond = [table_rental_location.scoutId == KT_rental_with_station.scoutId_2]
    table_rental_location_with_station = table_rental_location \
        .join(KT_rental_with_station, cond, "right") \
        .drop(*["geo_bln","geo_krs","geo_plz","scoutId_2"])
        #.select(["scoutId", "DHID", "Parent", "Name", "Latitude", "Longitude"])
    table_rental_location_with_station.show(5)

    # PERSIST to parquet
    table_name = "table_rental_location_with_station"
    columns = table_rental_location_with_station.columns
    helper.extract_to_table(base_table=table_rental_location_with_station, columns=columns,
                            table_name=table_name, output_path=output_path, show_example=False)

    return
