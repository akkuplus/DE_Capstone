import os
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import StructType, StructField, StringType

import model.helper as helper


def main():
    spark = helper.create_spark_session()
    helper.logger.debug("Created Spark session")

    # SET input and output path
    input_path = os.path.join(os.getcwd(), "data")
    helper.logger.debug(f"Set input_data to {input_path}")
    output_path = os.path.join(os.getcwd(), "data")
    helper.logger.debug(f"Set output_data to {output_path}")


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
    # match via table_majorstations_in_municipals(MunicipalityCode) -> zipcode -> table_rental_location(geo_plz)

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
    print("Missing values of zip code in stations")
    print(f"Total of rows:{table_stations_with_zip.select(colums).count()}")
    print(f"Total rows missing a zipcode:{table_stations_with_zip.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in colums]).collect()}")
    # -> we have 971 missing PLZ -> continue with most stations

    # JOIN Rentals with Stations on zip code
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

