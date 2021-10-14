import configparser
import logging
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

fh = logging.FileHandler('../event.log')
fh.setLevel(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.debug("Initialized logger for App")


def get_config():
    # GET Access settings
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    return config


def create_spark_session():
    """
    Create or get a Spark session.

    :return: SparkSession
    """

    config = get_config()

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    # GET Spark Session
    # https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.0/hadoop-aws-2.7.0.jar
    # https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()
    logger.debug("Received Spark Instance")

    # PROVIDE S3 access settings to Spark
    logger.debug("Provided S3 access credentials")

    return spark


def get_schemas(name: str):
    """
    Return structure data / schema applied to the data.

    :param name: name if requested schema: SONG or LOG
    :return: requested schema.
    """

    if name.upper() == 'RENTAL':
        RENTAL_SCHEMA = T.StructType([
             T.StructField("regio1", T.StringType(), True),
             T.StructField("serviceCharge", T.DoubleType(), True),
             T.StructField("heatingType", T.StringType(), True),
             T.StructField("telekomTvOffer", T.StringType(), True),
             T.StructField("telekomHybridUploadSpeed", T.StringType(), True),
             T.StructField("newlyConst", T.BooleanType(), True),
             T.StructField("balcony", T.BooleanType(), True),
             T.StructField("picturecount", T.IntegerType(), True),
             T.StructField("pricetrend", T.DoubleType(), True),
             T.StructField("telekomUploadSpeed", T.StringType(), True),
             T.StructField("totalRent", T.DoubleType(), True),
             T.StructField("yearConstructed", T.DoubleType(), True),
             T.StructField("scoutId", T.IntegerType(), True),
             T.StructField("noParkSpaces", T.StringType(), True),
             T.StructField("firingTypes", T.StringType(), True),
             T.StructField("hasKitchen", T.BooleanType(), True),
             T.StructField("geo_bln", T.StringType(), True),
             T.StructField("cellar", T.BooleanType(), True),
             T.StructField("yearConstructedRange", T.DoubleType(), True),
             T.StructField("baseRent", T.DoubleType(), True),
             T.StructField("houseNumber", T.StringType(), True),
             T.StructField("livingSpace", T.StringType(), True),
             T.StructField("geo_krs", T.StringType(), True),
             T.StructField("condition", T.StringType(), True),
             T.StructField("interiorQual", T.StringType(), True),
             T.StructField("petsAllowed", T.StringType(), True),
             T.StructField("street", T.StringType(), True),
             T.StructField("streetPlain", T.StringType(), True),
             T.StructField("lift", T.BooleanType(), True),
             T.StructField("baseRentRange", T.IntegerType(), True),
             T.StructField("typeOfFlat", T.StringType(), True),
             T.StructField("geo_plz", T.StringType(), True),
             T.StructField("noRooms", T.StringType(), True),
             T.StructField("thermalChar", T.StringType(), True),
             T.StructField("floor", T.StringType(), True),
             T.StructField("numberOfFloors", T.DoubleType(), True),
             T.StructField("noRoomsRange", T.IntegerType(), True),
             T.StructField("garden", T.BooleanType(), True),
             T.StructField("livingSpaceRange", T.IntegerType(), True),
             T.StructField("regio2", T.StringType(), True),
             T.StructField("regio3", T.StringType(), True),
             # T.StructField("description", T.StringType(), True),  # Dropped this field
             # T.StructField("facilities", T.StringType(), True),  # Dropped this field
             T.StructField("heatingCosts", T.DoubleType(), True),
             T.StructField("energyEfficiencyClass", T.StringType(), True),
             T.StructField("lastRefurbish", T.DoubleType(), True),
             T.StructField("electricityBasePrice", T.DoubleType(), True),
             T.StructField("electricityKwhPrice", T.DoubleType(), True),
             T.StructField("date", T.StringType(), True),
        ])

        return RENTAL_SCHEMA

    if name.upper() == 'STATION':
        STATION_SCHEMA = T.StructType([
            T.StructField("SeqNo", T.IntegerType(), True),
            T.StructField("Type", T.StringType(), True),
            T.StructField("DHID", T.StringType(), True),
            T.StructField("Parent", T.StringType(), True),
            T.StructField("Name", T.StringType(), True),
            T.StructField("Latitude", T.StringType(), True),
            T.StructField("Longitude", T.StringType(), True),
            T.StructField("MunicipalityCode", T.StringType(), True),
            T.StructField("Municipality", T.StringType(), True),
            T.StructField("DistrictCode", T.StringType(), True),
            T.StructField("District", T.StringType(), True),
            T.StructField("Condition", T.StringType(), True),
            T.StructField("State", T.StringType(), True),
            T.StructField("Description", T.StringType(), True),
            T.StructField("Authority", T.StringType(), True),
            T.StructField("DelfiName", T.StringType(), True),
            T.StructField("TariffDHID", T.StringType(), True),
            T.StructField("TariffName", T.StringType(), True),
        ])

        return STATION_SCHEMA

    raise ValueError(f"Found no schema with name {name}.")
    return


def extract_to_table(base_table, columns, table_name, output_path):
    """
    Extract from a base table a given set of columns to parquet.

    :para   base_table: Spark Dataframe holding data.
    :para   columns: Columns to extraxt from Dataframe
    :para   table_name: Name of the extract
    :para   output_path: location where to save. The filename of the parquet file is exclusive.

    """

    try:

        # EXTRACT columns to create table
        assert set(columns) == set(base_table.columns), f"Columns are not identical for table {table_name}"

        table = base_table\
            .select(columns)\
            .drop_duplicates()\
            .dropna(subset='scoutId')

        # SHOW examples
        print(f"\nShowing sample of {table_name}:")
        table.show(10, truncate=False)

        # PERSIST to parquet
        output_location = os.path.join(output_path, f"{table_name}.parquet")
        table.write.mode('overwrite').parquet(output_location)
        logger.debug(f"Exported {table_name} as parquet")

    except Exception as ex:
        logger.error(f"Error writing {table_name} tp parquet. Reason: {ex}")

    pass
    return
