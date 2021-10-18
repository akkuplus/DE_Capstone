import configparser
import logging
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

data_location = os.getcwd()
os.path.split(data_location)
data_location = 'event.log'
fh = logging.FileHandler(data_location)
fh.setLevel(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.debug("\nInitialized logger for App")


def _get_config_reader():
    # GET access settings
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    return config


def get_config_or_default(section, option, default=None):
    config = _get_config_reader()

    return config.get(section, option) if config.has_section(section) and config.has_option(section, option) else default


def create_spark_session():
    """
    Create or get a Spark session.

    :return: SparkSession
    """


    # GET Spark Session
    # https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.0/hadoop-aws-2.7.0.jar
    # https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
    SPARK_JARS = get_config_or_default('AWS', 'SPARK_JARS')

    assert SPARK_JARS, f"Error reading Jar-files for Spark {SPARK_JARS}"

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", SPARK_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.debug("Initialized Spark instance")

    assert test_s3_access(spark), "Error writing parquet testfile to S3a"

    return spark


def test_s3_access(spark):
    """
    Test provide configuration data and test write acces to S3a using a Spark session.

    :param spark: SparkSession
    """

    # GET S3 access settings
    ACCESS_ID = get_config_or_default('AWS', 'ACCESS_ID')
    ACCESS_SECRET = get_config_or_default('AWS', 'ACCESS_SECRET')
    S3A_ENDPOINT = get_config_or_default('AWS', 'S3A_ENDPOINT')
    BUCKET_NAME = get_config_or_default('AWS', 'BUCKET_NAME')
    DIRECTORY = get_config_or_default('AWS', 'DIRECTORY')
    TEST_FILE = get_config_or_default('AWS', 'TEST_FILE')

    assert type(ACCESS_ID) == str \
        and type(ACCESS_SECRET) == str \
        and type(S3A_ENDPOINT) == str \
        and type(BUCKET_NAME) == str \
        and type(DIRECTORY) == str \
        and type(TEST_FILE) == str, \
        f"Error providing AWS S3a access keys as environment variable"

    # SET Spark configuration to access S3a
    # See https://stackoverflow.com/questions/55119337/read-files-from-s3-pyspark
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_ID)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", ACCESS_SECRET)
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", S3A_ENDPOINT)
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                         "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")

    # TEST read-write access
    try:
        data, columns = [(1, 1), (2, 2)], ["col1", "col2"]
        data_location = f"{BUCKET_NAME}/{DIRECTORY}/{TEST_FILE}"
        logger.debug(f"S3 location is {data_location}")

        # Requirements for reading from / writing to S3:
        # "HADOOP_HOME" as environment variable pointing to directory "Hadoop" Within, find <HADOOP_HOME>/bin: hadoop.dll
        # and winutils.exe. "%HADOOP_HOME%\bin" is part of path-variable und hadoop.dll is in /windows/system32/
        # See: https://sparkbyexamples.com/spark/spark-hadoop-exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io-nativeio-nativeiowindows-access0ljava-lang-stringiz/
        # See: https://stackoverflow.com/questions/41851066/exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io
        spark.createDataFrame(data=data, schema=columns).write.mode("overwrite").parquet(data_location)
        spark.read.parquet(data_location).show()
    except Exception as ex:
        logger.error(f"Error writing parquet testfile to S3 address {data_location}. Reason {ex}")
        raise
    logger.debug("Sucessfully wrote testfile to S3")

    return True


def get_schemas(name: str):
    """
    Return structure data / schema applied to the data.

    :param name: name if requested schema: SONG or LOG
    :return: requested schema.
    """

    if name.upper() == 'MAPPING':
        MAPPING_Municipal_Zip_SCHEMA = T.StructType([
            T.StructField("AGS", T.StringType(), True),
            T.StructField("Bezeichnung", T.StringType(), True),
            T.StructField("PLZ", T.StringType(), True)
        ])

        return MAPPING_Municipal_Zip_SCHEMA

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


def extract_to_table(base_table, columns, table_name, output_path, show_Example=True):
    """
    Extract from a base table a given set of columns to parquet.

    :para   base_table: Spark Dataframe holding data.
    :para   columns: Columns to extraxt from Dataframe
    :para   table_name: Name of the extract
    :para   output_path: location where to save. The filename of the parquet file is exclusive.

    """

    try:

        # EXTRACT columns to create table
        assert set(columns).issubset(set(base_table.columns)), f"Not all columns in table {table_name}. "

        table = base_table\
            .select(columns)\
            .drop_duplicates()

        # SHOW examples
        if show_Example:
            print(f"\nShowing sample of {table_name}:")
            table.show(10, truncate=False)

        # PERSIST to parquet
        output_location = os.path.join(output_path, f"{table_name}.parquet")
        table.write.mode('overwrite').parquet(output_location)
        logger.debug(f"Exported {table_name} as parquet")

    except Exception as ex:
        logger.error(f"Error writing {table_name} to parquet. Reason:\n{ex}")


    return
