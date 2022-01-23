import configparser
import s3fs
import logging
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import re
import requests
import sys

# INIT Logger
data_location = 'event_and_error.log'
# See https://gist.github.com/wassname/d17325f36c36fa663dd7de3c09a55e74
formatter = logging.Formatter('[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
file_handler = logging.FileHandler(filename=data_location)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format='[{%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=[
        file_handler,
        stream_handler
    ]
)

# Pyspark logger: see https://stackoverflow.com/questions/34248908/how-to-prevent-logging-of-pyspark-answer-received-and-command-to-send-messag
logging.getLogger("py4j").setLevel(logging.INFO)
logging.getLogger('pyspark').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.info("\nInitialized logger")


def _get_config_reader():
    # GET access settings
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    return config


def get_config_or_default(section, option, default=None):
    config = _get_config_reader()

    return config.get(section, option) if config.has_section(section) and config.has_option(section, option) else default


def get_setup(do_test_s3_access=False, default_spark_log_level="ERROR"):
    """
    Return Spark sessions, and get path for input and output data as in config file dl.cfg.
    """

    spark = create_spark_session(do_test_s3_access=do_test_s3_access, default_spark_log_level=default_spark_log_level)

    # SET input and output path
    input_path = get_config_or_default("S3", "INPUT_PATH")
    assert input_path, "Input_Path is not set"
    logger.info(f"Set input_data_path to {input_path}")

    output_path = get_config_or_default("S3", "OUTPUT_PATH")
    assert input_path, "Output_Path is not set"
    logger.info(f"Set output_data_path to {output_path}")

    return spark, input_path, output_path


def create_spark_session(do_test_s3_access=False, default_spark_log_level="ERROR"):
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
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
        .getOrCreate()

    spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    logger.info("Initialized Spark instance")

    # GET S3 access settings
    ACCESS_ID = get_config_or_default('S3', 'AWS_ACCESS_KEY_ID')
    ACCESS_SECRET = get_config_or_default('S3', 'AWS_SECRET_ACCESS_KEY')
    OUTPUT_PATH = get_config_or_default('S3', 'OUTPUT_PATH')

    assert bool(ACCESS_ID) \
        and bool(ACCESS_SECRET) \
        and bool(OUTPUT_PATH), \
        f"Error reading AWS S3a access keys. A key is missing."

    # SET Spark configuration to access S3a
    # See https://stackoverflow.com/questions/55119337/read-files-from-s3-pyspark
    # https://stackoverflow.com/questions/34209196/amazon-s3a-returns-400-bad-request-with-spark
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_ID)
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_ID)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", ACCESS_SECRET)
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", ACCESS_SECRET)

    # S3A_ENDPOINT = get_config_or_default('S3', 'S3A_ENDPOINT')
    #
    # OUTPUT_PATH = get_config_or_default('S3', 'OUTPUT_PATH')
    #spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", S3A_ENDPOINT)
    #spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    #spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
    #                                     "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")

    if do_test_s3_access:
        assert test_s3_access(spark), "Error writing parquet testfile to S3"

    return spark


def check_photon_service(url="http://localhost:2322/api"):
    # TEST connection to Photon:
    logger.info(f"Querying Photon geocode service...")
    try:
        querystring = {"q": "Brandenburger Tor, Pariser Platz, 10117 Berlin", "lang": "de", "limit": "1"}
        response = requests.request("GET", url, params=querystring)
        assert '"osm_id":518071791' in response.text, f"Can't find expected OSM ID for {url}"
        coords = geocode_coordinates(querystring['q'], url=url)
        logger.info(f"Successfully queried Photon geocode service. Brandenburger Tor in Berlin is at {coords}.")
    except requests.exceptions as ex:
        logger.error(f"Error testing Photon geocoding service. Reason: {ex}")
        return False

    return True


def test_s3_access(spark, OUTPUT_PATH=None):
    """
    Test write and read access to S3a using a Spark session.

    :param spark: SparkSession
    """

    # TEST read-write access
    try:
        OUTPUT_PATH = get_config_or_default('S3', 'OUTPUT_PATH')
        assert s3_object_exists, f"Object {OUTPUT_PATH} does not exists."

        data, columns = [(1, 1)], ["col1", "col2"]
        data_location = f"{OUTPUT_PATH}/test.parquet"
        logger.info(f"Testing access to S3 location {data_location}")

        # Requirements for reading from / writing to S3 using Win10:
        # "HADOOP_HOME" as environment variable points to directory "Hadoop" Within, find <HADOOP_HOME>/bin: hadoop.dll
        # and winutils.exe. "%HADOOP_HOME%\bin" is part of path environment variable
        # and hadoop.dll is in /windows/system32/
        # See: https://sparkbyexamples.com/spark/spark-hadoop-exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io-nativeio-nativeiowindows-access0ljava-lang-stringiz/
        # See: https://stackoverflow.com/questions/41851066/exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io

        spark.createDataFrame(data=data, schema=columns).write.mode("overwrite").parquet(data_location)
        spark.read.parquet(data_location).show()
    except Exception as ex:
        logger.error(f"Error accessing S3 address {data_location}. Reason {ex}")
        raise
    logger.info(f"Successfully read from and wrote to S3 location {data_location}")

    return True


def get_schemas(name: str):
    """
    Return structure data / schema applied to the data.

    :param name: name if requested schema: SONG or LOG
    :return: requested schema.
    """

    if name.upper() == 'MAPPING_MUNICIPAL_ZIP':
        SCHEMA = T.StructType([
            T.StructField("AGS", T.StringType(), True),
            T.StructField("Bezeichnung", T.StringType(), True),
            T.StructField("PLZ", T.StringType(), True)
        ])

        return SCHEMA

    elif name.upper() == 'MAPPING_ZIP_COOR':
        SCHEMA = T.StructType([
            T.StructField("PLZ", T.StringType(), True),
            T.StructField("lat", T.DoubleType(), True),
            T.StructField("lng", T.DoubleType(), True)
        ])

        return SCHEMA

    elif name.upper() == 'RENTAL':
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

    elif name.upper() == 'STATION':
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

    else:
        raise ValueError(f"Found no schema with name {name}.")

    return


def s3_object_exists(input_location):

    if input_location[:2].upper() == "S3":

        logger.debug(f"Checking Existence of S3 object...")
        # GET S3 access settings
        ACCESS_ID = get_config_or_default('S3', 'AWS_ACCESS_KEY_ID')
        ACCESS_SECRET = get_config_or_default('S3', 'AWS_SECRET_ACCESS_KEY')
        OUTPUT_PATH = get_config_or_default('S3', 'OUTPUT_PATH')

        assert bool(ACCESS_ID) \
               and bool(ACCESS_SECRET) \
               and bool(OUTPUT_PATH), \
            f"Error reading AWS S3a access keys. A key is missing."

        s3_fs = s3fs.S3FileSystem(key=ACCESS_ID, secret=ACCESS_SECRET)
        return True if s3_fs.exists(input_location) else False

    if input_location[:2].upper() != "S3":
        logger.debug(f"Checking Existence of local object..")
        return True if os.path.exists(input_location) else False


def get_s3_object(input_location):

    # GET S3 access settings
    ACCESS_ID = get_config_or_default('S3', 'AWS_ACCESS_KEY_ID')
    ACCESS_SECRET = get_config_or_default('S3', 'AWS_SECRET_ACCESS_KEY')
    OUTPUT_PATH = get_config_or_default('S3', 'OUTPUT_PATH')

    assert bool(ACCESS_ID) \
        and bool(ACCESS_SECRET) \
        and bool(OUTPUT_PATH), \
        f"Error reading AWS S3a access keys. A key is missing."

    s3_fs = s3fs.S3FileSystem(key=ACCESS_ID, secret=ACCESS_SECRET)
    assert s3_object_exists(input_location), f"Not existent object {input_location}"
    return s3_fs.open(input_location)


def extract_to_table(base_table,
                     columns,
                     table_name,
                     output_path,
                     show_example=False,
                     single_partition=False,
                     partition_name=None
                     ):
    """
    Extract from a base table a given set of columns to parquet.

    :para   base_table: Spark Dataframe holding data.
    :para   columns: Columns to extraxt from Dataframe
    :para   table_name: Name of the extract
    :para   output_path: location where to save. The filename of the parquet file is exclusive.
    :para   single_partition: repartitions dataframe to just a single partitions (to avoids small partitioned data)
    """

    try:

        # EXTRACT columns to create table
        assert set(columns).issubset(set(base_table.columns)), f"Not all columns in table {table_name}. "

        base_table = base_table\
            .select(columns)\
            .drop_duplicates()

        # SHOW examples
        if show_example:
            print(f"\nShowing sample of {table_name}:")
            base_table.show(10, truncate=False)

        # PERSIST to parquet
        output_location = os.path.join(output_path, f"{table_name}.parquet")

        if single_partition and not partition_name:
            # See https://mungingdata.com/apache-spark/output-one-file-csv-parquet/ or
            # https://kontext.tech/column/spark/296/data-partitioning-in-spark-pyspark-in-depth-walkthrough
            logger.info(f"Repartitioning dataframe that has {base_table.rdd.getNumPartitions()} partition(s)")
            base_table = base_table.repartition(1)
            logger.info(f"Successfully repartitioned dataframe to {base_table.rdd.getNumPartitions()} partition")
            logger.info(f"Writing {table_name} to {output_location} ...")
            base_table.write.mode('overwrite').parquet(output_location)

        if (partition_name) and (partition_name in base_table.columns):
            logger.info(f"Repartitioned dataframe by {partition_name} as partition")
            logger.info(f"Writing {table_name} to {output_location} ...")
            base_table.write.mode('overwrite').partitionBy(partition_name).parquet(output_location)

        assert s3_object_exists(output_location), f"Error writing. Object does not exists ({output_location})"
        logger.info(f"Successfully wrote {table_name} to {output_location}")
        base_table.unpersist()
        del base_table

    except Exception as ex:
        logger.error(f"Error writing {table_name} to parquet. Reason:\n{ex}")

    return


def create_df_from_parquet(spark, name, input_path):
    try:
        if name.endswith(".parquet/"):
            name = name[:-1]
        if not name.endswith(".parquet"):
            name = f"{name}.parquet"

        data_location = os.path.join(input_path, name)
        data_location = data_location.replace(r"\\",r"\\")
        logger.debug(f"Data Location is {data_location}")

        if bool(re.match("[a-zA-Z]{1}3", data_location[:2])) :
            logger.debug("Data Location has S3 address")
            assert s3_object_exists(data_location), f"Not existing object {data_location}"

        """
        else re.match("[a-zA-Z]{1}\:", data_location[:2]):

            logger.debug("Data Location has local address")
            assert os.path.exists(data_location), f"Error reading path {data_location}"
        """

        df = spark.read.parquet(data_location)
        logger.info(f"Successfully imported {name} to Spark dataframe. Sourced object imported.")
    except Exception as ex:
        logger.error(f"Error creating dataframe {name}. Reason: {ex}")
        df = None

    return df


def geocode_coordinates(address_string, url):
    try:
        logger.debug(f"Querying {address_string}")
        # querystring = {"q": "Brandenburger Tor, Pariser Platz, 10117 Berlin", "lang": "de", "limit": "1"}
        querystring = {"q": address_string, "lang": "de", "limit": "1"}
        response = requests.request("GET", url, params=querystring)
        if len(response.json()["features"]) > 0:
            coords = response.json()["features"][0]["geometry"]["coordinates"]
        else:
            coords = (float('nan'), float('nan'))
        logger.info(f"Queried {address_string}.")
    except Exception as ex:
        logger.error(f"Error querying {address_string}. Reason: {ex}. Response: {response}")
        coords = float('nan'), float('nan')

    return coords[0], coords[1]


def geocode_zipcodes(lat, lon, url="http://localhost:2322/reverse"):
    try:
        logger.debug(f"Querying {lat}, {lon} ...")
        querystring = {"lat": lat, "lon": lon}  # /reverse?lon=10&lat=52
        queryurl = f"{url}?lon={lon}&lat={lat}"
        response = requests.request("GET", url, params=querystring)
        if len(response.json()["features"]) > 0:
            zipcode = response.json()["features"][0]["properties"]["postcode"]
        else:
            zipcode = None

        logger.info(f"Queried {lat}, {lon}")
    except Exception as ex:
        logger.error(f"Error querying {queryurl}. Reason: {ex}.")
        zipcode = ""

    return zipcode


def combine(col1, col2):
    if col1 is not None:
        return col1
    else:
        return col2