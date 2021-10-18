# AWS cli / Boto / MINIO
import boto3
import os

import model.helper as helper

# TEST login
# GET S3 access settings
ACCESS_ID = helper.get_config_or_default('AWS', 'ACCESS_ID')
ACCESS_SECRET = helper.get_config_or_default('AWS', 'ACCESS_SECRET')
S3A_ENDPOINT = helper.get_config_or_default('AWS', 'S3A_ENDPOINT')
BUCKET_NAME = helper.get_config_or_default('AWS', 'BUCKET_NAME')
DIRECTORY = helper.get_config_or_default('AWS', 'DIRECTORY')
TEST_FILE = helper.get_config_or_default('AWS', 'TEST_FILE')

assert type(ACCESS_ID) == str \
       and type(ACCESS_SECRET) == str \
       and type(S3A_ENDPOINT) == str \
       and type(BUCKET_NAME) == str \
       and type(DIRECTORY) == str \
       and type(TEST_FILE) == str, \
    f"Error providing AWS S3a access keys as environment variable"

# SET connection

session = boto3.Session(
    aws_access_key_id=ACCESS_ID,
    aws_secret_access_key=ACCESS_SECRET,
)
s3 = session.resource('s3')

# Filename - File to upload
# Bucket - Bucket to upload to (the top level directory under AWS S3)
# Key - S3 object name (can contain subdirectories). If not specified then file_name is used

# SETUP generic function to copy
data_location = r"C:/Python/_Working/DatEng_Capstone/event.log"
os.path.exists(data_location)
s3_key_path = DIRECTORY + "2."+TEST_FILE
bucket_name = BUCKET_NAME.replace("s3a://", "")
s3.meta.client.upload_file(Filename=TEST_FILE, Bucket=bucket_name, Key=s3_key_path)


with open(data_location, "rb") as file:
    s3.meta.client.upload_fileobj(file, bucket_name, s3_key_path)
    s3.Object(bucket_name, s3_key_path).wait_until_exists()
    file.close()

