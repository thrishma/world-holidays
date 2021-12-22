import logging
import os
import boto3
import json

LOGGER = logging.getLogger()

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

class S3Handler:
    DOWNLOAD_PATH = '/tmp'

    def __init__(self, record=None, bucket_name=S3_BUCKET_NAME, key_name=None, profile_name=None, config=None):
        self.s3 = None
        self.s3_resource = None
        self.object = None
        self.part_list = None
        self.part_object = None
        self.config = config
        self.session = boto3.session.Session(profile_name=profile_name)
        if record is not None:
            self.bucket = record['s3']['bucket']
            self.bucket_name = record['s3']['bucket']['name']
            self.bucket_key = record['s3']['object']['key']
        elif bucket_name is not None:
            self.bucket_name = bucket_name
            self.bucket_key = key_name
    
    def put_object(self, data=b''):
        self.load_s3_Object()
        self.object.put(Body=data)
    
    def get_s3_object(self):
        self.load_s3_Object()
        return self.object


def create_file_in_s3(filename, bucket_key, data):
    # create a new file in the given path
    s3_handler = S3Handler(bucket_name=S3_BUCKET_NAME, key_name=bucket_key)
    s3_handler.put_object(data)
    LOGGER.info(f"File {filename} created in S3. Path: {bucket_key}")

def read_file_in_s3(bucket_key):
    # read a file from the given folder path
    s3_handler = S3Handler(bucket_name=S3_BUCKET_NAME, key_name=bucket_key)
    s3_object = s3_handler.get_s3_object().get()
    data = s3_object['Body'].read().decode('utf-8')
    LOGGER.info(f"Read {bucket_key}")
    return json.loads(data)