"""
s3lambda.py takes in a few arguments to a main function
then leverages this info to do the following:
1. Obtain a CSV file from S3
2. Parse the CSV file
3. Hand the parsed lines into a Kinesis stream
Additionally modifying the timeout is supported.
Your event should have the following items:
{
  "s3_bucket": "BUCKET",
  "s3_key": "KEY_TO_YOUR_S3_ASSET",
  "kinesis_stream": "PATH_TO_KINESIS_STREAM",
  "default_throttling": "1"
}
The default_throttling is a delay for inserting items into the 
stream in seconds.
Keep in mind lambda functions have a maximum lifespan of 5 minutes, so your delays should be small.
"""
__author__ = "Upender K. Nimbekar"
__contact__ = "upender.kumar@gmail.com"

# Global Imports
# from __future__ import print_function

import boto3
import cPickle
import csv
import datetime
import pytz
import tempfile
import time
from StringIO import StringIO
import matplotlib.image as mpimg

kinesis_client = boto3.client("kinesis")

# Actual Code
def get_file_from_s3(s3_bucket, s3_key):
    """
    get_file_from_S3 will create an S3 client using boto first.
    Next it fetches your object from the specified bucket and converts the string
    into a StringBuffer ( to be treated like a file object.)
    The buffer is then returned.
    """
    s3 = boto3.resource('s3')
    obj = s3.Object(s3_bucket, s3_key)
    data = obj.get()['Body'].read()
    buffer = StringIO(data)
    return buffer


def get_image_from_s3(s3_bucket, s3_key):
    """
    get_file_from_S3 will create an S3 client using boto first.
    Next it fetches your object from the specified bucket and converts the string
    into a StringBuffer ( to be treated like a file object.)
    The buffer is then returned.
    """
    s3 = boto3.resource('s3', region_name='us-east-1')
    bucket = s3.Bucket(s3_bucket)
    image = bucket.Object(s3_key)
    tmp = tempfile.NamedTemporaryFile()

    img = ''
    with open(tmp.name, 'wb') as f:
        image.download_fileobj(f)
        img = mpimg.imread(tmp.name)

    return img


def push_data_to_kinesis_stream(data, kinesis_stream, delay):
    """
    push_data_to_kinesis_stream takes in a single line from a CSV, a kinesis stream, and the desire delay.
    Currently it just prints the data, then sleeps for the specified delay in seconds.
    #TODO add code to push to kinesis here.
    """
    print data
    time.sleep(delay)


def push_image_to_kinesis_stream(buff, kinesis_stream, frame_count, enable_kinesis=True,
                                 write_file=False, delay=1):
    try:
        img_bytes = bytearray(buff)
        utc_dt = pytz.utc.localize(datetime.datetime.now())
        now_ts_utc = (utc_dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()
        frame_package = {
            'ApproximateCaptureTime': now_ts_utc,
            'FrameCount': frame_count,
            'ImageBytes': img_bytes
        }

        if write_file:
            print("Writing file img_{}.jpg".format(frame_count))
            target = open("img_{}.jpg".format(frame_count), 'w')
            target.write(img_bytes)
            target.close()

        # put encoded image in kinesis stream
        if enable_kinesis:
            print("Sending image to Kinesis")
            response = kinesis_client.put_record(
                StreamName=kinesis_stream,
                Data=cPickle.dumps(frame_package),
                PartitionKey="partitionkey"
            )
            print(response)

        time.sleep(delay)

    except Exception as e:
        print e


def parse_file(data_buffer, kinesis_stream, delay=1):
    """
    parse_file takes in a data_buffer or StringBuffer of the file.
    It then parses the string as a full object into specific lines.
    The lines are handed over to push_data_to_kinesis_stream.
    """
    reader = csv.reader(buffer)
    for line in reader:
        push_data_to_kinesis_stream(data=line, kinesis_stream=kinesis_stream, delay=delay)


def s3_hanlder(event, context):
    # live_data_buffer = get_file_from_s3(s3_bucket=event['s3_bucket'], s3_key=event['s3_key'])
    # parse_file(data_buffer=live_data_buffer, kinesis_stream=event['kinesis_stream'],
    #            delay=int(event['default_throttling']))
    img_buffer = get_image_from_s3(s3_bucket=event['s3_bucket'], s3_key=event['s3_key'])
    push_image_to_kinesis_stream(img_buffer, "FrameStream", 1)
