from __future__ import print_function

import cPickle
import csv
import datetime
# import tempfile
import time
import boto3
import pytz
# import matplotlib.image as mpimg
from StringIO import StringIO

frameCount = 0
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
    # s3 = boto3.resource('s3', region_name='us-east-1')
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    image = response[u'Body'].read()
    return image


def push_data_to_kinesis_stream(data, kinesis_stream, delay):
    """
    push_data_to_kinesis_stream takes in a single line from a CSV, a kinesis stream, and the desire delay.
    Currently it just prints the data, then sleeps for the specified delay in seconds.
    #TODO add code to push to kinesis here.
    """
    print(data)
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
        print(e)


def parse_file(data_buffer, kinesis_stream, delay=1):
    """
    parse_file takes in a data_buffer or StringBuffer of the file.
    It then parses the string as a full object into specific lines.
    The lines are handed over to push_data_to_kinesis_stream.
    """
    reader = csv.reader(buffer)
    for line in reader:
        push_data_to_kinesis_stream(data=line, kinesis_stream=kinesis_stream, delay=delay)


def handler(event, context):
    # live_data_buffer = get_file_from_s3(s3_bucket=event['s3_bucket'], s3_key=event['s3_key'])
    # parse_file(data_buffer=live_data_buffer, kinesis_stream=event['kinesis_stream'],
    #            delay=int(event['default_throttling']))
    print('in s3imagegetter.handler function')
    # retrieve bucket name and file_key from the S3 event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']
    print('Reading {} from {}'.format(s3_key, s3_bucket))

    global frameCount
    frameCount = frameCount + 1
    img_buffer = get_image_from_s3(s3_bucket, s3_key)
    print("img_buffer = {}".format(img_buffer))

    push_image_to_kinesis_stream(img_buffer, "FrameStream", frameCount)
