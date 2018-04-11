from __future__ import print_function
import base64
import datetime
import time
import decimal
import uuid
import json
import cPickle
import boto3
import pytz
from pytz import timezone


def load_config():
    '''Load configuration from file.'''
    with open('imageprocessor-params.json', 'r') as conf_file:
        conf_json = conf_file.read()
        return json.loads(conf_json)

def convert_ts(ts, config):
    '''Converts a timestamp to the configured timezone. Returns a localized datetime object.'''
    #lambda_tz = timezone('US/Pacific')
    tz = timezone(config['timezone'])
    utc = pytz.utc

    utc_dt = utc.localize(datetime.datetime.utcfromtimestamp(ts))

    localized_dt = utc_dt.astimezone(tz)

    return localized_dt


def process_image(event, context):

    #Initialize clients
    rekog_client = boto3.client('rekognition')
    sns_client = boto3.client('sns')
    s3_client = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')

    #Load config
    config = load_config()

    s3_bucket = config["s3_bucket"]
    s3_key_frames_root = config["s3_key_frames_root"]

    ddb_table = dynamodb.Table(config["ddb_table"])

    rekog_max_labels = config["rekog_max_labels"]
    rekog_min_conf = float(config["rekog_min_conf"])

    label_watch_list = config["label_watch_list"]
    label_watch_min_conf = float(config["label_watch_min_conf"])
    label_watch_phone_num = config["label_watch_phone_num"]

    count = 0
    print('Total Records to process from Kinesis = {}'.format(len(event['Records'])))

    #Iterate on frames fetched from Kinesis
    for record in event['Records']:
        count = count + 1
        frame_package_b64 = record['kinesis']['data']
        frame_package = cPickle.loads(base64.b64decode(frame_package_b64))

        print('Frame# {}'.format(count))
        img_bytes = frame_package["ImageBytes"]
        print('Image bytes size = {}'.format(len(img_bytes)))
        approx_capture_ts = frame_package["ApproximateCaptureTime"]
        frame_count = frame_package["FrameCount"]

        now_ts = time.time()

        frame_id = str(uuid.uuid4())
        processed_timestamp = decimal.Decimal(now_ts)
        approx_capture_timestamp = decimal.Decimal(approx_capture_ts)

        now = convert_ts(now_ts, config)
        year = now.strftime("%Y")
        mon = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")

        rekog_response = rekog_client.detect_labels(
            Image={
                'Bytes': img_bytes
            },
            MaxLabels=rekog_max_labels,
            MinConfidence=rekog_min_conf
        )


        #Iterate on rekognition labels...
        for label in rekog_response['Labels']:
            lbl = label['Name']
            conf = label['Confidence']
            label['OnWatchList'] = False

            #Convert from float to decimal for DynamoDB
            label['Confidence'] = decimal.Decimal(conf)

            #Print labels and confidence to lambda console
            print('{} .. conf %{:.2f}'.format(lbl, conf))

            #Check label watch list and trigger action
            if(label_watch_phone_num
                and lbl.upper() in (label.upper() for label in label_watch_list)
                and conf >= label_watch_min_conf
                and lbl.upper() == 'HUMAN'):

                person_found = person_of_interest_finder(rekog_client, img_bytes, config)
                print('Returned value of Person = {}'.format(person_found))

                if person_found:
                    label['OnWatchList'] = True
                    notification_txt = 'On {}, {} named {} was detected with %{} confidence.'.format(
                        now.strftime('%x %X %Z'),
                        lbl,
                        person_found,
                        round(conf,2))

                    print(notification_txt)

                    #Send SNS notification
                    #sns_client.publish(PhoneNumber=label_watch_phone_num, Message=notification_txt)
                    sns_client.publish(TopicArn='arn:aws:sns:us-east-1:721045248511:video-frame-alert-topic',
                                        Subject='Found Person {}'.format(person_found),
                                        Message=notification_txt,
                                        MessageAttributes={
                                            'FaceDetected': {
                                                'DataType'    : 'Binary',
                                                'BinaryValue' : img_bytes
                                            }
                                        })
                    break


        #print("rekog_response:\n{}", rekog_response)
        s3_key = (s3_key_frames_root + '{}/{}/{}/{}/{}.jpg').format(year, mon, day, hour, frame_id)

        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=img_bytes
        )

        #print_rekog_labels(rekog_response)

        #Persist frame data in dynamodb
        #Covert Labels Confidence from Flot to Decimal to persist in DynamoDB
        convert_to_decimal(rekog_response)

        item = {
            'frame_id': frame_id,
            'processed_timestamp' : processed_timestamp,
            'approx_capture_timestamp' : approx_capture_timestamp,
            'rekog_labels' : rekog_response['Labels'],
            'rekog_orientation_correction' :
                rekog_response['OrientationCorrection']
                if 'OrientationCorrection' in rekog_response else 'ROTATE_0',
            'processed_year_month' : year + mon, #To be used as a Hash Key for DynamoDB GSI
            's3_bucket' : s3_bucket,
            's3_key' : s3_key
        }

        ddb_table.put_item(Item=item)

    print('Successfully processed {} records.'.format(len(event['Records'])))
    return

def print_rekog_labels(rekog_response):
    print('In fn: print_rekog_labels')
    for label in rekog_response['Labels']:
        print('Label: {}, Confidence: {}, Confidence(in Decimal): {}'.format(label['Name'], label['Confidence'], decimal.Decimal(label['Confidence'])))

def convert_to_decimal(rekog_response):
    print('In fn: convert_to_decimal')
    for label in rekog_response['Labels']:
        label['Confidence'] = decimal.Decimal(label['Confidence'])
        #print('Label: {}, Confidence: {}, Confidence(in Decimal): {}'.format(label['Name'], label['Confidence'], decimal.Decimal(label['Confidence'])))

def person_of_interest_finder(rekog_client, img_bytes, config):
    rekog_search_face_response = rekog_client.search_faces_by_image(
        CollectionId=config['face_collection'],
        Image={
            'Bytes': img_bytes
        },
        MaxFaces=config['search_max_faces'],
        FaceMatchThreshold=config['face_match_threshold']
    )

    print('Total face Matches found = {}'.format(len(rekog_search_face_response['FaceMatches'])))
    person_watch_list = config['person_watch_list']

    if len(rekog_search_face_response['FaceMatches']) > 0:
        print("found the person")
        for facematch in rekog_search_face_response['FaceMatches']:
            imgId = facematch['Face']['ImageId']
            extImgId = facematch['Face']['ExternalImageId']

            print('Image Id {} from collection {}'.format(imgId, config['face_collection']))
            print('External Image Id {} from collection {}'.format(extImgId, config['face_collection']))

            #Check if Human found is in the person watch list
            if(extImgId.upper() in (person.upper() for person in person_watch_list)):
                return extImgId


def handler(event, context):
    return process_image(event, context)
