from __future__ import print_function

import base64
import json
import logging
import random
from datetime import datetime
import cv2
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

print('Loading function')

rekognition = boto3.client('rekognition')
dynamodb = boto3.resource('dynamodb')
kvs = boto3.client("kinesisvideo")
s3 = boto3.resource('s3')
    
def send_ses_message(from_email, to_email, subject, body):
    ses_client = boto3.client("ses")
   
    try:
        response=ses_client.send_email(
            Source=from_email,
            Destination={
                'ToAddresses': [
                    to_email
            ]},
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'utf8'
            },
                'Body': {
                    'Text': {
                        'Data': body,
                        'Charset': 'utf8'
                    }
                }
            })
    except ClientError as e:
        print(e.response['Error']['Message'])
    
    
def save_passcode(passcode, visitor_id):
    table = dynamodb.Table('passcodes')
    table.put_item(
        Item={
            'AccessCode': passcode,
            'VisitorId': visitor_id
        }
    )
    
def visitor_lookup(face_id):
    table = dynamodb.Table('visitors')
    response = table.get_item(
        Key={
            'FaceId': face_id
        },
        AttributesToGet=[
            'Email',
            'LastTime',
            'Authorized',
            'Photos'
        ]
    )
    
    print('VISITOR LOOKUP RESULT')
    print(response)
    
    email = response["Item"]["Email"]
    last_time = response["Item"]["LastTime"]
    authorized = response["Item"]["Authorized"]
    objectkey = response["Item"]["Photos"][0]['ObjectKey']
    return last_time, authorized, email, objectkey
    
    
def update_email_timestamp(face_id):
    table = dynamodb.Table('visitors')
    response = table.update_item(
        Key={
            'FaceId': face_id
        },
        AttributeUpdates={
            'LastTime': {
                'Value': int(datetime.now().timestamp())
            }
        }
    )
    
    
def capture_and_index_face(stream_arn, image_id):
    response = kvs.get_data_endpoint(
        StreamARN=stream_arn,
        APIName='GET_MEDIA'
    )

    endpoint = response['DataEndpoint']
    video_client = boto3.client('kinesis-video-media', endpoint_url=endpoint)
    stream = video_client.get_media(StreamARN=stream_arn, StartSelector={'StartSelectorType': 'NOW'})
    print('PAYLOAD:%s' % stream['Payload'])

    with open('/tmp/stream.mkv', 'wb') as f:
        streamBody = stream['Payload'].read(640 * 480)  
        f.write(streamBody)


        # use openCV to get a frame
        cap = cv2.VideoCapture('/tmp/stream.mkv')

        ret, frame = cap.read()
        cv2.imwrite('/tmp/new_frame.jpg', frame)

        s3_client = boto3.client('s3')
        s3_client.upload_file(
            '/tmp/new_frame.jpg',
            'hw2-b1-visitors',
            image_id
        )

        cap.release()
        print('Image uploaded')

        response = rekognition.index_faces(
            CollectionId='hw2_faces',
            Image={
                'S3Object': {
                    'Bucket': 'hw2-b1-visitors',
                    'Name': image_id
                }
            },
            ExternalImageId=image_id,
            DetectionAttributes=[
                'ALL',
            ],
            MaxFaces=1,
            QualityFilter='AUTO'
        )
        print('REKOGNITION RESPONSE: %s' % response)
        
        table = dynamodb.Table('visitors')
    
        response = table.put_item(
            Item={
                'FaceId': response['FaceRecords'][0]['Face']['FaceId'],
                'Authorized': False,
                'LastTime': 0,
                "Email": "None",
                'Photos': [
                    {
                        'ObjectKey': image_id,
                        'Bucket': 'hw2-b1-visitors',
                        'CreatedTimestamp': str(datetime.now())
                    }
                ]
            }
        )
        print('DYNAMO RESPONSE:')
        print(response)



def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    owner = 'yl4272@columbia.edu'
    
    records = event['Records']
    for r in records:
        data = r['kinesis']['data']
        data = base64.b64decode(data)
        # print('DATA:')
        print(data)

        json_data = json.loads(data)
        
        stream_arn = json_data["InputInformation"]["KinesisVideo"]["StreamArn"]
        
        # Save timestamp, used as primary key for face
        time_stamp = int(json_data["InputInformation"]["KinesisVideo"]["ServerTimestamp"])

        try:
            if json_data["FaceSearchResponse"][0]["MatchedFaces"] == []: # Unkown face
                print("UNKOWN FACE")
                time_interval = int(time_stamp/10)
                
                invocation_records = dynamodb.Table('invocation_records')
                response = invocation_records.get_item(
                    Key={
                        'TimeStamp': time_interval
                    }
                )
                # print response
                # An unknown face processed in the current 10 second interval; Do not process again
                if 'Item' in response: 
                    print("ALREADY PROCESSED: %s" % time_interval)
                    return
                else: # Put in new record
                    print('LOGGING NEW INTERVAL: %s' % time_interval)
                    invocation_records.put_item(
                        Item={
                            'TimeStamp': time_interval
                        })
                
                image_id = str(time_stamp) +'.jpg'
                # print('IMAGE ID: %s' % image_id)
                capture_and_index_face(stream_arn, image_id)

            else: # Known face
                print('KNOWN FACE')
                face_id = json_data["FaceSearchResponse"][0]["MatchedFaces"][0]["Face"]["FaceId"]
                # print("FACE ID: %s" % face_id)
                last_time, authorized, email, objectkey = visitor_lookup(face_id)
                # If processed in the past minute, do not process again
                if int(datetime.now().timestamp()) - last_time < 60:
                    return
                if authorized==False: # unapproved visitor
                    print('UNAUTHORIZED VISITOR')
                    link = "http://hw2-frontend.s3-website-us-east-1.amazonaws.com/?face_id="+face_id+"&objectkey="+objectkey
                    send_ses_message(owner, owner, "New Visitor", link)
                    update_email_timestamp(face_id)
                    return

                print('AUTHORIZED VISITOR')
                passcode = str(random.randrange(10000, 99999))
                save_passcode(passcode, face_id)
                send_ses_message(owner, email, 'Your Access Code', passcode)
                update_email_timestamp(face_id)

        except IndexError:
            # No faces found; Take no action
            print("NO FACE")
