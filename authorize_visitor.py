import boto3
import json
from datetime import datetime

def authorize_visitor(face_id, name, email):
    dynamodb = boto3.resource('dynamodb')
    visitors_table = dynamodb.Table('visitors')
    response = visitors_table.update_item(
        Key={
            'FaceId': face_id
        },
        AttributeUpdates={
            'Name': {
                'Value': name
            },
            'Email': {
                'Value': email
            },
            'Authorized': {
                'Value': True
            }
        }
    )
    print(response)


def lambda_handler(event, context):

    face_id = event["faceId"]
    name = event["name"]
    email = event["email"]

    authorize_visitor(face_id, name, email)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Visitor Authorized')
    }
