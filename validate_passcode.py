import boto3

def lambda_handler(event, context):
    print(event)
    
    passcode = event["AccessCode"]
    print(passcode)

    dynamodb = boto3.resource("dynamodb")
    print("get table")
    passcode_table = dynamodb.Table("passcodes")

    response = passcode_table.get_item(
        Key={
            'AccessCode': passcode
        },
        AttributesToGet=[
            'VisitorId',
        ]
    )
    try:
        face_id = response['Item']['VisitorId']
    except KeyError:
        print('OTP Mismatch. Permission denied.')
        return {
            'body': False,
            'Name': 'OTP Mismatch or Wrong OTP'
        }
    print(face_id)
    print('VALID!!!')
    visitor_table = dynamodb.Table("visitors")
    response = visitor_table.get_item(
        Key={
            'FaceId': face_id
        },
        AttributesToGet=[
            'Name',
        ]
    )
    name = "Hi, " + response['Item']['Name'] + " Door is open"
    return {
            'body': True,
            'Name': name
        }
