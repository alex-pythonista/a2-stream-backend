import json
import boto3
import os
from botocore.exceptions import ClientError

# Initialize MediaConvert and S3 clients
mediaconvert = boto3.client('mediaconvert')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the SQS message body
    
    s3_event = json.loads(event['Records'][0]['body'])
    bucket_name = s3_event['Records'][0]['s3']['bucket']['name']
    object_key = s3_event['Records'][0]['s3']['object']['key']


        

    return {
        'statusCode': 200,
        'body': event
    }
