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
    
    # MediaConvert settings
    input_file = f"s3://{bucket_name}/{object_key}"
    output_path = f"s3://{bucket_name}/HLS/"
    
    # Specify the MediaConvert job settings
    job_settings = {
            "Inputs": [
                {
                    "FileInput": input_file,
                    "VideoSelector": {
                        "ColorSpace": "FOLLOW"
                    }
                }
            ],
            "OutputGroups": [
                {
                    "Name": "Apple HLS",
                    "OutputGroupSettings": {
                        "Type": "HLS_GROUP_SETTINGS",
                        "HlsGroupSettings": {
                            "Destination": output_path,
                            "SegmentLength": 10,
                            "SegmentControl": "SEGMENTED_FILES",
                            "OutputSelection": "MANIFESTS_AND_SEGMENTS",
                            "DirectoryStructure": "SINGLE_DIRECTORY",
                            "ManifestCompression": "NONE",
                            "AdditionalManifests": [
                                {
                                    "ManifestNameModifier": "_master",
                                    "SelectedOutputs": ["_1080p", "_720p", "_360p"]
                                }
                            ]
                        }
                    },
                    "Outputs": [
                        {
                            "Preset": "System-Avc_16x9_1080p_29_97fps_8Mbps",
                            "NameModifier": "_1080p"
                        },
                        {
                            "Preset": "System-Avc_16x9_720p_29_97fps_4_5Mbps",
                            "NameModifier": "_720p"
                        },
                        {
                            "Preset": "System-Avc_16x9_360p_29_97fps_600kbps",
                            "NameModifier": "_360p"
                        }
                    ]
                }
            ]
        }
    # Create the MediaConvert job
    try:
        response = mediaconvert.create_job(
            Role=os.environ['MEDIACONVERT_ROLE'],
            UserMetadata={
                'JobName': 'TranscodeToHLS'
            },
            Settings=job_settings
        )
        print(f"MediaConvert Job created successfully: {response['Job']['Id']}")
    except ClientError as e:
        print(f"Failed to create MediaConvert job: {e}")
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Job Created Successfully')
    }
