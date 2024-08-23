from fastapi import FastAPI, File, UploadFile, HTTPException
import boto3
# from botocore.exceptions import SignatureDoesNotMatch
import mimetypes
import uuid
from dotenv import load_dotenv
load_dotenv()
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION = os.getenv("REGION")

app = FastAPI()

# Initialize the S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)

# Define allowed video MIME types
ALLOWED_VIDEO_TYPES = ["video/mp4", "video/mpeg", "video/ogg", "video/webm", "video/quicktime"]

@app.post("/upload-video/")
async def upload_video(file: UploadFile = File(...)):
    # Check the file type
    file_type, _ = mimetypes.guess_type(file.filename)

    if file_type not in ALLOWED_VIDEO_TYPES:
        raise HTTPException(status_code=400, detail="Invalid file type. Only video files are allowed.")
    
    # Upload the file to S3
    bucket_name = "temp-videos-vidmox-test"
    s3_key = f"{uuid.uuid4()}.{file.filename.split('.')[-1]}"
    
    try:
        multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
        upload_id = multipart_upload['UploadId']
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")
    
    try:
        chunk_size = 50 * 1024 * 1024  # 50 MB

        parts = []
        part_number = 1

        while True:
            # Read chunk from file
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            
            # Upload chunk to S3
            part = s3_client.upload_part(
                Bucket=bucket_name,
                Key=s3_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=chunk
            )
            parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
            part_number += 1

        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        file_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
        return {"url": file_url}
    
    except Exception as e:
    #     # Abort multipart upload if an error occurs
        s3_client.abort_multipart_upload(Bucket=bucket_name, Key=s3_key, UploadId=upload_id)
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")
