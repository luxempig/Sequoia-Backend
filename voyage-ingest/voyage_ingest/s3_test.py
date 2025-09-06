import boto3
from botocore.exceptions import ClientError

BUCKET = "your-bucket-name"
KEY = "media/test-upload.txt"

with open("test-upload.txt", "w") as f:
    f.write("Hello from EC2 at S3 test!\n")

s3 = boto3.client("s3")

try:
    s3.upload_file("test-upload.txt", BUCKET, KEY)
    print(f"✅ Uploaded test-upload.txt to s3://{BUCKET}/{KEY}")
except ClientError as e:
    print(f"❌ Upload failed: {e}")
