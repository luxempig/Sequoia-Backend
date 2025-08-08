
import os
from typing import Optional
import boto3
from botocore.client import Config as BotoConfig

_AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
_MEDIA_BUCKET = os.getenv("MEDIA_BUCKET", "")

_s3 = None
def _client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=_AWS_REGION, config=BotoConfig(signature_version="s3v4"))
    return _s3

def presign_s3_key(key: str, expires: int = 3600) -> Optional[str]:
    """Return a presigned https URL for an S3 object key, or None if bucket not set."""
    bucket = _MEDIA_BUCKET
    if not bucket or not key:
        return None
    return _client().generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires,
    )
