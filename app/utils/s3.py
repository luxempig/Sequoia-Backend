import os
import re
from typing import Optional, Tuple
import boto3
from botocore.client import Config as BotoConfig

_AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
_MEDIA_BUCKET_FALLBACK = os.getenv("MEDIA_BUCKET", "")  # used if s3_url is just a key (no bucket)

_s3 = None
def _client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=_AWS_REGION, config=BotoConfig(signature_version="s3v4"))
    return _s3

_S3_URL_RE = re.compile(r"^s3://([^/]+)/(.+)$")

def _parse_s3_url(s3_url: str) -> Optional[Tuple[str, str]]:
    """
    Accepts either:
      - s3://bucket/key
      - key (bare key) -> uses MEDIA_BUCKET env as bucket if present
    Returns (bucket, key) or None if not resolvable.
    """
    if not s3_url:
        return None
    m = _S3_URL_RE.match(s3_url)
    if m:
        return m.group(1), m.group(2)
    # Bare key path case
    if _MEDIA_BUCKET_FALLBACK:
        return _MEDIA_BUCKET_FALLBACK, s3_url.lstrip("/")
    return None

def presign_from_media_s3_url(s3_url: str, expires: int = 3600) -> Optional[str]:
    """
    Given media.s3_url (either s3://bucket/key or bare key), return presigned HTTPS URL.
    If bucket/key cannot be determined, returns None.
    """
    parsed = _parse_s3_url(s3_url)
    if not parsed:
        return None
    bucket, key = parsed
    try:
        return _client().generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires,
        )
    except Exception:
        return None
