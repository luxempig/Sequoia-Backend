from __future__ import annotations

import io
import os
import re
import mimetypes
import logging
from typing import Dict, List, Tuple, Optional

import boto3
from PIL import Image

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from voyage_ingest.slugger import normalize_source

from voyage_ingest.slugger import president_from_voyage_slug

LOG = logging.getLogger("voyage_ingest.drive_sync")

AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")
S3_PRIVATE_BUCKET = os.environ.get("S3_PRIVATE_BUCKET", "sequoia-canonical")
S3_PUBLIC_BUCKET  = os.environ.get("S3_PUBLIC_BUCKET",  "sequoia-public")

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# ------- Google Drive helpers -------

def _drive_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds)

def _parse_drive_file_id(url: str) -> Optional[str]:
    """
    Accepts URLs like:
      https://drive.google.com/file/d/<FILE_ID>/view
    """
    m = re.search(r"/file/d/([A-Za-z0-9_\-]+)/", url or "")
    return m.group(1) if m else None

def _download_drive_binary(file_id: str) -> Tuple[bytes, str]:
    """
    Returns (bytes, mimeType) of the Drive file.
    """
    svc = _drive_service()
    meta = svc.files().get(fileId=file_id, fields="id,name,mimeType").execute()
    mime = meta["mimeType"]
    req = svc.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _status, done = downloader.next_chunk()
    return buf.getvalue(), mime

# ------- Media type detection -------

IMAGE_MIMES = {
    "image/jpeg", "image/png", "image/webp", "image/gif", "image/tiff"
}
VIDEO_MIMES = {
    "video/mp4", "video/quicktime", "video/x-msvideo", "video/x-matroska"
}
AUDIO_MIMES = {
    "audio/mpeg", "audio/wav", "audio/aac", "audio/ogg"
}
PDF_MIME = "application/pdf"

def detect_media_type(mime: str, filename_hint: str = "") -> str:
    m = (mime or "").lower()
    if m in IMAGE_MIMES:
        return "image"
    if m in VIDEO_MIMES:
        return "video"
    if m in AUDIO_MIMES:
        return "audio"
    if m == PDF_MIME:
        return "pdf"
    # fallback by extension
    mt, _ = mimetypes.guess_type(filename_hint or "")
    if (mt or "").startswith("image/"):
        return "image"
    if (mt or "").startswith("video/"):
        return "video"
    if (mt or "").startswith("audio/"):
        return "audio"
    if (mt or "") == "application/pdf":
        return "pdf"
    return "other"

def guess_extension(mime: str, filename_hint: str = "") -> str:
    ext = mimetypes.guess_extension(mime or "") or ""
    if not ext and filename_hint:
        # try from hint (e.g., ".JPG" normalize)
        ext = os.path.splitext(filename_hint)[1]
    ext = (ext or "").lower().lstrip(".")
    if ext == "jpe":
        ext = "jpg"
    if ext == "":
        ext = "bin"
    return ext

# ------- S3 helpers -------

def _s3():
    return boto3.client("s3", region_name=AWS_REGION)

def _s3_url(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"

def _public_http_url(bucket: str, key: str) -> str:
    # Public bucket assumed to have public access for object reading.
    return f"https://{bucket}.s3.amazonaws.com/{key}"

def _s3_key_for_original(vslug: str, mslug: str, mtype: str, ext: str, credit: str) -> str:
    source_slug = normalize_source(credit)
    pres_slug = president_from_voyage_slug(vslug)
    return f"media/{pres_slug}/{source_slug}/{vslug}/{mtype}/{mslug}.{ext}"

def _s3_key_for_derivative(vslug: str, mslug: str, mtype: str, credit: str, kind: str) -> str:
    source_slug = normalize_source(credit)
    pres_slug = president_from_voyage_slug(vslug)
    return f"media/{pres_slug}/{source_slug}/{vslug}/{mtype}/{mslug}_{kind}.jpg"

def _upload_bytes(bucket: str, key: str, data: bytes, content_type: Optional[str] = None) -> None:
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    _s3().put_object(Bucket=bucket, Key=key, Body=data, **extra)

def _make_image_derivatives(img_bytes: bytes, max_long_edge_preview=1600, thumb_size=320) -> Tuple[bytes, bytes]:
    """
    Returns (preview_jpg_bytes, thumb_jpg_bytes)
    """
    with Image.open(io.BytesIO(img_bytes)) as im:
        im = im.convert("RGB")
        # preview
        w, h = im.size
        if w >= h:
            new_w = min(max_long_edge_preview, w)
            new_h = int(h * (new_w / w))
        else:
            new_h = min(max_long_edge_preview, h)
            new_w = int(w * (new_h / h))
        preview = im.resize((new_w, new_h), Image.LANCZOS)
        buf_prev = io.BytesIO()
        preview.save(buf_prev, format="JPEG", quality=88, optimize=True)
        # thumb
        im_copy = im.copy()
        im_copy.thumbnail((thumb_size, thumb_size), Image.LANCZOS)
        buf_th = io.BytesIO()
        im_copy.save(buf_th, format="JPEG", quality=85, optimize=True)
        return buf_prev.getvalue(), buf_th.getvalue()

# ------- Public API -------

def process_all_media(media_items: List[Dict], voyage_slug: str) -> Tuple[Dict[str, Tuple[Optional[str], Optional[str]]], List[str]]:
    """
    For each media item (with auto-generated 'slug'):
      - Download from Drive
      - Detect media_type and extension
      - Upload original to private bucket under media/<source>/<voyage_slug>/<type>/<slug>.<ext>
      - If image: generate preview + thumb to public bucket
    Returns:
      s3_links: { media_slug: (s3_private_url, public_derivative_preview_url|None) }
      warnings: [ ... ]
    """
    s3_links: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    warnings: List[str] = []

    for i, m in enumerate(media_items, start=1):
        mslug = (m.get("slug") or "").strip()
        credit = (m.get("credit") or "").strip()
        gdl = (m.get("google_drive_link") or "").strip()
        title = (m.get("title") or "").strip()

        if not mslug or not gdl:
            warnings.append(f"media #{i} missing slug or google_drive_link; skipping")
            continue

        file_id = _parse_drive_file_id(gdl)
        if not file_id:
            warnings.append(f"{mslug}: invalid google_drive_link (no /file/d/<ID>/)")
            continue

        try:
            blob, mime = _download_drive_binary(file_id)
        except Exception as e:
            warnings.append(f"{mslug}: failed to download from Drive: {e}")
            continue

        # Detect type and ext
        mtype = detect_media_type(mime)
        ext = guess_extension(mime)

        # Upload original
        orig_key = _s3_key_for_original(voyage_slug, mslug, mtype, ext, credit)
        try:
            _upload_bytes(S3_PRIVATE_BUCKET, orig_key, blob, content_type=mime)
            s3_private = _s3_url(S3_PRIVATE_BUCKET, orig_key)
        except Exception as e:
            warnings.append(f"{mslug}: failed to upload original to s3://{S3_PRIVATE_BUCKET}/{orig_key}: {e}")
            s3_private = None

        public_url = None

        # Derivatives for images only
        if mtype == "image" and blob:
            try:
                prev, th = _make_image_derivatives(blob)
                prev_key = _s3_key_for_derivative(voyage_slug, mslug, mtype, credit, "preview")
                th_key   = _s3_key_for_derivative(voyage_slug, mslug, mtype, credit, "thumb")
                _upload_bytes(S3_PUBLIC_BUCKET, prev_key, prev, content_type="image/jpeg")
                _upload_bytes(S3_PUBLIC_BUCKET, th_key,   th,   content_type="image/jpeg")
                public_url = _public_http_url(S3_PUBLIC_BUCKET, prev_key)
            except Exception as e:
                warnings.append(f"{mslug}: failed to create/upload derivatives: {e}")

        s3_links[mslug] = (s3_private, public_url)

        LOG.info("Processed media %s (type=%s) -> %s", mslug, mtype, orig_key)

    return s3_links, warnings
