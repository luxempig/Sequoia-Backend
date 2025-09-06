from __future__ import annotations

import io
import os
import re
import mimetypes
import logging
from typing import Dict, List, Tuple, Optional

import boto3
from PIL import Image
import requests

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from voyage_ingest.slugger import normalize_source, president_from_voyage_slug

LOG = logging.getLogger("voyage_ingest.drive_sync")

AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")
S3_PRIVATE_BUCKET = os.environ.get("S3_PRIVATE_BUCKET", "sequoia-canonical")
S3_PUBLIC_BUCKET  = os.environ.get("S3_PUBLIC_BUCKET",  "sequoia-public")

DRIVE_SCOPES  = ["https://www.googleapis.com/auth/drive.readonly"]

DROPBOX_ACCESS_TOKEN = os.environ.get("DROPBOX_ACCESS_TOKEN", "").strip()
DROPBOX_TIMEOUT = int(os.environ.get("DROPBOX_TIMEOUT", "60"))

# ------- Google services -------
def _drive_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds)

# ------- Link parsing & downloads -------
def _parse_drive_file_id(url: str) -> Optional[str]:
    m = re.search(r"/file/d/([A-Za-z0-9_\-]+)/", url or "")
    return m.group(1) if m else None

def _download_drive_binary(file_id: str) -> Tuple[bytes, str, str]:
    svc = _drive_service()
    meta = svc.files().get(fileId=file_id, fields="id,name,mimeType").execute()
    mime = meta.get("mimeType") or "application/octet-stream"
    name = meta.get("name") or "file"
    req = svc.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _status, done = downloader.next_chunk()
    return buf.getvalue(), mime, name

def _download_dropbox_binary(shared_url: str) -> Tuple[bytes, str, Optional[str]]:
    if DROPBOX_ACCESS_TOKEN:
        api = "https://content.dropboxapi.com/2/sharing/get_shared_link_file"
        headers = {
            "Authorization": f"Bearer {DROPBOX_ACCESS_TOKEN}",
            "Dropbox-API-Arg": f'{{"url":"{shared_url}"}}',
        }
        r = requests.post(api, headers=headers, timeout=DROPBOX_TIMEOUT)
        r.raise_for_status()
        ctype = r.headers.get("Content-Type","application/octet-stream")
        dispo = r.headers.get("Content-Disposition","")
        ext = None
        m = re.search(r'filename\*?=.*?\.([A-Za-z0-9]{1,8})', dispo)
        if m: ext = m.group(1).lower()
        return r.content, ctype, ext
    else:
        dl = shared_url
        if "dl=0" in dl: dl = dl.replace("dl=0","dl=1")
        elif "dl=1" in dl: pass
        elif "?" in dl: dl = dl + "&dl=1"
        else: dl = dl + "?dl=1"
        r = requests.get(dl, timeout=DROPBOX_TIMEOUT)
        r.raise_for_status()
        ctype = r.headers.get("Content-Type","application/octet-stream")
        dispo = r.headers.get("Content-Disposition","")
        ext = None
        m = re.search(r'filename\*?=.*?\.([A-Za-z0-9]{1,8})', dispo)
        if m: ext = m.group(1).lower()
        return r.content, ctype, ext

# ------- Media type/ext detection -------
IMAGE_EXTS = {"jpg","jpeg","png","webp","gif","tiff"}
VIDEO_EXTS = {"mp4","mov","avi","mkv"}
AUDIO_EXTS = {"mp3","wav","aac","ogg"}
PDF_EXTS   = {"pdf"}

def _ext_from_name_or_mime(name: str, mime: str) -> str:
    ext = os.path.splitext(name or "")[1].lstrip(".").lower()
    if not ext:
        ext_guess = (mimetypes.guess_extension(mime or "") or "").lstrip(".").lower()
        if ext_guess == "jpe": ext_guess = "jpg"
        ext = ext_guess
    return ext or "bin"

def detect_media_type_from_ext(ext: str) -> str:
    e = (ext or "").lower()
    if e in IMAGE_EXTS: return "image"
    if e in VIDEO_EXTS: return "video"
    if e in AUDIO_EXTS: return "audio"
    if e in PDF_EXTS:   return "pdf"
    return "other"

# ------- S3 -------
def _s3(): return boto3.client("s3", region_name=AWS_REGION)
def _s3_url(bucket: str, key: str) -> str: return f"s3://{bucket}/{key}"
def _public_http_url(bucket: str, key: str) -> str: return f"https://{bucket}.s3.amazonaws.com/{key}"

def _s3_key_for_original(vslug: str, mslug: str, ext: str, credit: str) -> str:
    source_slug = normalize_source(credit)
    pres_slug = president_from_voyage_slug(vslug)
    return f"media/{pres_slug}/{source_slug}/{vslug}/{ext}/{mslug}.{ext}"

def _s3_key_for_derivative(vslug: str, mslug: str, ext: str, credit: str, kind: str) -> str:
    source_slug = normalize_source(credit)
    pres_slug = president_from_voyage_slug(vslug)
    return f"media/{pres_slug}/{source_slug}/{vslug}/{ext}/{mslug}_{kind}.jpg"

def _upload_bytes(bucket: str, key: str, data: bytes, content_type: Optional[str] = None) -> None:
    extra = {}
    if content_type: extra["ContentType"] = content_type
    _s3().put_object(Bucket=bucket, Key=key, Body=data, **extra)

def _copy_object(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str, content_type: Optional[str] = None) -> None:
    extra = {"CopySource": {"Bucket": src_bucket, "Key": src_key}, "Bucket": dst_bucket, "Key": dst_key}
    if content_type:
        extra["MetadataDirective"] = "REPLACE"
        extra["ContentType"] = content_type
    _s3().copy_object(**extra)

def _delete_object(bucket: str, key: str) -> None:
    _s3().delete_object(Bucket=bucket, Key=key)

def _make_image_derivatives(img_bytes: bytes, max_long_edge_preview=1600, thumb_size=320) -> Tuple[bytes, bytes]:
    with Image.open(io.BytesIO(img_bytes)) as im:
        im = im.convert("RGB")
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
        im_copy = im.copy()
        im_copy.thumbnail((thumb_size, thumb_size), Image.LANCZOS)
        buf_th = io.BytesIO()
        im_copy.save(buf_th, format="JPEG", quality=85, optimize=True)
        return buf_prev.getvalue(), buf_th.getvalue()

# ------- Public API -------
def process_all_media(media_items: List[Dict], voyage_slug: str) -> Tuple[Dict[str, Tuple[Optional[str], Optional[str]]], List[str]]:
    """
    Download each media by link, upload original to S3:
      media/{pres}/{source}/{voyage}/{ext}/{slug}.{ext}
    For images, also create preview/thumb JPEGs in public bucket.
    Returns:
      s3_links: { media_slug: (s3_private_url, public_preview_url|None) }
      warnings: [ ... ]
    """
    s3_links: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    warnings: List[str] = []

    for i, m in enumerate(media_items, start=1):
        mslug = (m.get("slug") or "").strip()
        credit = (m.get("credit") or "").strip()
        link = (m.get("google_drive_link") or "").strip()
        title = (m.get("title") or "").strip()

        if not mslug or not link:
            warnings.append(f"media #{i} missing slug or link; skipping")
            s3_links[mslug or f"missing-{i}"] = (None, None)
            continue

        blob = None
        mime = None
        fname = ""

        if "/file/d/" in link:  # Google Drive
            file_id = _parse_drive_file_id(link)
            if not file_id:
                warnings.append(f"{mslug}: invalid Google Drive link")
                s3_links[mslug] = (None, None)
                continue
            try:
                blob, mime, fname = _download_drive_binary(file_id)
            except Exception as e:
                warnings.append(f"{mslug}: failed to download from Drive: {e}")
                s3_links[mslug] = (None, None)
                continue
        elif "dropbox.com" in link.lower():
            try:
                blob, mime, ext_hint = _download_dropbox_binary(link)
                fname = f"file.{ext_hint or 'bin'}"
            except Exception as e:
                warnings.append(f"{mslug}: failed to download from Dropbox: {e}")
                s3_links[mslug] = (None, None)
                continue
        else:
            warnings.append(f"{mslug}: unsupported media link (not Drive/Dropbox)")
            s3_links[mslug] = (None, None)
            continue

        # Extension & type
        ext = _ext_from_name_or_mime(fname, mime)
        mtype = detect_media_type_from_ext(ext)

        # Upload original
        orig_key = _s3_key_for_original(voyage_slug, mslug, ext, credit)
        try:
            _upload_bytes(S3_PRIVATE_BUCKET, orig_key, blob, content_type=mime)
            s3_private = _s3_url(S3_PRIVATE_BUCKET, orig_key)
        except Exception as e:
            warnings.append(f"{mslug}: failed to upload original to s3://{S3_PRIVATE_BUCKET}/{orig_key}: {e}")
            s3_private = None

        public_url = None
        if mtype == "image" and blob:
            try:
                prev, th = _make_image_derivatives(blob)
                prev_key = _s3_key_for_derivative(voyage_slug, mslug, ext, credit, "preview")
                th_key   = _s3_key_for_derivative(voyage_slug, mslug, ext, credit, "thumb")
                _upload_bytes(S3_PUBLIC_BUCKET, prev_key, prev, content_type="image/jpeg")
                _upload_bytes(S3_PUBLIC_BUCKET, th_key,   th,   content_type="image/jpeg")
                public_url = _public_http_url(S3_PUBLIC_BUCKET, prev_key)
            except Exception as e:
                warnings.append(f"{mslug}: failed to create/upload derivatives: {e}")

        s3_links[mslug] = (s3_private, public_url)
        LOG.info("Processed media %s -> %s", mslug, orig_key)

    return s3_links, warnings
