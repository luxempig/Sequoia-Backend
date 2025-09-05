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
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

DROPBOX_ACCESS_TOKEN = os.environ.get("DROPBOX_ACCESS_TOKEN", "").strip()
DROPBOX_TIMEOUT = int(os.environ.get("DROPBOX_TIMEOUT", "60"))

# ------- Google services -------

def _drive_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds)

def _sheets_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds)

# ------- Read existing media rows to map link -> existing S3 path -------

_MEDIA_CACHE: Optional[Dict[str, Dict[str,str]]] = None

def _media_link_map(spreadsheet_id: Optional[str]) -> Dict[str, Dict[str,str]]:
    """
    Returns { link_string_lower: { 's3_url':..., 'media_type':..., 'credit':..., 'media_slug':..., 'voyage_slug':... } }
    """
    global _MEDIA_CACHE
    if _MEDIA_CACHE is not None:
        return _MEDIA_CACHE
    _MEDIA_CACHE = {}
    if not spreadsheet_id:
        return _MEDIA_CACHE
    svc = _sheets_service()
    title = "media"
    try:
        res = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{title}!A:ZZ").execute()
        vals = res.get("values", []) or []
        if not vals:
            return _MEDIA_CACHE
        hdr = [h.strip().lower() for h in vals[0]]
        idx = {name: (hdr.index(name) if name in hdr else -1) for name in
               ["google_drive_link","s3_url","media_type","credit","media_slug","voyage_slug"]}
        for row in vals[1:]:
            link = (row[idx["google_drive_link"]] if idx["google_drive_link"] >= 0 and idx["google_drive_link"] < len(row) else "").strip()
            if not link: continue
            _MEDIA_CACHE[link.lower()] = {
                "s3_url": (row[idx["s3_url"]] if idx["s3_url"] >= 0 and idx["s3_url"] < len(row) else "").strip(),
                "media_type": (row[idx["media_type"]] if idx["media_type"] >= 0 and idx["media_type"] < len(row) else "").strip(),
                "credit": (row[idx["credit"]] if idx["credit"] >= 0 and idx["credit"] < len(row) else "").strip(),
                "media_slug": (row[idx["media_slug"]] if idx["media_slug"] >= 0 and idx["media_slug"] < len(row) else "").strip(),
                "voyage_slug": (row[idx["voyage_slug"]] if idx["voyage_slug"] >= 0 and idx["voyage_slug"] < len(row) else "").strip(),
            }
    except Exception as e:
        LOG.warning("Failed to read media tab for link map: %s", e)
    return _MEDIA_CACHE

# ------- Link parsing & downloads -------

def _parse_drive_file_id(url: str) -> Optional[str]:
    m = re.search(r"/file/d/([A-Za-z0-9_\-]+)/", url or "")
    return m.group(1) if m else None

def _download_drive_binary(file_id: str) -> Tuple[bytes, str]:
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

def _download_dropbox_binary(shared_url: str) -> Tuple[bytes, str, Optional[str]]:
    """
    Returns (bytes, content_type, filename_ext_guess)
    If DROPBOX_ACCESS_TOKEN is present, use API for reliability. Otherwise, use direct-download link (?dl=1).
    """
    if DROPBOX_ACCESS_TOKEN:
        # https://content.dropboxapi.com/2/sharing/get_shared_link_file
        api = "https://content.dropboxapi.com/2/sharing/get_shared_link_file"
        headers = {
            "Authorization": f"Bearer {DROPBOX_ACCESS_TOKEN}",
            "Dropbox-API-Arg": f'{{"url": "{shared_url}"}}',
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
        # sanitize to force download
        dl = shared_url
        if "dl=0" in dl: dl = dl.replace("dl=0", "dl=1")
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

# ------- Media type detection -------

IMAGE_MIMES = {"image/jpeg","image/png","image/webp","image/gif","image/tiff"}
VIDEO_MIMES = {"video/mp4","video/quicktime","video/x-msvideo","video/x-matroska"}
AUDIO_MIMES = {"audio/mpeg","audio/wav","audio/aac","audio/ogg"}
PDF_MIME = "application/pdf"

def detect_media_type(mime: str, filename_hint: str = "") -> str:
    m = (mime or "").lower()
    if m in IMAGE_MIMES: return "image"
    if m in VIDEO_MIMES: return "video"
    if m in AUDIO_MIMES: return "audio"
    if m == PDF_MIME:   return "pdf"
    mt, _ = mimetypes.guess_type(filename_hint or "")
    if (mt or "").startswith("image/"): return "image"
    if (mt or "").startswith("video/"): return "video"
    if (mt or "").startswith("audio/"): return "audio"
    if (mt or "") == "application/pdf": return "pdf"
    return "other"

def guess_extension(mime: str, filename_hint: str = "") -> str:
    ext = mimetypes.guess_extension(mime or "") or ""
    if not ext and filename_hint:
        ext = os.path.splitext(filename_hint)[1]
    ext = (ext or "").lower().lstrip(".")
    if ext == "jpe": ext = "jpg"
    if ext == "": ext = "bin"
    return ext

# ------- S3 helpers -------

def _s3():
    return boto3.client("s3", region_name=AWS_REGION)

def _s3_url(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"

def _public_http_url(bucket: str, key: str) -> str:
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

def process_all_media(media_items: List[Dict], voyage_slug: str, spreadsheet_id: Optional[str] = None) -> Tuple[Dict[str, Tuple[Optional[str], Optional[str]]], List[str]]:
    """
    For each media item:
      - If an existing media row with the same link exists and the expected new S3 path differs,
        MOVE it (copy to new key, then delete old). Derivatives are copied if present.
      - Else download (Drive/Dropbox), upload original; if image: generate preview + thumb.
    Returns:
      s3_links: { media_slug: (s3_private_url, public_derivative_preview_url|None) }
      warnings: [ ... ]
    """
    s3_links: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    warnings: List[str] = []
    link_map = _media_link_map(spreadsheet_id)

    for i, m in enumerate(media_items, start=1):
        mslug = (m.get("slug") or "").strip()
        credit = (m.get("credit") or "").strip()
        link = (m.get("google_drive_link") or "").strip()
        title = (m.get("title") or "").strip()

        if not mslug or not link:
            warnings.append(f"media #{i} missing slug or link; skipping")
            continue

        link_lc = link.lower()
        existing = link_map.get(link_lc)

        # Try a rename/move first if same link already exists in Sheets
        if existing and existing.get("s3_url"):
            try:
                old_s3 = existing["s3_url"]
                if old_s3.startswith("s3://"):
                    _, rest = old_s3[5:].split("/", 1)
                    old_bucket, old_key = old_s3[5:].split("/", 1)[0], old_s3[5:].split("/", 1)[1]
                else:
                    # Unexpected form; skip rename
                    old_bucket, old_key = S3_PRIVATE_BUCKET, ""

                # Derive old ext, mtype
                old_ext = os.path.splitext(old_key)[1].lstrip(".") if old_key else None
                mtype_current = (m.get("media_type") or existing.get("media_type") or "other").strip().lower()
                ext_for_new = old_ext or "bin"

                new_orig_key = _s3_key_for_original(voyage_slug, mslug, mtype_current, ext_for_new, credit)
                if old_key and new_orig_key != old_key:
                    # Copy original
                    _copy_object(old_bucket, old_key, S3_PRIVATE_BUCKET, new_orig_key)
                    # Delete old original per requirement (same link, renamed)
                    _delete_object(old_bucket, old_key)

                    # Try to copy derivatives if they exist
                    pres_old = president_from_voyage_slug(existing.get("voyage_slug") or voyage_slug)
                    source_old = normalize_source(existing.get("credit") or credit)
                    mtype_old = (existing.get("media_type") or mtype_current or "other")
                    old_ms = existing.get("media_slug") or mslug
                    old_prev = f"media/{pres_old}/{source_old}/{existing.get('voyage_slug') or voyage_slug}/{mtype_old}/{old_ms}_preview.jpg"
                    old_th   = f"media/{pres_old}/{source_old}/{existing.get('voyage_slug') or voyage_slug}/{mtype_old}/{old_ms}_thumb.jpg"
                    new_prev = _s3_key_for_derivative(voyage_slug, mslug, mtype_current, credit, "preview")
                    new_th   = _s3_key_for_derivative(voyage_slug, mslug, mtype_current, credit, "thumb")
                    try:
                        _copy_object(S3_PUBLIC_BUCKET, old_prev, S3_PUBLIC_BUCKET, new_prev, content_type="image/jpeg")
                        _delete_object(S3_PUBLIC_BUCKET, old_prev)
                    except Exception:
                        pass
                    try:
                        _copy_object(S3_PUBLIC_BUCKET, old_th, S3_PUBLIC_BUCKET, new_th, content_type="image/jpeg")
                        _delete_object(S3_PUBLIC_BUCKET, old_th)
                    except Exception:
                        pass

                    s3_links[mslug] = (_s3_url(S3_PRIVATE_BUCKET, new_orig_key), _public_http_url(S3_PUBLIC_BUCKET, new_prev))
                    LOG.info("Renamed media for same link -> %s", new_orig_key)
                    continue  # handled by move, no download needed
            except Exception as e:
                warnings.append(f"{mslug}: failed to move existing S3 object for same link: {e}")

        # Otherwise, download and upload as usual
        blob = None
        mime = None
        ext_hint = None

        if "/file/d/" in link:  # Google Drive
            file_id = _parse_drive_file_id(link)
            if not file_id:
                warnings.append(f"{mslug}: invalid Google Drive link (no /file/d/<ID>/)")
                s3_links[mslug] = (None, None)
                continue
            try:
                blob, mime = _download_drive_binary(file_id)
            except Exception as e:
                warnings.append(f"{mslug}: failed to download from Drive: {e}")
                s3_links[mslug] = (None, None)
                continue
        elif "dropbox.com" in link.lower():
            try:
                blob, mime, ext_hint = _download_dropbox_binary(link)
            except Exception as e:
                warnings.append(f"{mslug}: failed to download from Dropbox: {e}")
                s3_links[mslug] = (None, None)
                continue
        else:
            warnings.append(f"{mslug}: unsupported media link (not Drive/Dropbox)")
            s3_links[mslug] = (None, None)
            continue

        # Detect type / ext
        mtype = (m.get("media_type") or "").strip().lower()
        if not mtype:
            mtype = detect_media_type(mime, filename_hint=title)
        ext = guess_extension(mime, filename_hint=("." + ext_hint) if ext_hint else title)

        # Upload original
        orig_key = _s3_key_for_original(voyage_slug, mslug, mtype, ext, credit)
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
