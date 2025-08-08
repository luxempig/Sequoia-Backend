#!/usr/bin/env python3
"""
Drive-optimized ingest: download URLs (incl. Google Drive), upload to S3 with metadata.
Idempotent: uses SHA-256-based stable key; skips if object already exists (unless --force).

Usage:
  pip install boto3 requests gdown urllib3 python-dotenv
  python ingest_sources_drive_optimized.py     --csv stage_sources.csv     --bucket uss-sequoia-bucket     --region us-east-2     --threads 4     --success-out out/uploaded_success.csv     --fail-out out/uploaded_failures.csv

CSV columns expected (headers case-insensitive; extras ignored):
  url, source_type, source_origin, publication_date, publication, headline, page, notes

Tip: Lower threads for Drive-heavy runs (2–4). This script throttles Drive per-host.
"""
import argparse, csv, hashlib, mimetypes, os, random, re, sys, tempfile, threading, time
from datetime import datetime
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import boto3, botocore
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import gdown  # for Google Drive (handles confirm token/cookies)
except Exception as e:
    gdown = None

# -------- helpers --------
CT_TO_EXT = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png", "image/tiff": ".tif",
    "image/gif": ".gif", "application/pdf": ".pdf", "video/quicktime": ".mov", "video/mp4": ".mp4"
}

def guess_ext(url_path: str, content_type: Optional[str]) -> str:
    # From URL
    filename = url_path.rsplit("/", 1)[-1]
    if "." in filename:
        ext = "." + filename.split(".")[-1].split("?")[0].split("#")[0].lower()
        if 1 <= len(ext) <= 6:
            return ext
    # From Content-Type
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct in CT_TO_EXT: return CT_TO_EXT[ct]
        ext = mimetypes.guess_extension(ct) or ""
        return ext.lower()
    return ""

def parse_year_month(datestr: str) -> Tuple[str,str]:
    if not datestr: return ("unknown","unknown")
    m = re.match(r"^\s*(\d{4})(?:[-/](\d{1,2}))?", datestr)
    if m:
        year = m.group(1); month = int(m.group(2) or "1")
        return (year, f"{month:02d}")
    return ("unknown","unknown")

def stable_key(source_type: str, origin: str, year: str, month: str, sha: str, ext: str) -> str:
    def slug(x: str) -> str:
        x = (x or "").strip().lower()
        x = re.sub(r"[^a-z0-9._-]+", "-", x)
        x = re.sub(r"-{2,}", "-", x).strip("-")
        return x or "unknown"
    st = slug(source_type); so = slug(origin)
    ex = (ext or "").lower()
    if ex and not ex.startswith("."): ex = "." + ex
    return f"raw/{st}/{so}/{year}/{month}/{sha}{ex}"

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def domain_origin(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        parts = host.lower().split(".")
        return ".".join(parts[-2:]) if len(parts)>=2 else host.lower()
    except Exception:
        return ""

# -------- HTTP client + per-host throttling --------
def make_http_session() -> requests.Session:
    retry = Retry(total=5, connect=3, read=3, backoff_factor=0.7,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET","HEAD"])
    sess = requests.Session()
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.mount("http://", HTTPAdapter(max_retries=retry))
    sess.headers.update({"User-Agent": "Mozilla/5.0 (SequoiaIngest/1.0)"})
    return sess

HOST_LIMITS = {
    "drive.google.com": 2,
    "docs.google.com": 2,
    # others default unlimited
}
_host_semaphores = {h: threading.Semaphore(v) for h,v in HOST_LIMITS.items()}
def host_semaphore(url: str):
    host = urlparse(url).hostname or ""
    return _host_semaphores.get(host)

# -------- Downloaders --------
def download_google_drive(url: str, timeout: int) -> Tuple[str, str, int]:
    if gdown is None:
        raise RuntimeError("gdown not installed; pip install gdown")
    # Extract file id
    fid = None
    m = re.search(r"/file/d/([^/]+)/", url)
    if m: fid = m.group(1)
    if not fid:
        qs = parse_qs(urlparse(url).query)
        fid = (qs.get("id") or [None])[0]
    if not fid:
        raise RuntimeError("Could not parse Google Drive file id from URL")

    tmpdir = tempfile.mkdtemp(prefix="gdown_")
    # gdown will figure out confirm token/cookies; quiet download
    out = os.path.join(tmpdir, fid)
    gdown.download(id=fid, output=out, quiet=True)  # raises on failure
    # Try to infer content-type from extension
    ext = os.path.splitext(out)[1].lower()
    ct = {
        ".pdf":"application/pdf", ".jpg":"image/jpeg", ".jpeg":"image/jpeg",
        ".png":"image/png", ".tif":"image/tiff", ".tiff":"image/tiff",
        ".mp4":"video/mp4", ".mov":"video/quicktime"
    }.get(ext, "")
    size = os.path.getsize(out)
    return out, ct, size

def download_to_temp(url: str, timeout: int, http: requests.Session) -> Tuple[str, str, int]:
    sem = host_semaphore(url)
    if sem: sem.acquire()
    try:
        # Gentle jitter to avoid bursts to rate-limited hosts
        if sem: time.sleep(0.6 + random.random())

        if "drive.google.com" in url or "docs.google.com" in url:
            return download_google_drive(url, timeout)

        with http.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            ct = r.headers.get("Content-Type","") or ""
            cl = int(r.headers.get("Content-Length","0") or 0)
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk: tmp.write(chunk)
                tmp.flush()
            return tmp.name, ct, cl
    finally:
        if sem: sem.release()

# -------- Worker --------
def process_row(row: Dict[str,str], row_idx: int, s3, http: requests.Session, bucket: str, force: bool, timeout: int) -> Tuple[bool, Dict[str,str]]:
    url = (row.get("url") or row.get("URL") or "").strip()
    if not url:
        return False, {"row": str(row_idx), "error": "missing url"}

    stype = (row.get("source_type") or "").strip() or "Web"
    origin = (row.get("source_origin") or "").strip() or domain_origin(url)
    pub_date = (row.get("publication_date") or "").strip()
    publication = (row.get("publication") or "").strip()
    headline = (row.get("headline") or "").strip()
    page = (row.get("page") or "").strip()
    notes = (row.get("notes") or "").strip()

    try:
        tmp_path, content_type, _ = download_to_temp(url, timeout=timeout, http=http)
    except Exception as e:
        return False, {"row": str(row_idx), "url": url, "error": f"download_failed: {e}"}

    try:
        sha = sha256_file(tmp_path)
        # Choose extension
        ext = guess_ext(url, content_type)
        year, month = parse_year_month(pub_date)
        key = stable_key(stype, origin, year, month, sha, ext)

        # Idempotency check
        try:
            s3.head_object(Bucket=bucket, Key=key)
            os.unlink(tmp_path)
            return True, {"row": str(row_idx), "url": url, "s3_key": key, "sha256": sha, "skipped": "already_exists"}
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in ("404","NoSuchKey","NotFound"):
                raise

        extra_args = {
            "Metadata": {
                "sha256": sha,
                "source_type": stype,
                "source_origin": origin,
                "publication_date": pub_date,
                "publication": publication,
                "headline": headline,
                "page": page,
                "notes": notes,
                "url": url
            },
            "ServerSideEncryption": "AES256",
        }
        if content_type:
            extra_args["ContentType"] = content_type.split(";")[0].strip().lower()

        s3.upload_file(tmp_path, bucket, key, ExtraArgs=extra_args)
        os.unlink(tmp_path)
        return True, {"row": str(row_idx), "url": url, "s3_key": key, "sha256": sha, "content_type": content_type}
    except Exception as e:
        try: os.unlink(tmp_path)
        except Exception: pass
        return False, {"row": str(row_idx), "url": url, "error": f"upload_failed: {e}"}

# -------- Main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--region", default=None)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--success-out", default="uploaded_success.csv")
    ap.add_argument("--fail-out", default="uploaded_failures.csv")
    args = ap.parse_args()

    # boto3 session
    session_kwargs = {}
    if args.region: session_kwargs["region_name"] = args.region
    if args.profile: session_kwargs["profile_name"] = args.profile
    session = boto3.session.Session(**session_kwargs)
    s3 = session.client("s3")

    # Early bucket check
    try:
        s3.head_bucket(Bucket=args.bucket)
    except Exception as e:
        raise SystemExit(f"Bad bucket? Could not access '{args.bucket}': {e}")

    http = make_http_session()

    # Read CSV
    import csv as _csv
    with open(args.csv, newline="", encoding="utf-8-sig") as f:
        reader = _csv.DictReader(f)
        rows = list(reader)
        if not rows:
            print("No rows found in CSV.")
            open(args.success_out, "w").close()
            open(args.fail_out, "w").close()
            return

    # Process (threaded)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    successes, failures = [], []
    print(f"Writing success CSV to: {args.success_out}")
    print(f"Writing failures CSV to: {args.fail_out}")
    print(f"Total rows: {len(rows)} | Threads: {args.threads}")

    with ThreadPoolExecutor(max_workers=args.threads) as ex:
        futs = []
        for idx, row in enumerate(rows, start=2):
            futs.append(ex.submit(process_row, row, idx, s3, http, args.bucket, args.force, args.timeout))
        for fut in as_completed(futs):
            ok, info = fut.result()
            (successes if ok else failures).append(info)
            if (len(successes)+len(failures)) % 25 == 0:
                print(f"Processed: {len(successes)+len(failures)} / {len(rows)}")

    # Write reports
    if successes:
        fields = sorted({k for d in successes for k in d.keys()})
        with open(args.success_out, "w", newline="", encoding="utf-8") as out:
            w = csv.DictWriter(out, fieldnames=fields)
            w.writeheader(); w.writerows(successes)
    else:
        open(args.success_out, "w").close()

    if failures:
        fields = sorted({k for d in failures for k in d.keys()})
        with open(args.fail_out, "w", newline="", encoding="utf-8") as out:
            w = csv.DictWriter(out, fieldnames=fields)
            w.writeheader(); w.writerows(failures)
    else:
        open(args.fail_out, "w").close()

    print(f"Done. Success: {len(successes)}, Failures: {len(failures)}")

if __name__ == "__main__":
    main()
