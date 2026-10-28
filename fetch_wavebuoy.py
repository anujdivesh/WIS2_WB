#!/usr/bin/env python3
"""Pull new wave buoy data from the SPC ocean-obs API.

Reads url / station_id / last_data_time from config.json, keeps the API
response unchanged except that "data" only holds records newer than
last_data_time and station_id is replaced, writes it to
output/<station_id>_<UTC timestamp>.json, uploads it to MinIO, then updates
last_data_time.

Every run is logged to the console and to logs/fetch_wavebuoy.log.

Usage: python3 fetch_wavebuoy.py [config.json]
"""

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "output"
LOG_DIR = SCRIPT_DIR / "logs"

log = logging.getLogger("fetch_wavebuoy")


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    file_handler = RotatingFileHandler(LOG_DIR / "fetch_wavebuoy.log",
                                       maxBytes=5 * 1024 * 1024, backupCount=5)
    console_handler = logging.StreamHandler(sys.stdout)
    for handler in (file_handler, console_handler):
        handler.setFormatter(fmt)
        log.addHandler(handler)
    log.setLevel(logging.INFO)


def fetch(url, retries=3, timeout=60):
    # The API's firewall returns 403 for Python's default "Python-urllib" User-Agent.
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; wis2-wavebuoy-fetch/1.0)",
        "Accept": "application/json",
    })
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.load(resp)
        except Exception as exc:
            # 4xx errors (except 429 Too Many Requests) won't fix themselves on retry.
            client_error = (isinstance(exc, urllib.error.HTTPError)
                            and 400 <= exc.code < 500 and exc.code != 429)
            if attempt == retries or client_error:
                raise
            log.warning("Fetch attempt %d/%d failed (%s), retrying in 5s", attempt, retries, exc)
            time.sleep(5)


def upload_to_minio(minio_cfg, file_path):
    from minio import Minio

    # Password can come from the environment instead of being kept in config.json.
    secret_key = os.environ.get("WIS2BOX_STORAGE_PASSWORD") or minio_cfg["secret_key"]
    endpoint = minio_cfg.get("endpoint", "localhost:9000")
    client = Minio(
        endpoint=endpoint,
        access_key=minio_cfg.get("access_key", "wis2box"),
        secret_key=secret_key,
        secure=minio_cfg.get("secure", False))

    # Object path should match the dataset metadata id / topic in the data mappings.
    object_name = f"{minio_cfg['path'].rstrip('/')}/{file_path.name}"
    bucket = minio_cfg.get("bucket", "wis2box-incoming")
    log.info("Uploading %s to %s -> %s/%s", file_path.name, endpoint, bucket, object_name)
    client.fput_object(bucket, object_name, str(file_path), content_type="application/json")
    return f"{bucket}/{object_name}"


def run(config_path):
    log.info("Reading config %s", config_path)
    config = json.loads(config_path.read_text())

    url = config.get("url")
    if not url:
        raise ValueError(f"url missing in {config_path}")
    last_time = config.get("last_data_time") or ""
    station_id = config.get("station_id")
    log.info("last_data_time = %s", last_time or "(none, taking all records)")

    log.info("Fetching %s", url)
    result = fetch(url)
    if not isinstance(result.get("data"), list):
        raise ValueError(f"Unexpected response from {url}: no 'data' list")
    log.info("Received %d record(s) from API", len(result["data"]))

    # Times share one ISO-8601 format (e.g. 2026-10-28T00:23:31.000Z), so string
    # comparison orders them correctly. Keeps the API's original record order.
    result["data"] = [r for r in result["data"] if not last_time or r["time"] > last_time]
    if station_id:
        result["station_id"] = station_id
    station_id = result.get("station_id") or "station"

    if not result["data"]:
        log.info("No new data for %s since %s, nothing to do", station_id, last_time)
        return

    newest = max(r["time"] for r in result["data"])
    oldest = min(r["time"] for r in result["data"])
    log.info("%d new record(s) for %s, %s to %s", len(result["data"]), station_id, oldest, newest)

    OUT_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = OUT_DIR / f"{station_id}_{stamp}.json"
    out_file.write_text(json.dumps(result, indent=2) + "\n")
    log.info("Wrote %s", out_file)

    # Upload before updating last_data_time, so a failed upload is retried next run.
    if config.get("minio"):
        dest = upload_to_minio(config["minio"], out_file)
        log.info("Uploaded to %s", dest)
    else:
        log.info("No minio section in config, skipping upload")

    # Write to a temp file then rename, so a crash can't leave a half-written config.
    config["last_data_time"] = newest
    tmp = config_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(config, indent=2) + "\n")
    tmp.replace(config_path)
    log.info("Updated last_data_time: %s -> %s", last_time or "(none)", newest)


def main():
    setup_logging()
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else SCRIPT_DIR / "config.json"
    log.info("===== Run started =====")
    try:
        run(config_path)
    except KeyboardInterrupt:
        log.warning("Run INTERRUPTED (Ctrl+C), last_data_time not updated")
        sys.exit(130)
    except Exception as exc:
        log.exception("Run FAILED, last_data_time not updated: %s", exc)
        log.info("===== Run finished with errors =====")
        sys.exit(1)
    log.info("===== Run finished OK =====")


if __name__ == "__main__":
    main()
