
import json
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

APP_TITLE = "PMTiles Demo API"

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minio12345")
S3_BUCKET = os.environ.get("S3_BUCKET", "pmtiles-demo")

PUBLIC_TILES_BASE = os.environ.get("PUBLIC_TILES_BASE", "/tiles")
BUNDLES_DB = os.environ.get("BUNDLES_DB", "/app/data/bundles.db")
WORKDIR = Path(os.environ.get("WORKDIR", "/app/work"))

WORKDIR.mkdir(parents=True, exist_ok=True)
Path(BUNDLES_DB).parent.mkdir(parents=True, exist_ok=True)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

app = FastAPI(title=APP_TITLE)


def db_conn():
    conn = sqlite3.connect(BUNDLES_DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bundles (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              description TEXT,
              bounds TEXT,              -- JSON [minLon,minLat,maxLon,maxLat]
              source_key TEXT NOT NULL, -- s3 object key
              pmtiles_key TEXT NOT NULL,-- s3 object key
              created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )


init_db()


def run(cmd: list[str], cwd: Optional[Path] = None):
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}")
    return p.stdout


def s3_put_file(local_path: Path, key: str, content_type: Optional[str] = None):
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    s3.upload_file(str(local_path), S3_BUCKET, key, ExtraArgs=extra)


def safe_name(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-zA-Z0-9._-]+", "", s)
    return s[:120] if s else "bundle"


def ogrinfo_bounds(dataset_path: Path) -> Optional[list[float]]:
    """
    Try to get bounds via ogrinfo.
    This works for GeoJSON, GPKG, SHP, etc.
    """
    try:
        out = run(["ogrinfo", "-so", "-al", "-json", str(dataset_path)])
        j = json.loads(out)
        def find_extent(obj):
            if isinstance(obj, dict):
                if "extent" in obj and isinstance(obj["extent"], (list, tuple)) and len(obj["extent"]) == 4:
                    return obj["extent"]
                for v in obj.values():
                    r = find_extent(v)
                    if r:
                        return r
            if isinstance(obj, list):
                for v in obj:
                    r = find_extent(v)
                    if r:
                        return r
            return None

        ext = find_extent(j)
        if ext and len(ext) == 4:
            return [float(ext[0]), float(ext[1]), float(ext[2]), float(ext[3])]
    except Exception:
        return None
    return None


def convert_to_geojson(src_path: Path, dst_geojson: Path):
    run([
        "ogr2ogr",
        "-f", "GeoJSON",
        "-t_srs", "EPSG:4326",
        str(dst_geojson),
        str(src_path),
    ])


def tippecanoe_to_mbtiles(geojson_path: Path, mbtiles_path: Path, layer_name: str):
    run([
        "tippecanoe",
        "-o", str(mbtiles_path),
        "-l", layer_name,
        "-zg",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        str(geojson_path),
    ])


def mbtiles_to_pmtiles(mbtiles_path: Path, pmtiles_path: Path):
    run(["pmtiles", "convert", str(mbtiles_path), str(pmtiles_path)])


def ensure_pmtiles(upload_path: Path, bundle_name: str, work: Path) -> tuple[Path, Optional[list[float]]]:
    """
    Returns (pmtiles_path, bounds)
    Handles:
      - .pmtiles  (pass through)
      - .mbtiles  (convert)
      - .geojson/.json (tippecanoe->mbtiles->pmtiles)
      - .zip (assumed shapefile zip or other OGR-supported contents)
      - anything else OGR can read -> ogr2ogr -> geojson -> tippecanoe -> pmtiles
    """
    bounds = None
    ext = upload_path.suffix.lower()

    if ext == ".pmtiles":
        return upload_path, None

    if ext == ".mbtiles":
        pm = work / f"{safe_name(bundle_name)}.pmtiles"
        mbtiles_to_pmtiles(upload_path, pm)
        return pm, None

    src_for_ogr = upload_path
    if ext == ".zip":
        unzip_dir = work / "unzipped"
        unzip_dir.mkdir(exist_ok=True)
        run(["unzip", "-o", str(upload_path), "-d", str(unzip_dir)])

        shp = next(unzip_dir.rglob("*.shp"), None)
        if shp:
            src_for_ogr = shp
        else:
            any_file = next((p for p in unzip_dir.rglob("*") if p.is_file()), None)
            if not any_file:
                raise HTTPException(400, "Zip was empty or contained no files.")
            src_for_ogr = any_file

    bounds = ogrinfo_bounds(src_for_ogr)

    if ext in (".geojson", ".json"):
        geojson = upload_path
    else:
        geojson = work / "converted.geojson"
        convert_to_geojson(src_for_ogr, geojson)

    mb = work / "out.mbtiles"
    pm = work / f"{safe_name(bundle_name)}.pmtiles"
    tippecanoe_to_mbtiles(geojson, mb, layer_name=safe_name(bundle_name))
    mbtiles_to_pmtiles(mb, pm)
    return pm, bounds


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/bundles")
def list_bundles():
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM bundles ORDER BY created_at DESC").fetchall()
    items = []
    for r in rows:
        bounds = json.loads(r["bounds"]) if r["bounds"] else None
        items.append({
            "id": r["id"],
            "name": r["name"],
            "description": r["description"],
            "bounds": bounds,
            "source_key": r["source_key"],
            "pmtiles_key": r["pmtiles_key"],
            "pmtiles_url": f"{PUBLIC_TILES_BASE}/{r['pmtiles_key']}",
            "created_at": r["created_at"],
        })
    return {"items": items}


@app.get("/api/bundles/{bundle_id}")
def get_bundle(bundle_id: str):
    with db_conn() as conn:
        r = conn.execute("SELECT * FROM bundles WHERE id=?", (bundle_id,)).fetchone()
    if not r:
        raise HTTPException(404, "Not found")
    bounds = json.loads(r["bounds"]) if r["bounds"] else None
    return {
        "id": r["id"],
        "name": r["name"],
        "description": r["description"],
        "bounds": bounds,
        "source_key": r["source_key"],
        "pmtiles_key": r["pmtiles_key"],
        "pmtiles_url": f"{PUBLIC_TILES_BASE}/{r['pmtiles_key']}",
        "created_at": r["created_at"],
    }


@app.post("/api/bundles")
async def create_bundle(
    name: str = Form(...),
    description: str = Form(""),
    file: UploadFile = File(...),
):
    bundle_id = str(uuid.uuid4())
    bundle_name = name.strip() or "bundle"
    original_filename = file.filename or "upload"
    original_filename = safe_name(original_filename)

    work = WORKDIR / bundle_id
    work.mkdir(parents=True, exist_ok=True)

    try:
        upload_path = work / original_filename
        with upload_path.open("wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)

        source_key = f"sources/{bundle_id}/{original_filename}"
        s3_put_file(upload_path, source_key, content_type=file.content_type or "application/octet-stream")

        pmtiles_path, bounds = ensure_pmtiles(upload_path, bundle_name=bundle_name, work=work)

        pmtiles_key = f"pmtiles/{bundle_id}/{safe_name(bundle_name)}.pmtiles"
        s3_put_file(pmtiles_path, pmtiles_key, content_type="application/octet-stream")

        with db_conn() as conn:
            conn.execute(
                "INSERT INTO bundles (id,name,description,bounds,source_key,pmtiles_key) VALUES (?,?,?,?,?,?)",
                (bundle_id, bundle_name, description, json.dumps(bounds) if bounds else None, source_key, pmtiles_key),
            )

        return JSONResponse({
            "id": bundle_id,
            "name": bundle_name,
            "description": description,
            "bounds": bounds,
            "source_key": source_key,
            "pmtiles_key": pmtiles_key,
            "pmtiles_url": f"{PUBLIC_TILES_BASE}/{pmtiles_key}",
        })

    except RuntimeError as e:
        raise HTTPException(400, str(e))
    finally:
        shutil.rmtree(work, ignore_errors=True)
