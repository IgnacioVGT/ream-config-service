from flask import Flask, jsonify, request
import os, yaml, io
from google.cloud import storage
import psycopg2

app = Flask(__name__)

BUCKET = os.environ["RELEASES_BUCKET"]          # p.ej. ream-releases-<project>
DB_HOST = os.environ["DB_HOST"]                 # /cloudsql/<PROJECT:REGION:INSTANCE>
DB_NAME = os.environ.get("DB_NAME","config_demo")
DB_USER = os.environ.get("DB_USER","postgres")
DB_PASS = os.environ["DB_PASS"]

storage_client = storage.Client()

def get_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

@app.get("/releases")
def list_releases():
    blobs = storage_client.list_blobs(BUCKET, prefix="releases/", delimiter="/")
    for _ in blobs:  # obliga a poblar blobs.prefixes
        pass
    ids = sorted([p.split("/")[1] for p in blobs.prefixes])
    return jsonify(ids)

@app.get("/releases/<release_id>")
def get_release(release_id):
    blob = storage_client.bucket(BUCKET).blob(f"releases/{release_id}/release.yaml")
    if not blob.exists():
        return jsonify({"error":"release not found"}), 404
    data = yaml.safe_load(io.BytesIO(blob.download_as_bytes()))
    return jsonify(data)

@app.post("/assign")
def assign_release():
    body = request.get_json(force=True)
    tenant = body.get("tenant_id")
    release = body.get("release_id")
    if not tenant or not release:
        return jsonify({"error":"tenant_id and release_id required"}), 400
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
              INSERT INTO tenant_release_current (tenant_id, release_id, changed_by)
              VALUES (%s, %s, %s)
              ON CONFLICT (tenant_id) DO UPDATE
              SET release_id = EXCLUDED.release_id, changed_at = CURRENT_TIMESTAMP;
            """, (tenant, release, "cloudrun"))
    return jsonify({"status":"ok","message":f"Release {release} asignado a {tenant}"})
