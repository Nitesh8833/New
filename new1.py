# ========= Dataproc Jupyter → Cloud SQL for PostgreSQL =========
# Choose ONE of the two approaches below.
# A) Cloud SQL Python Connector  (no .pem files, easiest)        -> set USE_CONNECTOR = True
# B) Direct TCP + server CA downloaded from GCS (psycopg2)       -> set USE_CONNECTOR = False
# ===============================================================

# %pip install -q cloud-sql-python-connector pg8000 google-cloud-storage psycopg2-binary

import os
import io
import ssl
import tempfile
from pathlib import Path
from typing import Optional

# ---- Config (EDIT THESE) --------------------------------------
PROJECT_ID   = "your-project-id"
REGION       = "us-east4"
INSTANCE_ID  = "your-instance-id"            # e.g. "pdipggsd1"
DB_NAME      = "pdipggsd1_db"
DB_USER      = "pdipggsd1_user"
DB_PASSWORD  = "REPLACE_WITH_DB_PASSWORD"

# If using direct TCP (Option B)
DB_HOST      = "10.247.163.124"              # private or public IP
DB_PORT      = 5432

# If your server CA is in GCS (Option B)
BUCKET_NAME  = "usmedphcb-pdi-intake-devstg"
SERVER_CA_GCS= "prv_rstrcnf_conformed_files/cloudsql_instance/server-ca.pem"

# Switch method here
USE_CONNECTOR = True   # True = Option A (recommended), False = Option B

# ===============================================================
def _mask(s: Optional[str], keep=2) -> str:
    if not s: return ""
    return s if len(s) <= keep*2 else f"{s[:keep]}…{s[-keep:]}"

# -------------------- OPTION A: CONNECTOR ----------------------
def connect_with_connector():
    from google.cloud.sql.connector import Connector, IPTypes
    import pg8000

    icn = f"{PROJECT_ID}:{REGION}:{INSTANCE_ID}"
    connector = Connector()
    conn = connector.connect(
        icn,
        "pg8000",
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        ip_type=IPTypes.PRIVATE,  # or IPTypes.PUBLIC if you use public IP
        # enable_iam_auth=True,   # uncomment if you configured IAM DB auth
    )
    # Keep a handle so we can close it on kernel stop if needed:
    conn._connector = connector
    return conn

# -------------------- OPTION B: DIRECT + GCS CA ----------------
def _download_from_gcs(bucket: str, blob: str, dest: Path) -> Path:
    from google.cloud import storage
    dest.parent.mkdir(parents=True, exist_ok=True)
    storage.Client().bucket(bucket).blob(blob).download_to_filename(dest)
    return dest

def _ensure_valid_pem(path: Path) -> None:
    with open(path, "rb") as f:
        head = f.read(64)
    if b"BEGIN CERTIFICATE" not in head:
        raise ValueError(f"{path} is not a valid PEM (no BEGIN CERTIFICATE).")

def connect_with_psycopg2_via_gcs_ca():
    import psycopg2

    # 1) Download server CA -> local temp file
    local_ca = Path(tempfile.gettempdir()) / "server-ca.pem"
    _download_from_gcs(BUCKET_NAME, SERVER_CA_GCS, local_ca)
    _ensure_valid_pem(local_ca)

    # 2) Connect (PostgreSQL needs ONLY sslrootcert; DO NOT pass sslcert/sslkey)
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        sslmode="verify-ca",
        sslrootcert=str(local_ca),
    )
    return conn

# ----------------------------- MAIN ----------------------------
def test_query(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT version(), now();")
        row = cur.fetchone()
        print("PostgreSQL version:", row[0])
        print("DB time:", row[1])

if __name__ == "__main__":
    print(f"DB: {DB_NAME}  USER: {DB_USER}  PASS: {_mask(DB_PASSWORD)}")
    if USE_CONNECTOR:
        print("Connecting via Cloud SQL Python Connector…")
        conn = connect_with_connector()
    else:
        print("Connecting via direct TCP + server CA from GCS…")
        conn = connect_with_psycopg2_via_gcs_ca()

    try:
        print("Connected. Running test query…")
        test_query(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        # Close the connector if used
        if USE_CONNECTOR and hasattr(conn, "_connector"):
            conn._connector.close()
        print("Connection closed.")
