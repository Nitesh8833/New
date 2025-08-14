# main.py
import os
import subprocess
import logging
import json
import re
import io
import base64

from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from google.cloud import storage
from google.cloud import secretmanager
from pandas.api.types import is_datetime64tz_dtype
from openpyxl.utils import get_column_letter

# --- Global Configuration and Mappings ---

LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR

# SOURCE header- OUTPUT column name
MAPPING: Dict[str, str] = {
    "business_owner": "Business Team",
    "group_type": "Group Team",
    "roster_id": "Roster ID",
    "roster_name": "Provider Entity",
    "parent_transaction_type": "Parent Transaction Type",
    "transaction_type": "Transaction Type",
    "total_rows_with_errors": "Total Number of Rows With Error",
    "critical_error_codes": "Critical Error Codes",
    "error_details": "Error Description",
}


# --- Helper Functions ---

def download_cert_from_gcs(bucket_name, gcs_path, local_path):
    """Downloads a file from a Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.download_to_filename(local_path)


def get_db_connection_with_gcs_certs(
    dbname, user, password, host, port,
    client_cert, client_key, server_ca
):
    """
    Establishes a PostgreSQL database connection using SSL certificates
    that have already been downloaded to local paths.
    """
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
        sslmode='verify-ca',
        sslcert=client_cert,
        sslkey=client_key,
        sslrootcert=server_ca
    )
    return conn


def normalize(s: str) -> str:
    """Lower/trim and remove spaces/. for robust header matching."""
    return re.sub(r'[\t\-\_\. ]+', "", s.strip()).lower()


def build_renamer(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> Dict[str, str]:
    """Return [real source col in df: desired output col]."""
    norm_to_real = {normalize(c): c for c in df.columns}
    renamer: Dict[str, str] = {}
    matched = 0
    missing = 0
    for src_label, out_col in mapping_src_to_out.items():
        key = normalize(src_label)
        if key in norm_to_real:
            renamer[norm_to_real[key]] = out_col
            matched += 1
        else:
            missing += 1
            logging.warning("Source column '%s' not found; creating empty '%s'.", src_label, out_col)
    logging.info(f"[MAP] matched=%d missing=%d", matched, missing)
    return renamer


def extract_and_rename(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> pd.DataFrame:
    """
    Select & rename columns based on a mapping, create missing columns,
    and clean up string data.
    """
    output_order: list[str] = list(mapping_src_to_out.values())
    renamer = build_renamer(df, mapping_src_to_out)
    if renamer:
        selected = df[list(renamer.keys())].rename(columns=renamer)
    else:
        selected = pd.DataFrame()

    for out_col in output_order:
        if out_col not in selected.columns:
            selected[out_col] = pd.NA

    selected = selected[output_order]

    for c in selected.columns:
        if pd.api.types.is_string_dtype(selected[c]):
            selected[c] = selected[c].astype("string").str.strip()

    return selected


def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    """Parses a GCS URI into a bucket and object name."""
    if not gs_uri.startswith("gs://"):
        raise ValueError("gs_uri must start with 'gs://'")
    path = gs_uri[5:]
    bucket, sep, object_name = path.partition("/")
    if not bucket or not sep or not object_name:
        raise ValueError(f"Invalid gs_uri: expected 'gs://<bucket>/<object>'")
    return bucket, object_name


def _next_available_name(client: storage.Client, bucket: str, object_name: str) -> str:
    """If object exists, append _001/_002 before the extension."""
    bkt = client.bucket(bucket)
    if not bkt.blob(object_name).exists(client=client):
        return object_name

    if "/" in object_name:
        dir_, file_ = object_name.rsplit("/", 1)
        prefix = dir_ + "/"
    else:
        prefix, file_ = "", object_name

    if "." in file_:
        stem, ext = file_.rsplit(".", 1)
    else:
        stem, ext = file_, ""
    
    i = 1
    while True:
        candidate = f"{prefix}{stem}_{i:03d}.{ext}" if ext else f"{prefix}{stem}_{i:03d}"
        if not bkt.blob(candidate).exists(client=client):
            return candidate
        i += 1


def autosize_and_freeze_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    """Formats an Excel sheet with column autosizing and frozen panes."""
    ws = writer.sheets[sheet_name]
    for idx, col in enumerate(df.columns, start=1):
        s = df[col].astype("string")
        max_cell = int(s.map(lambda x: len(str(x)) if pd.notna(x) else 0).max())
        max_len = max(len(str(col)), max_cell)
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)
    ws.freeze_panes = "A2"


def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a copy where any timezone-aware datetimes are made timezone-naive
    (kept in UTC, tz info removed).
    """
    out = df.copy()
    for c in out.columns:
        s = out[c]
        if pd.api.types.is_datetime64tz_dtype(s):
            out[c] = s.dt.tz_convert("UTC").dt.tz_localize(None)
            continue
        if s.dtype == "object":
            def fix(x):
                if isinstance(x, pd.Timestamp):
                    if x.tzinfo is not None:
                        try:
                            return x.tz_convert("UTC").tz_localize(None)
                        except Exception:
                            return x.tz_localize(None)
                if isinstance(x, datetime) and x.tzinfo is not None:
                    return x.astimezone(timezone.utc).replace(tzinfo=None)
                return x
            out[c] = s.map(fix)
    return out


def write_df_to_gcs(
    df: pd.DataFrame,
    gs_uri: Optional[str] = None,
    bucket: Optional[str] = None,
    object_name: Optional[str] = None,
    fmt: str = "xlsx",
    sheet_name: str = "Sheet1",
    auto_increment: bool = False,
) -> str:
    """
    Uploads a DataFrame to GCS as CSV/XLSX.
    """
    client = storage.Client()
    if gs_uri:
        bucket_name, obj_name = parse_gs_uri(gs_uri)
    else:
        if not bucket or not object_name:
            raise ValueError("Provide either gs_uri OR (bucket AND object_name).")
        bucket_name, obj_name = bucket, object_name

    if auto_increment:
        obj_name = _next_available_name(client, bucket_name, obj_name)
    blob = client.bucket(bucket_name).blob(obj_name)
    fmt = fmt.lower()

    if fmt == "csv":
        payload = df.to_csv(index=False)
        blob.upload_from_string(payload, content_type="text/csv")
    elif fmt == "xlsx":
        safe_df = make_excel_safe(df)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            safe_df.to_excel(writer, index=False, sheet_name=sheet_name)
            autosize_and_freeze_openpyxl(writer, safe_df, sheet_name)
        bio.seek(0)
        blob.upload_from_file(
            bio,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        raise ValueError(f"fmt must be 'csv' or 'xlsx'")

    out_uri = f"gs://{bucket_name}/{obj_name}"
    logging.info(f"[OUT] %s -> %s", fmt, out_uri)
    return out_uri


def run_pipeline_from_df(
    source_df: pd.DataFrame,
    *,
    gs_uri: Optional[str] = None,
    bucket: Optional[str] = None,
    object_name: Optional[str] = None,
    fmt: str = "xlsx",
    sheet_name: str = "Sheet1",
    auto_increment: bool = False,
) -> Tuple[pd.DataFrame, str]:
    """
    Runs the data processing pipeline: renames columns, and writes the
    resulting DataFrame to GCS.
    """
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(levelname)s: %(message)s",
        force=True,
    )
    logging.info(f"[SRC] Using in-memory DataFrame. Rows=%d Cols=%d",
                 len(source_df), len(source_df.columns))

    df_out = extract_and_rename(source_df, MAPPING)
    written_uri = write_df_to_gcs(
        df_out,
        gs_uri=gs_uri,
        bucket=bucket,
        object_name=object_name,
        fmt=fmt,
        sheet_name=sheet_name,
        auto_increment=auto_increment,
    )
    return df_out, written_uri


# --- Main Execution Block ---

if __name__ == "__main__":
    # --- Database connection details ---
    dbname = 'pdigpgsd1_db'
    user = 'pdigpgsd1_nh'
    password = 'pdigpgsd1_c7#6H_a8wd'
    host = '10.247.163.124'
    port = 5432

    # GCS locations for SSL certificates
    bucket_name = "usmedhcb-pdi-intake-devstg"
    client_cert_gcs = "prv_rstrcnf_conformed_files/cloudsql_instance/client-cert.pem"
    client_key_gcs = "prv_rstrcnf_conformed_files/cloudsql_instance/client-key.pem"
    server_ca_gcs = "prv_rstrcnf_conformed_files/cloudsql_instance/server-ca.pem"

    # Define temporary local paths for downloaded certificates
    local_certs_dir = "/tmp/certs"
    os.makedirs(local_certs_dir, exist_ok=True)
    local_client_cert = os.path.join(local_certs_dir, "client-cert.pem")
    local_client_key = os.path.join(local_certs_dir, "client-key.pem")
    local_server_ca = os.path.join(local_certs_dir, "server-ca.pem")

    # Download certs from GCS
    download_cert_from_gcs(bucket_name, client_cert_gcs, local_client_cert)
    download_cert_from_gcs(bucket_name, client_key_gcs, local_client_key)
    download_cert_from_gcs(bucket_name, server_ca_gcs, local_server_ca)

    # --- Query and Data Loading ---
    query = f"SELECT * FROM pdipp.prvrostercnf_conformed_file_stats"
    
    # Establish DB connection and run query
    with get_db_connection_with_gcs_certs(
        dbname, user, password, host, port,
        local_client_cert, local_client_key, local_server_ca
    ) as conn:
        result = pd.read_sql_query(query, con=conn)

    print(result)

    # --- Data Processing and GCS Upload ---
    GS_URI = "gs://pdi-prvrstr-stg-hcb-dev/report_stg/output.xlsx"
    FMT = "xlsx"

    df_final, written = run_pipeline_from_df(
        result,
        gs_uri=GS_URI,
        fmt="xlsx",
        sheet_name="converted",
        auto_increment=True
    )
    
    print("\nWritten to GCS URI:")
    print(written)

******************************************************


# --- FIXED callable: pull operator kwargs correctly and handle missing keys gracefully ---
def _get_op_kwargs(kwargs: dict) -> dict:
    """Return the keyword arguments passed to PythonOperator.op_kwargs.
    Airflow passes them directly as kwargs; older code may expect kwargs['op_kwargs'].
    """
    if "op_kwargs" in kwargs and isinstance(kwargs["op_kwargs"], dict):
        return kwargs["op_kwargs"]
    # In Airflow 2.x, op_kwargs are merged into **kwargs, so just return kwargs minus Airflow context keys.
    # Strip some common context keys so we don't accidentally treat them as pipeline configs.
    context_keys = {
        "ti", "task_instance", "ds", "ts", "dag", "dag_run", "run_id", "logical_date",
        "execution_date", "prev_ds", "next_ds", "data_interval_start", "data_interval_end",
        "macros", "conf", "params"
    }
    return {k: v for k, v in kwargs.items() if k not in context_keys}

def _run_pipeline(**kwargs):
    """
    Python function to be executed by the PythonOperator.
    Downloads certificates, connects to DB, runs processing, and uploads result.
    """
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO), format="%(levelname)s: %(message)s", force=True)

    # Pull operator kwargs safely (works whether or not they're nested under 'op_kwargs')
    op = _get_op_kwargs(kwargs)

    # Validate required keys early with readable errors
    required = [
        "bucket_name", "client_cert_gcs", "client_key_gcs", "server_ca_gcs",
        "dbname", "user", "password", "host", "port",
        "output_gs_uri", "output_fmt", "output_sheet_name", "output_auto_increment"
    ]
    missing = [k for k in required if k not in op]
    if missing:
        raise ValueError(f"Missing required op_kwargs: {missing}")

    # Define local paths for the downloaded certificates
    local_certs_dir = "/tmp/certs"
    os.makedirs(local_certs_dir, exist_ok=True)
    local_client_cert = os.path.join(local_certs_dir, "client-cert.pem")
    local_client_key = os.path.join(local_certs_dir, "client-key.pem")
    local_server_ca = os.path.join(local_certs_dir, "server-ca.pem")

    # Download certs from GCS
    download_cert_from_gcs(op["bucket_name"], op["client_cert_gcs"], local_client_cert)
    download_cert_from_gcs(op["bucket_name"], op["client_key_gcs"], local_client_key)
    download_cert_from_gcs(op["bucket_name"], op["server_ca_gcs"], local_server_ca)

    # Establish DB connection and run query
    with get_db_connection_with_gcs_certs(
        op["dbname"], op["user"], op["password"], op["host"], op["port"],
        local_client_cert, local_client_key, local_server_ca
    ) as conn:
        query = "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats"
        result_df = pd.read_sql_query(query, con=conn)

    # Run the processing and writing pipeline
    df_final, written_uri = run_pipeline_from_df(
        result_df,
        gs_uri=op["output_gs_uri"],
        fmt=op["output_fmt"],
        sheet_name=op["output_sheet_name"],
        auto_increment=op["output_auto_increment"]
    )

    logging.info("Pipeline finished. Final output URI: %s", written_uri)
    # Returning a string will push it to XCom automatically
    return written_uri


*****************************************

def _run_pipeline(**kwargs):
    """
    Python function to be executed by the PythonOperator.
    This function accepts a single `kwargs` dictionary and unpacks the
    necessary variables from it, providing a robust way to
    handle Airflow's keyword argument passing.
    """
    # Unpack parameters from kwargs
    dbname = kwargs.get('dbname')
    user = kwargs.get('user')
    password = kwargs.get('password')
    host = kwargs.get('host')
    port = kwargs.get('port')
    bucket_name = kwargs.get('bucket_name')
    client_cert_gcs = kwargs.get('client_cert_gcs')
    client_key_gcs = kwargs.get('client_key_gcs')
    server_ca_gcs = kwargs.get('server_ca_gcs')
    output_gs_uri = kwargs.get('output_gs_uri')
    output_fmt = kwargs.get('output_fmt')
    output_sheet_name = kwargs.get('output_sheet_name')
    output_auto_increment = kwargs.get('output_auto_increment')

    # It's a good practice to handle missing keys gracefully with .get()
    # or with a default value to prevent a KeyError.

    # --- Rest of your existing function logic below ---
    local_certs_dir = "/tmp/certs"
    os.makedirs(local_certs_dir, exist_ok=True)
    local_client_cert = os.path.join(local_certs_dir, "client-cert.pem")
    local_client_key = os.path.join(local_certs_dir, "client-key.pem")
    local_server_ca = os.path.join(local_certs_dir, "server-ca.pem")

    # Download certs from GCS
    download_cert_from_gcs(bucket_name, client_cert_gcs, local_client_cert)
    download_cert_from_gcs(bucket_name, client_key_gcs, local_client_key)
    download_cert_from_gcs(bucket_name, server_ca_gcs, local_server_ca)

    # Establish DB connection and run query
    with get_db_connection_with_gcs_certs(
        dbname, user, password, host, port,
        local_client_cert, local_client_key, local_server_ca
    ) as conn:
        query = f"SELECT * FROM pdipp.prvrostercnf_conformed_file_stats"
        result_df = pd.read_sql_query(query, con=conn)
        
    # Run the processing and writing pipeline
    df_final, written_uri = run_pipeline_from_df(
        result_df,
        gs_uri=output_gs_uri,
        fmt=output_fmt,
        sheet_name=output_sheet_name,
        auto_increment=output_auto_increment
    )
    
    print(f"Pipeline finished. Final output URI: {written_uri}")
    return written_uri
