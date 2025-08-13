import os
import logging
import re
import io
from typing import Dict, Optional, Tuple
import pandas as pd
import psycopg2
from google.cloud import storage
from google.cloud import secretmanager
from openpyxl.utils import get_column_letter
from datetime import datetime, timezone

# --- Global Configuration ---
LOG_LEVEL = "INFO"
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

# Initialize logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,
)

# --- Helper Functions ---
def download_cert_from_gcs(bucket_name: str, gcs_path: str, local_path: str):
    """Downloads a file from Google Cloud Storage."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.download_to_filename(local_path)
        logging.info(f"Downloaded {gcs_path} to {local_path}")
    except Exception as e:
        logging.error(f"Error downloading {gcs_path}: {str(e)}")
        raise

def get_db_connection(
    dbname: str, 
    user: str, 
    password: str, 
    host: str, 
    port: int,
    bucket_name: str, 
    client_cert_gcs: str, 
    client_key_gcs: str, 
    server_ca_gcs: str
) -> psycopg2.extensions.connection:
    """Establishes a PostgreSQL connection using SSL certificates from GCS."""
    try:
        # Create temporary files
        temp_dir = "/tmp/certs"
        os.makedirs(temp_dir, exist_ok=True)
        
        client_cert = f"{temp_dir}/client-cert.pem"
        client_key = f"{temp_dir}/client-key.pem"
        server_ca = f"{temp_dir}/server-ca.pem"
        
        # Download certificates
        download_cert_from_gcs(bucket_name, client_cert_gcs, client_cert)
        download_cert_from_gcs(bucket_name, client_key_gcs, client_key)
        download_cert_from_gcs(bucket_name, server_ca_gcs, server_ca)
        
        # Establish connection
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
        logging.info("Successfully connected to database")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise

def normalize(s: str) -> str:
    """Normalizes strings for robust header matching."""
    return re.sub(r'[\t\-\_\. ]+', "", s.strip()).lower()

def build_renamer(df: pd.DataFrame, mapping: Dict[str, str]) -> Dict[str, str]:
    """Creates column renaming dictionary."""
    norm_to_real = {normalize(c): c for c in df.columns}
    renamer = {}
    for src_label, out_col in mapping.items():
        key = normalize(src_label)
        if key in norm_to_real:
            renamer[norm_to_real[key]] = out_col
        else:
            logging.warning(f"Source column '{src_label}' not found; creating empty '{out_col}'")
    return renamer

def extract_and_rename(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """Selects & renames columns based on mapping."""
    renamer = build_renamer(df, mapping)
    if renamer:
        selected = df[list(renamer.keys())].rename(columns=renamer)
    else:
        selected = pd.DataFrame()
    
    # Create missing columns
    for out_col in mapping.values():
        if out_col not in selected.columns:
            selected[out_col] = pd.NA
    
    # Clean string columns
    for col in selected.columns:
        if pd.api.types.is_string_dtype(selected[col]):
            selected[col] = selected[col].astype("string").str.strip()
    
    return selected[mapping.values()]

def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    """Parses GCS URI into bucket and object name."""
    if not gs_uri.startswith("gs://"):
        raise ValueError("gs_uri must start with 'gs://'")
    path = gs_uri[5:]
    bucket, _, object_name = path.partition("/")
    if not bucket or not object_name:
        raise ValueError(f"Invalid gs_uri: {gs_uri}")
    return bucket, object_name

def autosize_and_freeze_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str):
    """Formats Excel sheet with column autosizing and frozen panes."""
    ws = writer.sheets[sheet_name]
    for idx, col in enumerate(df.columns, start=1):
        max_len = max(
            len(str(col)),
            df[col].astype("string").map(len).max()
        )
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)
    ws.freeze_panes = "A2"

def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Converts timezone-aware datetimes to naive UTC."""
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64tz_dtype(out[col]):
            out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return out

def write_to_gcs(
    df: pd.DataFrame,
    gs_uri: str,
    fmt: str = "xlsx",
    sheet_name: str = "Sheet1"
) -> str:
    """Uploads DataFrame to GCS as CSV or XLSX."""
    try:
        client = storage.Client()
        bucket_name, object_name = parse_gs_uri(gs_uri)
        bucket = client.bucket(bucket_name)
        
        # Generate unique filename with timestamp
        base_name, ext = os.path.splitext(object_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_name = f"{base_name}_{timestamp}{ext}"
        blob = bucket.blob(unique_name)
        
        if fmt.lower() == "csv":
            content = df.to_csv(index=False)
            blob.upload_from_string(content, content_type="text/csv")
        else:  # xlsx
            safe_df = make_excel_safe(df)
            with io.BytesIO() as output:
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    safe_df.to_excel(writer, index=False, sheet_name=sheet_name)
                    autosize_and_freeze_openpyxl(writer, safe_df, sheet_name)
                output.seek(0)
                blob.upload_from_file(
                    output, 
                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        output_uri = f"gs://{bucket_name}/{unique_name}"
        logging.info(f"Uploaded file to {output_uri}")
        return output_uri
    except Exception as e:
        logging.error(f"Error uploading to GCS: {str(e)}")
        raise

def run_pipeline():
    """Main pipeline execution function."""
    # Retrieve secrets from environment variables
    dbname = os.environ["DB_NAME"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    host = os.environ["DB_HOST"]
    port = int(os.environ["DB_PORT"])
    
    bucket_name = os.environ["CERT_BUCKET"]
    client_cert_gcs = os.environ["CLIENT_CERT_PATH"]
    client_key_gcs = os.environ["CLIENT_KEY_PATH"]
    server_ca_gcs = os.environ["SERVER_CA_PATH"]
    
    gs_uri = os.environ["OUTPUT_GS_URI"]
    
    try:
        # Database connection
        conn = get_db_connection(
            dbname, user, password, host, port,
            bucket_name, client_cert_gcs, client_key_gcs, server_ca_gcs
        )
        
        # Execute query
        query = "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats"
        df = pd.read_sql_query(query, conn)
        conn.close()
        logging.info(f"Retrieved {len(df)} records from database")
        
        # Process data
        processed_df = extract_and_rename(df, MAPPING)
        
        # Write to GCS
        output_uri = write_to_gcs(processed_df, gs_uri, fmt="xlsx", sheet_name="conformed_report")
        logging.info(f"Pipeline completed successfully. Output: {output_uri}")
        return output_uri
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_pipeline()
