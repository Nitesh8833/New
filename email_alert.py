import io
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import pandas as pd
from google.cloud import storage  # keep if you still use GCS elsewhere

def _df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.read()

def send_email_alert(
    recipient: str,
    subject: str,
    message_body: str,
    html_content: str | None = None,
    *,
    # NEW: attach a DataFrame as an Excel file
    df_attachment: pd.DataFrame | None = None,
    sheet_name: str = "Sheet1",
    attach_filename: str = "report.xlsx",
    # SMTP settings (works for port 25 or TLS on 587)
    smtp_server: str = "extmail.aetna.com",
    smtp_port: int = 25,
    username: str | None = None,
    password: str | None = None,
) -> bool:
    msg = MIMEMultipart("alternative")
    msg["From"] = username or "noreply@yourdomain.com"
    msg["To"] = recipient
    msg["Subject"] = subject

    msg.attach(MIMEText(message_body, "plain"))
    if html_content:
        msg.attach(MIMEText(html_content, "html"))

    # Build attachment from DataFrame (if provided)
    if df_attachment is not None:
        file_bytes = _df_to_excel_bytes(df_attachment, sheet_name=sheet_name)
        part = MIMEApplication(file_bytes,
                               _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        part.add_header("Content-Disposition", "attachment", filename=attach_filename)
        msg.attach(part)

    try:
        if smtp_port == 587 or username:  # TLS path
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            if username and password:
                server.login(username, password)
        else:  # plain port 25
            server = smtplib.SMTP(smtp_server, smtp_port)

        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        import logging
        logging.error(f"Failed to send email: {e}")
        return False



********************************************
# say `result` is your DataFrame
ok = send_email_alert(
    recipient="you@example.com",
    subject="Roster report",
    message_body="Attached is the latest roster report.",
    html_content="<p>Attached is the latest roster report.</p>",
    df_attachment=result,                 # ← attach DF as Excel
    sheet_name="Sheet1",
    attach_filename="Roster_Report.xlsx",
    smtp_server="extmail.aetna.com",
    smtp_port=25,                         # or 587 with TLS + creds below
    # username="your_user@domain.com",
    # password="your_password",
)

************************************************
# report_first_with_email.py

from __future__ import annotations

import io
import re
import logging
import smtplib
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.cloud import storage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"   # DEBUG, INFO, WARNING, ERROR

# SOURCE header  ->  OUTPUT column name
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

# ─────────────────────────────────────────────────────────────────────────────
# HEADER UTILITIES
# ─────────────────────────────────────────────────────────────────────────────
def _normalize(s: str) -> str:
    """Lower/trim and remove spaces/._- for robust header matching."""
    return re.sub(r"[\t\s\.\-_]+", "", str(s).strip().lower())


def _build_renamer(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> Dict[str, str]:
    """Return {real_source_col_in_df: desired_output_col} mapping."""
    norm_to_real = {_normalize(c): c for c in df.columns}
    renamer: Dict[str, str] = {}

    matched = missing = 0
    for src_label, out_col in mapping_src_to_out.items():
        key = _normalize(src_label)
        if key in norm_to_real:
            renamer[norm_to_real[key]] = out_col
            matched += 1
        else:
            missing += 1
            logging.warning("Source column '%s' not found; creating empty '%s'.", src_label, out_col)

    logging.info("[MAP] matched=%d missing=%d", matched, missing)
    return renamer


def extract_and_rename(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> pd.DataFrame:
    """
    Two-arg version:
      - Select & rename by mapping
      - Create any missing output columns as empty
      - Reorder to mapping's output order
      - Light string cleanup
    """
    output_order: List[str] = list(mapping_src_to_out.values())
    renamer = _build_renamer(df, mapping_src_to_out)
    selected = df[list(renamer.keys())].rename(columns=renamer) if renamer else pd.DataFrame()

    for out_col in output_order:
        if out_col not in selected.columns:
            selected[out_col] = pd.NA

    selected = selected[output_order]

    for c in selected.columns:
        if pd.api.types.is_string_dtype(selected[c]):
            selected[c] = selected[c].astype("string").str.strip()

    return selected

# ─────────────────────────────────────────────────────────────────────────────
# GCS HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError("gs_uri must start with 'gs://'")
    path = gs_uri[5:]
    bucket, sep, object_name = path.partition("/")
    if not bucket or not sep or not object_name:
        raise ValueError("Invalid gs_uri, expected 'gs://<bucket>/<object>'")
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
        ext = "." + ext
    else:
        stem, ext = file_, ""

    i = 1
    while True:
        candidate = f"{prefix}{stem}_{i:03d}{ext}"
        if not bkt.blob(candidate).exists(client=client):
            return candidate
        i += 1


def _autosize_and_freeze_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    ws = writer.sheets[sheet_name]
    from openpyxl.utils import get_column_letter

    for idx, col in enumerate(df.columns, start=1):
        s = df[col].astype("string")
        max_cell = int(s.map(lambda x: len(str(x)) if pd.notna(x) else 0).max()) if len(s) else 0
        max_len = max(len(str(col)), max_cell)
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)

    ws.freeze_panes = "A2"


def write_df_to_gcs(
    df: pd.DataFrame,
    *,
    gs_uri: Optional[str] = None,
    bucket: Optional[str] = None,
    object_name: Optional[str] = None,
    fmt: str = "xlsx",
    sheet_name: str = "Sheet1",
    auto_increment: bool = False,
) -> str:
    """
    Upload DataFrame to GCS as CSV/XLSX. Returns the written gs:// URI.
    Supports either gs_uri OR (bucket + object_name).
    """
    client = storage.Client()

    if gs_uri:
        bucket_name, obj_name = _parse_gs_uri(gs_uri)
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
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            _autosize_and_freeze_openpyxl(writer, df, sheet_name)
        bio.seek(0)
        blob.upload_from_file(
            bio,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        raise ValueError("fmt must be 'csv' or 'xlsx'")

    out_uri = f"gs://{bucket_name}/{obj_name}"
    logging.info("[OUT] %s -> %s", fmt.upper(), out_uri)
    return out_uri

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE ENTRY (from in-memory DataFrame)
# ─────────────────────────────────────────────────────────────────────────────
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
    - Uses your in-memory DataFrame (e.g., `result`)
    - Applies mapping (two-arg extract_and_rename)
    - Writes to GCS (gs_uri OR bucket/object)
    - Returns (df_out, written_gs_uri)
    """
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(levelname)s: %(message)s",
        force=True,
    )

    logging.info("[SRC] Using in-memory DataFrame. Rows=%d Cols=%d",
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

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL SENDER
# ─────────────────────────────────────────────────────────────────────────────
def send_email_alert(recipient: str, subject: str, message_body: str, html_content: Optional[str] = None) -> bool:
    """
    Send an email alert using the configured SMTP server with optional HTML formatting.
    """
    sender = "rudra.annangi@aetna.com"  # Use your Aetna email
    msg = MIMEMultipart('alternative')
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject

    # Always attach plain text version
    msg.attach(MIMEText(message_body, 'plain'))

    # Attach HTML version if provided
    if html_content:
        msg.attach(MIMEText(html_content, 'html'))

    try:
        # Aetna email configuration
        smtp_server = "extmail.aetna.com"
        smtp_port = 25
        logging.info(f"Connecting to SMTP server: {smtp_server}:{smtp_port}")
        server = smtplib.SMTP(smtp_server, smtp_port)
        logging.info(f"Sending notification email to {recipient}")
        server.send_message(msg)
        server.quit()
        logging.info(f"Alert email sent to {recipient}")
        return True
    except Exception as e:
        logging.error(f"Failed to send email alert: {e}")
        return False
