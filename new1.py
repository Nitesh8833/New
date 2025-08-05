# df_to_gcs_pipeline.py
from __future__ import annotations

import io
import logging
import re
from typing import Dict, List

import pandas as pd
from google.cloud import storage

# -------------------- EDIT MAPPING ONLY (if needed) --------------------
# SOURCE header -> OUTPUT column name
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

LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR


# ============================== Helpers ===============================

def _normalize(s: str) -> str:
    """Normalize a header for robust matching: lower, strip, remove spaces/_/-/dots."""
    return re.sub(r"[ \t\-\_\.]+", "", str(s).strip().lower())


def build_renamer(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> Dict[str, str]:
    """
    Build a renamer dict {source_col_in_df: output_col_name} using robust matching.
    mapping is SOURCE -> OUTPUT.
    """
    norm_to_real = {_normalize(c): c for c in df.columns}
    renamer: Dict[str, str] = {}
    matched, missing = 0, 0

    for src_label, out_col in mapping_src_to_out.items():
        key = _normalize(src_label)
        if key in norm_to_real:
            renamer[norm_to_real[key]] = out_col
            matched += 1
        else:
            missing += 1
            logging.warning("Source column '%s' not found; will create empty '%s'.", src_label, out_col)

    logging.info("[MAP] matched=%d missing=%d", matched, missing)
    return renamer


def extract_and_rename(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> pd.DataFrame:
    """
    Select & rename per mapping, create missing outputs as empty,
    reorder to output order, and lightly clean strings.
    """
    output_order: List[str] = list(mapping_src_to_out.values())
    renamer = build_renamer(df, mapping_src_to_out)

    selected = df[list(renamer.keys())].rename(columns=renamer) if renamer else pd.DataFrame()

    # ensure all desired outputs exist
    for out_col in output_order:
        if out_col not in selected.columns:
            selected[out_col] = pd.NA

    # reorder
    selected = selected[output_order]

    # light cleanup
    for c in selected.columns:
        if pd.api.types.is_string_dtype(selected[c]):
            selected[c] = selected[c].astype("string").str.strip()

    return selected


def _autosize_and_freeze_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    """Auto-size columns & freeze header (openpyxl)."""
    ws = writer.sheets[sheet_name]
    from openpyxl.utils import get_column_letter

    for idx, col in enumerate(df.columns, start=1):
        series = df[col].astype("string")
        max_cell = int(series.map(lambda x: len(str(x)) if pd.notna(x) else 0).max()) if len(series) else 0
        max_len = max(len(str(col)), max_cell)
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)

    ws.freeze_panes = "A2"


def write_df_to_gcs(df: pd.DataFrame, bucket: str, object_name: str, fmt: str = "xlsx", sheet_name: str = "Sheet1") -> None:
    """
    Write DataFrame directly to GCS as CSV or XLSX (autosized & frozen header).
    Requires ADC or a service account on the machine.
    """
    client = storage.Client()
    blob = client.bucket(bucket).blob(object_name)
    fmt = fmt.lower()

    if fmt == "csv":
        payload = df.to_csv(index=False)
        blob.upload_from_string(payload, content_type="text/csv")
        logging.info("[OUT] CSV -> gs://%s/%s", bucket, object_name)
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
        logging.info("[OUT] XLSX -> gs://%s/%s", bucket, object_name)
    else:
        raise ValueError("Unsupported fmt (use 'csv' or 'xlsx').")


def run_pipeline_from_df(source_df: pd.DataFrame, gcs_bucket: str, gcs_object: str, fmt: str = "xlsx", sheet_name: str = "Sheet1") -> pd.DataFrame:
    """
    Full pipeline: take existing DataFrame (e.g., `result`), transform, and write to GCS.
    Returns transformed DataFrame.
    """
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO), format="%(levelname)s: %(message)s")
    logging.info("[SRC] Using in-memory DataFrame (no DB config). Rows=%d Cols=%d", len(source_df), len(source_df.columns))

    df_out = extract_and_rename(source_df, MAPPING)

    write_df_to_gcs(df_out, bucket=gcs_bucket, object_name=gcs_object, fmt=fmt, sheet_name=sheet_name)
    return df_out
