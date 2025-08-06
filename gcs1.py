#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Roster KPI Pipeline (GCS only, no local file I/O):
  - Source: uses in-memory DataFrame `result` (from your SQL cell).
  - Preserves **all columns** from `result` and appends KPI columns.
  - Robust fuzzy column finding (e.g., 'Roster ID' vs 'roster_id').
  - Writes a single Excel sheet to **GCS** directly from memory (no temp files).
  - Excel-safe: removes timezone from datetimes and avoids NaT errors.

Usage:
  1) Ensure your notebook has a DataFrame named `result`.
  2) Set OUT_URI below to a GCS path like: gs://your-bucket/folder/roster_output_kpi.xlsx
  3) Run this module (or call `run_pipeline_to_gcs(OUT_URI)`).
"""

from __future__ import annotations

import io
import re
from typing import Optional, Dict, List, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------
# USER PARAMETERS (GCS only)
# ---------------------------------------------------------------------
OUT_URI = "gs://your-bucket/folder/roster_output_kpi.xlsx"  # <-- set your bucket/key
SHEET_NAME = "output"

# Optional friendly display names (keeps all other columns unchanged)
FRIENDLY_HEADERS: Dict[str, str] = {
    "business_owner": "Business Team",
    "group_type": "Group Type",
    "roster_id": "Roster ID",
    "roster_name": "Provider Entity",
    "parent_transaction_type": "Parent Transaction Type",
    "transaction_type": "Transaction Type",
}

# ---------------------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------------------

def _norm(s: str) -> str:
    """Lowercase + remove non-alphanumerics for robust matching."""
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())


def find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Find a matching column in df using exact-normalized then substring-normalized match."""
    norm_map = {_norm(c): c for c in df.columns}
    for cand in candidates:
        key = _norm(cand)
        if key in norm_map:
            return norm_map[key]
    for cand in candidates:
        ck = _norm(cand)
        for col in df.columns:
            if ck and ck in _norm(col):
                return col
    return None


def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Remove timezone info; replace NaT with blank; keep everything else as-is."""
    out = df.copy()
    # True tz-aware dtypes
    for col in out.select_dtypes(include=["datetimetz"]).columns:
        out[col] = out[col].dt.tz_localize(None)
    # Objects that may contain tz-aware datetimes
    for col in out.columns[out.dtypes.eq("object")]:
        s = out[col]
        try:
            has_tz = s.map(lambda x: getattr(x, "tzinfo", None) is not None).any()
        except Exception:
            has_tz = False
        if has_tz:
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True).dt.tz_localize(None)
    # Replace NaT for Excel
    out = out.replace({pd.NaT: ""})
    return out


def autosize_and_freeze_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    """Auto-size columns & freeze header row (safe for pd.NA)."""
    ws = writer.sheets[sheet_name]
    from openpyxl.utils import get_column_letter

    for idx, col in enumerate(df.columns, start=1):
        series = df[col].astype("string").fillna("")
        max_len = max(series.map(len).max() or 0, len(str(col))) + 2
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len, 60)
    ws.freeze_panes = "A2"


# ---------------------------------------------------------------------
# KPI HELPERS (operate on full df without dropping any columns)
# ---------------------------------------------------------------------

def _version_status_series(df: pd.DataFrame) -> Optional[pd.Series]:
    col = find_col(df, ["version_status", "Version Status", "status"])  # flexible naming
    if not col:
        return None
    return (df[col].astype("string").str.strip()
            .str.replace(r"[^\s\w\-]+", "", regex=True).str.upper())


def _roster_id_col(df: pd.DataFrame) -> Optional[str]:
    return find_col(df, ["roster_id", "Roster ID", "roster id", "rosterid"])  # common variants


def add_new_roster_formats(df: pd.DataFrame,
                           include_statuses: Tuple[str, ...] = ("NEW_FILE", "NEW_VERSION", "ACTIVE")) -> pd.DataFrame:
    out = df.copy()
    rid = _roster_id_col(out)
    if not rid:
        out["# New Roster Formats"] = 0
        return out
    vs = _version_status_series(out)
    if vs is None:
        out["# New Roster Formats"] = 0
        return out
    statuses = tuple(s.upper().replace(" ", "_").replace("-", "_") for s in include_statuses)
    is_new = vs.isin(statuses)
    counts = is_new.fillna(False).groupby(out[rid]).transform("sum")
    out["# New Roster Formats"] = counts.fillna(0).astype(int)
    return out


def add_changed_roster_formats(df: pd.DataFrame) -> pd.DataFrame:
    """# Changed Roster Formats = count of NEW_VERSION events where previous for same roster wasn't NEW_VERSION."""
    out = df.copy()
    rid = _roster_id_col(out)
    if not rid:
        out["# Changed Roster Formats"] = 0
        return out
    vs = _version_status_series(out)
    if vs is None:
        out["# Changed Roster Formats"] = 0
        return out
    prev_vs = vs.groupby(out[rid]).shift(1)
    changed = vs.eq("NEW_VERSION") & prev_vs.notna() & (prev_vs != "NEW_VERSION")
    counts = changed.fillna(False).groupby(out[rid]).transform("sum")
    out["# Changed Roster Formats"] = counts.fillna(0).astype(int)
    return out


def add_no_setup_or_format_change(df: pd.DataFrame,
                                   new_col_name: str = "# of Rosters with no Set up or Format Change") -> pd.DataFrame:
    out = df.copy()
    rid = _roster_id_col(out)
    if not rid:
        out[new_col_name] = 0
        return out
    ids = out[rid]
    unique_counts = ids.groupby(ids).transform(lambda s: s.dropna().nunique())
    out[new_col_name] = (unique_counts <= 1).fillna(False).astype(int)
    return out


def add_complex_rosters(df: pd.DataFrame,
                        complexity_candidates: Tuple[str, ...] = ("complexity", "Complexity", "complex"),
                        include_values: Tuple[str, ...] = ("COMPLEX",),
                        new_col_name: str = "# Complex Rosters") -> pd.DataFrame:
    out = df.copy()
    rid = _roster_id_col(out)
    if not rid:
        out[new_col_name] = 0
        return out
    cx_col = find_col(out, list(complexity_candidates))
    if not cx_col:
        out[new_col_name] = 0
        return out
    cx = (out[cx_col].astype("string").str.strip()
          .str.replace(r"[^\s\w\-]+", "", regex=True).str.upper())
    target = {v.upper().replace(" ", "_").replace("-", "_") for v in include_values}
    is_cx = cx.isin(target)
    counts = is_cx.fillna(False).groupby(out[rid]).transform("sum")
    out[new_col_name] = counts.fillna(0).astype(int)
    return out


def add_all_rosters(df: pd.DataFrame, new_col_name: str = "All Rosters") -> pd.DataFrame:
    out = df.copy()
    rid = _roster_id_col(out)
    if not rid:
        out[new_col_name] = 1
        return out
    counts = out[rid].value_counts(dropna=False)
    out[new_col_name] = out[rid].map(counts).astype(int)
    return out


def add_conformance_tat(df: pd.DataFrame) -> pd.DataFrame:
    """Conformance TAT = (end - start) as DD/HH/MM/SS; robust to tz."""
    out = df.copy()
    start_candidates = [
        "prms_posted_timestamp", "prms_posted_time", "posted_timestamp",
        "file_dropped_time", "file_dropped_timestamp", "prms_posted_ts",
    ]
    end_candidates = [
        "file_ingestion_timestamp", "ingestion_timestamp", "file_ingested_timestamp",
        "file_ingestion_time", "file_returned_timestamp", "update_timestamp",
        "processed_timestamp", "file_returned_time",
    ]
    start_col = find_col(out, start_candidates)
    end_col = find_col(out, end_candidates)
    if not start_col or not end_col:
        out["Conformance TAT"] = None
        return out
    out[start_col] = out[start_col].replace(["NaT", "None", ""], pd.NA)
    out[end_col] = out[end_col].replace(["NaT", "None", ""], pd.NA)
    start = pd.to_datetime(out[start_col], errors="coerce", utc=True).dt.tz_localize(None)
    end = pd.to_datetime(out[end_col], errors="coerce", utc=True).dt.tz_localize(None)
    tat = (end - start).round("S")
    tat = tat.where(tat >= pd.Timedelta(0))
    comps = tat.dt.components
    tat_str = (
        comps["days"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["hours"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["minutes"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["seconds"].astype("Int64").astype(str).str.zfill(2)
    ).where(~tat.isna(), None)
    out["Conformance TAT"] = tat_str
    return out


def add_rows_counts(df_src: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    col_in = find_col(df_src, ["input_rec_count", "input records count"])
    col_out = find_col(df_src, ["conformed_rec_count", "output_rec_count",
                                "conformed records count", "output records count"])
    out["# of rows in"] = df_src[col_in] if col_in else len(df_src)
    out["# of rows out"] = df_src[col_out] if col_out else len(out)
    return out


def add_unique_npi_counts(df_src: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    in_cnt = find_col(df_src, ["input_unique_npi_count", "unique npi input"]) 
    out_cnt = find_col(df_src, ["output_unique_npi_count", "unique npi output"]) 
    if in_cnt:
        out["# of unique NPI's in Input"] = df_src[in_cnt]
    else:
        npi_in = find_col(df_src, ["npi", "npi_number", "npi id", "npiid"]) 
        out["# of unique NPI's in Input"] = df_src[npi_in].nunique(dropna=True) if npi_in else pd.NA
    if out_cnt:
        out["# of unique NPI's in Output"] = df_src[out_cnt]
    else:
        npi_out = find_col(out, ["npi", "npi_number", "npi id", "npiid"]) 
        out["# of unique NPI's in Output"] = out[npi_out].nunique(dropna=True) if npi_out else pd.NA
    return out


# ---------------------------------------------------------------------
# FRIENDLY HEADERS + GCS OUTPUT
# ---------------------------------------------------------------------

def apply_friendly_headers(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """Rename only mapped columns; preserve all others and their order."""
    renames = {src: dst for src, dst in mapping.items() if src in df.columns}
    return df.rename(columns=renames)


def write_excel_to_gcs(df: pd.DataFrame, gcs_uri: str, sheet_name: str = SHEET_NAME) -> None:
    """Write DataFrame to an in-memory Excel and upload to GCS (no local files)."""
    try:
        from google.cloud import storage
    except ImportError as e:
        raise ImportError("Install google-cloud-storage to upload to GCS: pip install google-cloud-storage") from e

    # Create workbook in-memory
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        from openpyxl.utils import get_column_letter  # for autosize
        # Auto-size & freeze panes
        ws = writer.sheets[sheet_name]
        for idx, col in enumerate(df.columns, start=1):
            series = df[col].astype("string").fillna("")
            max_len = max(series.map(len).max() or 0, len(str(col))) + 2
            ws.column_dimensions[get_column_letter(idx)].width = min(max_len, 60)
        ws.freeze_panes = "A2"
    buffer.seek(0)

    # Upload buffer to GCS
    if not gcs_uri.startswith("gs://"):
        raise ValueError("OUT_URI must start with 'gs://'")
    _, remainder = gcs_uri.split("gs://", 1)
    bucket_name, blob_name = remainder.split("/", 1)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(buffer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    print(f"[INFO] Uploaded Excel to {gcs_uri}")


# ---------------------------------------------------------------------
# MAIN PIPELINE (preserve all columns, add KPIs, write to GCS)
# ---------------------------------------------------------------------

def run_pipeline_to_gcs(out_uri: str = OUT_URI) -> pd.DataFrame:
    # 1) Source from in-memory `result`
    if "result" not in globals():
        raise ValueError("DataFrame 'result' not found. Ensure your SQL cell defines it.")
    src = globals()["result"].copy()
    print(f"[INFO] Using DataFrame 'result' with shape={src.shape}")

    # 2) Preserve all columns and add KPIs progressively
    df = src.copy()
    df = add_new_roster_formats(df)
    df = add_changed_roster_formats(df)
    df = add_no_setup_or_format_change(df)
    df = add_complex_rosters(df)
    df = add_all_rosters(df)
    df = add_conformance_tat(df)
    df = add_rows_counts(src, df)
    df = add_unique_npi_counts(src, df)

    # 3) Optional friendly display names (only for known columns)
    df = apply_friendly_headers(df, FRIENDLY_HEADERS)

    # 4) Excel-safe and upload to GCS
    df = make_excel_safe(df)
    write_excel_to_gcs(df, out_uri, sheet_name=SHEET_NAME)
    return df


if __name__ == "__main__":
    run_pipeline_to_gcs(OUT_URI)
