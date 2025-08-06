#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ==== Roster KPI pipeline (DataFrame -> canonical -> KPIs -> friendly headers -> Excel/CSV local or GCS) ====

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import numpy as np
import pandas as pd


# ------------------------------------------------------------
# USER PARAMETERS (edit these)
# ------------------------------------------------------------
# Write to LOCAL path (example) or to GCS (set to "gs://bucket/folder/roster_output_kpi.xlsx")
OUT_URI = r"C:\Users\nitesh.kumar_spicemo\Downloads\roster_output_kpi.xlsx"

# If you prefer to read from a file (Excel/CSV) instead of in-notebook `result`, set SRC_PATH:
SRC_PATH: Optional[str] = None  # e.g. r"C:\path\to\input.xlsx" or None to use `result`
SHEET: Optional[str] = None     # if reading Excel and you need a specific sheet

# Mapping from SOURCE labels -> CANONICAL output column names.
MAPPING: Dict[str, str] = {
    "business_owner": "business_owner",
    "group_type": "group_type",
    "Roster ID": "roster_id",
    "roster_name": "roster_name",
    "parent_transaction_type": "parent_transaction_type",
    "transaction_type": "transaction_type",
}

# Friendly names for the final Excel (canonical -> display name)
FRIENDLY_HEADERS: Dict[str, str] = {
    "business_owner": "Business Team",
    "group_type": "Group Type",
    "roster_id": "Roster ID",
    "roster_name": "Provider Entity",
    "parent_transaction_type": "Parent Transaction Type",
    "transaction_type": "Transaction Type",

    # KPIs added by the pipeline:
    "# New Roster Formats": "# New Roster Formats",
    "# Changed Roster Formats": "# Changed Roster Formats",
    "# of Rosters with no Set up or Format Change": "# of Rosters with no Set up or Format Change",
    "# Complex Rosters": "# Complex Rosters",
    "All Rosters": "All Rosters",
    "Conformance TAT": "Conformance TAT",
    "# of rows in": "# of rows in",
    "# of rows out": "# of rows out",
    "# of unique NPI's in Input": "# of unique NPI's in Input",
    "# of unique NPI's in Output": "# of unique NPI's in Output",
}


# ------------------------------------------------------------
# UTILS
# ------------------------------------------------------------
def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make a DataFrame safe to write to Excel:
      * Remove timezone from tz-aware datetime columns
      * Convert tz-aware 'object' cells to tz-naive datetimes
      * Replace NaT with blanks (Excel can't display NaT)
    """
    out = df.copy()

    # True tz-aware dtype columns
    for col in out.select_dtypes(include=["datetimetz"]).columns:
        out[col] = out[col].dt.tz_localize(None)

    # Object columns that might hold tz-aware python datetimes
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


def _norm(s: str) -> str:
    """Lowercase and strip non-alphanumeric for fuzzy matching of column names."""
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())


def _normalize(s: str) -> str:
    """Trim and shrink whitespace for display normalization."""
    return re.sub(r"\s+", " ", str(s).strip()).lower()


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Find a column in df that matches any candidate by normalized name.
    Tries exact normalized match first; then contains-substring match.
    """
    norm_map = {_norm(c): c for c in df.columns}

    # exact normalized match
    for cand in candidates:
        k = _norm(cand)
        if k in norm_map:
            return norm_map[k]

    # relaxed: candidate token contained in column token
    for cand in candidates:
        ck = _norm(cand)
        for col in df.columns:
            if ck and ck in _norm(col):
                return col
    return None


def read_source(path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    """
    Read CSV/Excel into a DataFrame. If sheet is provided, use it for Excel.
    """
    ext = Path(path).suffix.lower()
    if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        print(f"[INFO] Reading Excel: {path} (sheet={sheet})")
        return pd.read_excel(path, sheet_name=sheet) if sheet else pd.read_excel(path)
    elif ext == ".csv":
        print(f"[INFO] Reading CSV: {path}")
        return pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported source extension '{ext}'. Use .xlsx or .csv")


# ------------------------------------------------------------
# COLUMN MAPPING (source -> canonical)
# ------------------------------------------------------------
def build_renamer(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> Dict[str, str]:
    """
    Build a {src_real_col: canonical_out_col} rename dict by normalized matching.
    Prints DEBUG for matched and WARN for missing.
    """
    norm_to_real = {_normalize(c): c for c in df.columns}
    renamer: Dict[str, str] = {}
    matched = 0
    missing = 0

    for src_label, out_col in mapping_src_to_out.items():
        norm_key = _normalize(src_label)
        if norm_key in norm_to_real:
            src_real = norm_to_real[norm_key]
            renamer[src_real] = out_col
            matched += 1
            print(f"[DEBUG] Matched source '{src_label}' -> '{src_real}'  as output '{out_col}'")
        else:
            missing += 1
            print(f"[WARN] Source column '{src_label}' not found. Output '{out_col}' will be empty.")

    print(f"[INFO] Mapping summary: matched={matched}, missing={missing}")
    return renamer


def extract_and_rename(
    df: pd.DataFrame,
    mapping_src_to_out: Dict[str, str],
    output_order: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Select only the mapped columns from 'df' and rename them to their canonical names.
    Ensures all columns in output_order exist (filled with NA if missing) and orders them.
    """
    renamer = build_renamer(df, mapping_src_to_out)
    selected = df[list(renamer.keys())].rename(columns=renamer) if renamer else pd.DataFrame()

    if output_order is None:
        output_order = list(mapping_src_to_out.values())

    for out_col in output_order:
        if out_col not in selected.columns:
            selected[out_col] = pd.NA

    selected = selected[output_order]

    # Strip whitespace in string columns
    for c in selected.columns:
        if pd.api.types.is_string_dtype(selected[c]):
            selected[c] = selected[c].astype("string").str.strip()

    return selected.reset_index(drop=True)


# ------------------------------------------------------------
# KPI HELPERS
# ------------------------------------------------------------
def _version_status_series(df_src: pd.DataFrame, df_out: pd.DataFrame) -> Optional[pd.Series]:
    """
    Locate and normalize a version-status column from the source (or output if needed).
    Returns an UPPERCASED Series like ["NEW_FILE", "NEW_VERSION", "ACTIVE", ...]
    """
    col = _find_col(df_src, ["version_status", "Version Status", "status"])
    if not col or len(df_src) != len(df_out):
        return None
    return (df_src[col].astype("string").str.strip()
            .str.replace(r"[^\s\w\-]+", "", regex=True).str.upper())


# ------------------------------------------------------------
# KPI COMPUTATIONS
# ------------------------------------------------------------
def add_new_roster_formats(
    df_src: pd.DataFrame,
    df_out: pd.DataFrame,
    roster_id_out_col: str = "roster_id",
    include_statuses: Tuple[str, ...] = ("NEW_FILE", "NEW_VERSION", "ACTIVE"),
) -> pd.DataFrame:
    """Per-roster_id count of rows where version_status is in include_statuses."""
    out = df_out.copy().reset_index(drop=True)
    if roster_id_out_col not in out.columns:
        out["# New Roster Formats"] = 0
        return out

    vs = _version_status_series(df_src, out)
    if vs is None:
        out["# New Roster Formats"] = 0
        return out

    statuses = tuple(s.upper().replace(" ", "_").replace("-", "_") for s in include_statuses)
    is_new = vs.isin(statuses)
    counts_per_id = is_new.fillna(False).groupby(out[roster_id_out_col]).transform("sum")
    out["# New Roster Formats"] = counts_per_id.fillna(0).astype(int)
    return out


def add_changed_roster_formats(
    df_src: pd.DataFrame,
    df_out: pd.DataFrame,
    roster_id_out_col: str = "roster_id",
) -> pd.DataFrame:
    """
    Count rows where current version_status == NEW_VERSION and previous status (same roster) was not NEW_VERSION.
    """
    out = df_out.copy().reset_index(drop=True)
    if roster_id_out_col not in out.columns:
        out["# Changed Roster Formats"] = 0
        return out

    vs = _version_status_series(df_src, out)
    if vs is None:
        out["# Changed Roster Formats"] = 0
        return out

    prev_vs = vs.groupby(out[roster_id_out_col]).shift(1)
    change_event = vs.eq("NEW_VERSION") & prev_vs.notna() & (prev_vs != "NEW_VERSION")
    counts_per_id = change_event.fillna(False).groupby(out[roster_id_out_col]).transform("sum")
    out["# Changed Roster Formats"] = counts_per_id.fillna(0).astype(int)
    return out


def add_no_setup_or_format_change(
    df_src: pd.DataFrame,
    df_out: pd.DataFrame,
    roster_id_out_col: str = "roster_id",
    new_col_name: str = "# of Rosters with no Set up or Format Change",
) -> pd.DataFrame:
    """
    A roster is 'no change' if it only appears once (unique count <= 1) in the submission.
    """
    out = df_out.copy().reset_index(drop=True)
    if roster_id_out_col not in out.columns:
        out[new_col_name] = 0
        return out

    ids = out[roster_id_out_col]
    unique_counts = ids.groupby(ids).transform(lambda s: s.dropna().nunique())
    no_change_flag = (unique_counts <= 1).fillna(False)
    out[new_col_name] = no_change_flag.astype(int)
    return out


def add_complex_rosters(
    df_src: pd.DataFrame,
    df_out: pd.DataFrame,
    roster_id_out_col: str = "roster_id",
    complexity_candidates: Tuple[str, ...] = ("complexity", "Complexity", "complex"),
    include_values: Tuple[str, ...] = ("COMPLEX",),
    new_col_name: str = "# Complex Rosters",
) -> pd.DataFrame:
    """
    Mark rosters as 'complex' when df_src has a complexity column containing any include_values.
    """
    out = df_out.copy().reset_index(drop=True)
    if roster_id_out_col not in out.columns:
        out[new_col_name] = 0
        return out

    cx_col = _find_col(df_src, list(complexity_candidates))
    if not cx_col or len(df_src) != len(out):
        out[new_col_name] = 0
        return out

    cx_norm = (df_src[cx_col].astype("string").str.strip()
               .str.replace(r"[^\s\w\-]+", "", regex=True).str.upper())
    target = {v.upper().replace(" ", "_").replace("-", "_") for v in include_values}
    is_complex = cx_norm.isin(target)
    counts_per_id = is_complex.fillna(False).groupby(out[roster_id_out_col]).transform("sum")
    out[new_col_name] = counts_per_id.fillna(0).astype(int)
    return out


def add_all_rosters(
    df_out: pd.DataFrame,
    roster_id_out_col: str = "roster_id",
    new_col_name: str = "All Rosters",
) -> pd.DataFrame:
    """Per-roster_id total rows count."""
    out = df_out.copy().reset_index(drop=True)
    if roster_id_out_col not in out.columns:
        out[new_col_name] = 1
        return out

    counts = out[roster_id_out_col].value_counts(dropna=False)
    out[new_col_name] = out[roster_id_out_col].map(counts).astype(int)
    return out


def add_conformance_tat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Conformance TAT = (end - start) formatted as DD/HH/MM/SS.
    We try several candidate names for start/end timestamps and remove timezone.
    """
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

    start_col = _find_col(out, start_candidates)
    end_col = _find_col(out, end_candidates)

    if not start_col or not end_col:
        print(f"[WARN] Conformance TAT skipped. start={start_col}, end={end_col}")
        out["Conformance TAT"] = None
        return out

    # Normalize obvious placeholder strings
    out[start_col] = out[start_col].replace(["NaT", "None", ""], pd.NA)
    out[end_col] = out[end_col].replace(["NaT", "None", ""], pd.NA)

    # Parse and drop timezone
    start = pd.to_datetime(out[start_col], errors="coerce", utc=True).dt.tz_localize(None)
    end = pd.to_datetime(out[end_col], errors="coerce", utc=True).dt.tz_localize(None)

    tat = (end - start).round("S")
    tat = tat.where(tat >= pd.Timedelta(0))  # no negative durations

    comps = tat.dt.components
    tat_str = (
        comps["days"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["hours"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["minutes"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["seconds"].astype("Int64").astype(str).str.zfill(2)
    ).where(~tat.isna(), None)

    out["Conformance TAT"] = tat_str
    return out


def add_rows_counts(df_src: pd.DataFrame, df_out: pd.DataFrame) -> pd.DataFrame:
    """
    Add '# of rows in' and '# of rows out'.
    If df_src has explicit count columns, prefer them; otherwise use lengths.
    """
    out = df_out.copy().reset_index(drop=True)

    col_in = _find_col(df_src, ["input_rec_count", "input records count"])
    col_out = _find_col(df_src, ["conformed_rec_count", "output_rec_count",
                                 "conformed records count", "output records count"])

    out["# of rows in"] = df_src[col_in] if col_in and len(df_src) == len(out) else len(df_src)
    out["# of rows out"] = df_src[col_out] if col_out and len(df_src) == len(out) else len(out)
    return out


def add_unique_npi_counts(df_src: pd.DataFrame, df_out: pd.DataFrame) -> pd.DataFrame:
    """
    Add '# of unique NPI's in Input' and '# of unique NPI's in Output'.
    Tries explicit count columns first; else computes on NPI columns if present.
    """
    out = df_out.copy().reset_index(drop=True)

    col_in_cnt = _find_col(df_src, ["input_unique_npi_count", "unique npi input", "unique npi count input"])
    col_out_cnt = _find_col(df_src, ["output_unique_npi_count", "unique npi output", "unique npi count output"])

    if col_in_cnt and len(df_src) == len(out):
        out["# of unique NPI's in Input"] = df_src[col_in_cnt]
    else:
        npi_in_col = _find_col(df_src, ["npi", "npi_number", "npi id", "npiid"])
        out["# of unique NPI's in Input"] = df_src[npi_in_col].nunique(dropna=True) if npi_in_col else pd.NA

    if col_out_cnt and len(df_src) == len(out):
        out["# of unique NPI's in Output"] = df_src[col_out_cnt]
    else:
        npi_out_col = _find_col(out, ["npi", "npi_number", "npi id", "npiid"])
        out["# of unique NPI's in Output"] = out[npi_out_col].nunique(dropna=True) if npi_out_col else pd.NA

    return out


# ------------------------------------------------------------
# FRIENDLY HEADERS + EXCEL/GCS OUTPUT
# ------------------------------------------------------------
def to_friendly(df: pd.DataFrame, columns_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Rename canonical column names to friendly display names and reorder them.
    (Fixes IndexError by ensuring we never pass a dict into df[...] indexing.)
    """
    df2 = df.rename(columns=columns_mapping)

    canonical_present = [c for c in columns_mapping.keys() if c in df.columns]
    friendly_order = [columns_mapping[c] for c in canonical_present]
    rest = [c for c in df2.columns if c not in friendly_order]

    return df2[friendly_order + rest]


def autosize_and_freeze(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    """Auto-size columns and freeze header row."""
    from openpyxl.utils import get_column_letter

    ws = writer.sheets[sheet_name]
    for idx, col in enumerate(df.columns, start=1):
        series = df[col].astype("string")
        max_cell_len = int(series.map(lambda x: len(x) if x is not None else 0).max() or 0)
        header_len = len(str(col))
        width = min(max(max_cell_len, header_len) + 2, 60)
        ws.column_dimensions[get_column_letter(idx)].width = width

    ws.freeze_panes = "A2"


def write_output_local(df: pd.DataFrame, out_path: str) -> None:
    """
    Write DataFrame to local .xlsx or .csv. Uses openpyxl for .xlsx.
    """
    out_path = str(out_path)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    ext = Path(out_path).suffix.lower()

    if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        try:
            import openpyxl  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "openpyxl is required to write .xlsx files. Install with: pip install openpyxl"
            ) from e

        safe = make_excel_safe(df)
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            sheet = "output"
            safe.to_excel(writer, index=False, sheet_name=sheet)
            autosize_and_freeze(writer, safe, sheet_name=sheet)
        print(f"[INFO] Wrote Excel: {out_path}")

    elif ext == ".csv":
        df.to_csv(out_path, index=False)
        print(f"[INFO] Wrote CSV: {out_path}")

    else:
        raise ValueError(f"Unsupported output extension '{ext}'. Use .xlsx or .csv")


def upload_to_gcs(local_path: str, gcs_uri: str) -> None:
    """
    Upload a local file to GCS using default credentials on Dataproc/Notebook.
    gcs_uri must be in the form: gs://bucket/path/to/file.ext
    """
    try:
        from google.cloud import storage
    except ImportError as e:
        raise ImportError(
            "google-cloud-storage is required to upload to GCS. "
            "Install with: pip install google-cloud-storage"
        ) from e

    if not gcs_uri.startswith("gs://"):
        raise ValueError("gcs_uri must start with 'gs://'")

    bucket_name, blob_name = gcs_uri[5:].split("/", 1)
    client = storage.Client()                # uses default service account
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

    print(f"[INFO] Uploaded to GCS: {gcs_uri}")


def write_output_any(df: pd.DataFrame, out_uri: str) -> None:
    """
    Write DataFrame either to local path or to a GCS URI (gs://...).
    For GCS, writes to a temporary local file first, then uploads.
    """
    if out_uri.lower().startswith("gs://"):
        suffix = ".xlsx" if out_uri.lower().endswith(".xlsx") else ".csv"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name
        try:
            write_output_local(df, tmp_path)
            upload_to_gcs(tmp_path, out_uri)
        finally:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass
    else:
        write_output_local(df, out_uri)


def get_source_dataframe() -> pd.DataFrame:
    """
    Use the in-notebook variable `result` if present and is a DataFrame; otherwise read from SRC_PATH.
    """
    if "result" in globals():
        obj = globals()["result"]
        if isinstance(obj, pd.DataFrame):
            df = obj.copy()
        else:
            df = pd.DataFrame(obj)
        print(f"[INFO] Using DataFrame from 'result' variable. shape={df.shape}")
        return df

    if SRC_PATH is None:
        raise ValueError(
            "No global 'result' DataFrame found and SRC_PATH is not set. "
            "Either run your SQL to create `result`, or set SRC_PATH to a file."
        )
    return read_source(SRC_PATH, SHEET)


# ------------------------------------------------------------
# MAIN PIPELINE
# ------------------------------------------------------------
def run_pipeline(src_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Full pipeline:
      - use provided src_df (or read from SRC_PATH or in-notebook `result`)
      - extract & rename to canonical
      - compute KPIs (including Conformance TAT in DD/HH/MM/SS)
      - rename to friendly headers
      - write Excel/CSV locally or to GCS
    Returns the final DataFrame used for the output.
    """
    # 1) Source
    df_src = src_df if src_df is not None else get_source_dataframe()

    # 2) Canonical selection/rename
    output_order: List[str] = list(MAPPING.values())
    out_df = extract_and_rename(df_src, MAPPING, output_order)
    print(f"[INFO] Prepared output (canonical) shape: {out_df.shape}")

    # 3) KPIs
    out_df = add_new_roster_formats(df_src, out_df)
    out_df = add_changed_roster_formats(df_src, out_df)
    out_df = add_no_setup_or_format_change(df_src, out_df)
    out_df = add_complex_rosters(df_src, out_df)
    out_df = add_all_rosters(out_df)
    out_df = add_conformance_tat(out_df)          # computes DD/HH/MM/SS using detected timestamp cols
    out_df = add_rows_counts(df_src, out_df)
    out_df = add_unique_npi_counts(df_src, out_df)

    # 4) Friendly header rename + write
    final_df = to_friendly(out_df, FRIENDLY_HEADERS)
    write_output_any(final_df, OUT_URI)

    print("[INFO] Done.")
    return final_df


# ------------------------------------------------------------
# Entry point (works in notebooks & scripts)
# ------------------------------------------------------------
if __name__ == "__main__":
    try:
        # If you already have a DataFrame named `result` from your SQL, this will use it.
        # Otherwise, set SRC_PATH above and it will read from file.
        final_df = run_pipeline()
    except Exception as e:
        print("[ERROR]", type(e).__name__, "-", e)
        raise
