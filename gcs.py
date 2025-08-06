#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ==== Roster Mapper — Canonical Pipeline (Cloud SQL -> canonical) + KPIs + Friendly Rename + GCS I/O ====
# NOTE: Mailer functionality removed. Input is from `result` DataFrame (if present) or SRC_PATH.
# Output is written to GCS (gs://...) or local path automatically.
#
# Requirements on Dataproc notebook:
#   pip install google-cloud-storage openpyxl pandas

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import Optional, Dict, List, Union, Tuple
import pandas as pd

# ===================== USER PARAMETERS =====================
# 1) Output target (GCS or local)
OUT_URI = "gs://YOUR_BUCKET/YOUR_FOLDER/roster_full_table_output.xlsx"  # <-- change to your bucket
# Example local fallback: OUT_URI = "/tmp/roster_full_table_output.xlsx"

# 2) If you still want a file fallback, keep these (used only if `result` is not defined):
SRC_PATH = r"/tmp/roster_full_table.xlsx"   # ignored if `result` variable exists in the notebook
SHEET: Optional[str] = None

# 3) MAPPING (SOURCE -> CANONICAL OUTPUT). LEFT = your source column names in `result` (or file); RIGHT = canonical.
#    Adjust LEFT keys to match your query result column names exactly (case-insensitive, spaces/underscore/hyphen tolerant).
MAPPING: Dict[str, str] = {
    "business_owner": "business_owner",
    "group_type": "group_type",
    "Roster ID": "roster_id",
    "Provider Entity": "roster_name",
    "Parent Transaction Type": "parent_transaction_type",
    "Transaction Type": "transaction_type",
}

# 4) Friendly headers to apply ONLY at the end
FRIENDLY_HEADERS: Dict[str, str] = {
    "business_owner": "Business Team",
    "group_type": "Group Team",
    "roster_id": "Roster ID",
    "roster_name": "Provider Entity",
    "parent_transaction_type": "Parent Transaction Type",
    "transaction_type": "Transaction Type",
}

# ===========================================================


# ------------------------------ Helpers --------------------------------------
def _normalize(s: str) -> str:
    """Normalize a header for robust matching: lower, strip, remove spaces/_/-/dots."""
    return re.sub(r"[ \t\-_\.]+", "", str(s).strip().lower())


def read_source(path: str, sheet: Optional[str]) -> pd.DataFrame:
    """Read .xlsx (optionally with sheet) or .csv based on file extension."""
    ext = Path(path).suffix.lower()
    if ext in [".xlsx", ".xlsm", ".xltx", ".xltm"]:
        print(f"[INFO] Reading Excel: {path} (sheet={sheet})")
        return pd.read_excel(path, sheet_name=sheet) if sheet else pd.read_excel(path)
    elif ext == ".csv":
        print(f"[INFO] Reading CSV: {path}")
        return pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported source extension '{ext}'. Use .xlsx or .csv")


def build_renamer(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> Dict[str, str]:
    """
    Build a renamer dict {source_col_name_in_df: output_col_name} using robust matching.
    *** IMPORTANT: mapping is SOURCE -> OUTPUT ***
    """
    norm_to_real = {_normalize(col): col for col in df.columns}
    renamer: Dict[str, str] = {}
    matched, missing = 0, 0
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


def extract_and_rename(df: pd.DataFrame, mapping_src_to_out: Dict[str, str],
                       output_order: List[str]) -> pd.DataFrame:
    """
    Select & rename per mapping (SOURCE -> OUTPUT), create missing outputs as empty,
    reorder to output_order, and lightly clean strings.
    """
    renamer = build_renamer(df, mapping_src_to_out)
    selected = df[list(renamer.keys())].rename(columns=renamer) if renamer else pd.DataFrame()

    # Ensure all desired output columns exist (create empty where missing)
    for out_col in output_order:
        if out_col not in selected.columns:
            selected[out_col] = pd.NA

    # Reorder columns to OUTPUT order
    selected = selected[output_order]

    # Light string cleanup
    for c in selected.columns:
        if pd.api.types.is_string_dtype(selected[c]):
            selected[c] = selected[c].astype("string").str.strip()

    return selected.reset_index(drop=True)


# ---------- Column lookup & common transforms ----------
def _find(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """Find a column by normalized name."""
    norm_to_real = {_normalize(c): c for c in df.columns}
    for c in candidates:
        k = _normalize(c)
        if k in norm_to_real:
            return norm_to_real[k]
    return None


def _version_status_series(df_src: pd.DataFrame, df_out: pd.DataFrame) -> Optional[pd.Series]:
    """Return normalized version_status (NEW_FILE/NEW_VERSION/EXISTING_VERSION) aligned with output rows."""
    col = _find(df_src, "version_status", "Version Status")
    if not col or len(df_src) != len(df_out):
        return None
    return (df_src[col].astype("string").str.strip()
            .str.replace(r"[\s\-]+", "_", regex=True).str.upper())


def _to_timedelta_str(td: pd.Timedelta) -> str | pd._libs.NaTType:
    """Format a Timedelta as DD/HH:MM:SS or NA."""
    if pd.isna(td):
        return pd.NA
    total_sec = int(td.total_seconds())
    sign = "-" if total_sec < 0 else ""
    total_sec = abs(total_sec)
    days, rem = divmod(total_sec, 86400)
    hrs, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    return f"{sign}{days:02d}/{hrs:02d}:{mins:02d}:{secs:02d}"


# --------------------------- Derived KPI columns -----------------------------
def add_new_roster_formats(
    df_src: pd.DataFrame,
    df_out: pd.DataFrame,
    roster_id_out_col: str = "roster_id",
    include_statuses: Tuple[str, ...] = ("NEW_FILE", "NEW_VERSION"),
) -> pd.DataFrame:
    out = df_out.copy().reset_index(drop=True)
    if roster_id_out_col not in out.columns:
        out["# New Roster Formats"] = 0
        return out
    vs = _version_status_series(df_src, out)
    if vs is None:
        out["# New Roster Formats"] = 0
        return out
    statuses = {s.upper().replace(" ", "_").replace("-", "_") for s in include_statuses}
    is_new = vs.isin(statuses)
    counts_per_id = is_new.fillna(False).groupby(out[roster_id_out_col]).transform("sum")
    out["# New Roster Formats"] = counts_per_id.fillna(0).astype(int)
    return out


def add_changed_roster_formats(
    df_src: pd.DataFrame,
    df_out: pd.DataFrame,
    roster_id_out_col: str = "roster_id",
) -> pd.DataFrame:
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
    out = df_out.copy().reset_index(drop=True)
    if roster_id_out_col not in out.columns:
        out[new_col_name] = 0
        return out
    vs = _version_status_series(df_src, out)
    if vs is None or len(vs) != len(out):
        out[new_col_name] = 0
        return out
    ids = out[roster_id_out_col]
    unique_counts = vs.groupby(ids).transform(lambda s: s.dropna().nunique())
    no_change_flag = (unique_counts <= 1).fillna(False)
    out[new_col_name] = no_change_flag.astype(int)
    return out


def add_complex_rosters(
    df_src: pd.DataFrame,
    df_out: pd.DataFrame,
    roster_id_out_col: str = "roster_id",
    complexity_candidates: Tuple[str, ...] = ("complexity", "Complexity"),
    include_values: Tuple[str, ...] = ("COMPLEX",),
    new_col_name: str = "# Complex Rosters",
) -> pd.DataFrame:
    out = df_out.copy().reset_index(drop=True)
    if roster_id_out_col not in out.columns:
        out[new_col_name] = 0
        return out
    cx_col = _find(df_src, *complexity_candidates)
    if not cx_col or len(df_src) != len(out):
        out[new_col_name] = 0
        return out
    cx_norm = (df_src[cx_col].astype("string").str.strip()
               .str.replace(r"[\\s\\-]+", "_", regex=True).str.upper())
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
    out = df_out.copy().reset_index(drop=True)
    if roster_id_out_col not in out.columns:
        out[new_col_name] = 1
        return out
    counts = out[roster_id_out_col].value_counts(dropna=False)
    out[new_col_name] = out[roster_id_out_col].map(counts).astype(int)
    return out


def add_conformance_tat(df_src: pd.DataFrame, df_out: pd.DataFrame) -> pd.DataFrame:
    out = df_out.copy().reset_index(drop=True)
    start_col = _find(df_src, "file_ingestion_timestamp", "ingestion_timestamp")
    end_col = _find(df_src, "prms_posted_timestamp", "update_timestamp", "processed_timestamp")
    if start_col and end_col and len(df_src) == len(out):
        start = pd.to_datetime(df_src[start_col], errors="coerce", utc=True)
        end = pd.to_datetime(df_src[end_col], errors="coerce", utc=True)
        tat = end - start
        out["Conformance TAT"] = tat.map(lambda x: _to_timedelta_str(x) if pd.notna(x) else pd.NA)
    else:
        out["Conformance TAT"] = pd.NA
    return out


def add_rows_counts(df_src: pd.DataFrame, df_out: pd.DataFrame) -> pd.DataFrame:
    out = df_out.copy().reset_index(drop=True)
    col_in = _find(df_src, "input_rec_count", "input records count")
    col_out = _find(df_src, "conformed_rec_count", "output_rec_count", "conformed records count")
    out["# of rows in"] = df_src[col_in] if col_in and len(df_src) == len(out) else pd.NA
    out["# of rows out"] = df_src[col_out] if col_out and len(df_src) == len(out) else pd.NA
    return out


def add_unique_npi_counts(df_src: pd.DataFrame, df_out: pd.DataFrame) -> pd.DataFrame:
    out = df_out.copy().reset_index(drop=True)
    col_in = _find(df_src, "input_unique_npi_count", "unique npi input")
    col_out = _find(df_src, "conformed_unique_npi_count", "unique npi output")
    out["# of unique NPI's in Input"] = df_src[col_in] if col_in and len(df_src) == len(out) else pd.NA
    out["# of unique NPI's in Output"] = df_src[col_out] if col_out and len(df_src) == len(out) else pd.NA
    return out


# --------------------------- Friendly rename (end of pipeline) ----------------
def to_friendly(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df2 = df.rename(columns=mapping)
    base_canon_order = [c for c in mapping.keys() if c in df.columns]
    base_friendly_order = [mapping[c] for c in base_canon_order]
    rest = [c for c in df2.columns if c not in base_friendly_order]
    return df2[base_friendly_order + rest]


# --------------------------- Excel formatting --------------------------------
def autosize_and_freeze(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    ws = writer.sheets[sheet_name]
    from openpyxl.utils import get_column_letter
    for idx, col in enumerate(df.columns, start=1):
        series = df[col].astype(str)
        lengths = series.map(len)
        max_cell = int(lengths.max()) if not lengths.empty else 0
        max_len = max(len(str(col)), max_cell)
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)
    ws.freeze_panes = "A2"


def write_output_local(df: pd.DataFrame, out_path: str) -> None:
    ext = Path(out_path).suffix.lower()
    if ext == ".xlsx":
        try:
            import openpyxl  # noqa: F401
        except ImportError as e:
            raise ImportError("openpyxl is required to write .xlsx files. Install with: pip install openpyxl") from e
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="output")
            autosize_and_freeze(writer, df, "output")
        print(f"[INFO] Wrote Excel: {out_path}")
    elif ext == ".csv":
        df.to_csv(out_path, index=False)
        print(f"[INFO] Wrote CSV: {out_path}")
    else:
        raise ValueError(f"Unsupported output extension '{ext}'. Use .xlsx or .csv")


def upload_to_gcs(local_path: str, gcs_uri: str) -> None:
    try:
        from google.cloud import storage
    except ImportError as e:
        raise ImportError("google-cloud-storage is required to upload to GCS. Install with: pip install google-cloud-storage") from e

    if not gcs_uri.startswith("gs://"):
        raise ValueError("gcs_uri must start with 'gs://'")

    _, remainder = gcs_uri.split("gs://", 1)
    bucket_name, blob_name = remainder.split("/", 1)
    client = storage.Client()  # uses Dataproc default service account credentials
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"[INFO] Uploaded to GCS: {gcs_uri}")


def write_output_any(df: pd.DataFrame, out_uri: str) -> None:
    """Write to local path or GCS URI."""
    if out_uri.startswith("gs://"):
        # write to a temp local file, then upload to GCS
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
        # local path
        Path(out_uri).parent.mkdir(parents=True, exist_ok=True)
        write_output_local(df, out_uri)


# --------------------------- Source resolution (Cloud SQL -> result) ----------
def get_source_dataframe() -> pd.DataFrame:
    """
    Prefer the in-notebook variable `result` (from Cloud SQL query).
    Fallback to reading from SRC_PATH if `result` is not defined.
    """
    if "result" in globals():
        obj = globals()["result"]
        if isinstance(obj, pd.DataFrame):
            df = obj.copy()
        else:
            df = pd.DataFrame(obj)
        print(f"[INFO] Using DataFrame from `result` variable. shape={df.shape}")
        return df
    else:
        print("[WARN] `result` not found in notebook globals; falling back to SRC_PATH.")
        return read_source(SRC_PATH, SHEET)


# --------------------------------- RUN ---------------------------------------
if __name__ == "__main__":
    try:
        # 1) Get source (Cloud SQL result if present; else file)
        df_src = get_source_dataframe()

        # 1b) (Optional) sort by a date column BEFORE KPIs if chronology matters
        # date_col = _find(df_src, "roster_date", "received_date")
        # if date_col:
        #     df_src = df_src.sort_values(date_col, kind="stable").reset_index(drop=True)

        # 2) Map & extract (SOURCE -> CANONICAL)
        OUTPUT_ORDER: List[str] = list(MAPPING.values())
        out_df = extract_and_rename(df_src, MAPPING, OUTPUT_ORDER)
        print(f"[INFO] Prepared output (canonical) shape: {out_df.shape}")

        # Guard: KPI alignment expects 1:1 rows
        if len(df_src) != len(out_df):
            raise ValueError(f"Row count mismatch: source={len(df_src)} output={len(out_df)}")

        # 3) KPIs
        out_df = add_new_roster_formats(df_src, out_df)
        out_df = add_changed_roster_formats(df_src, out_df)
        out_df = add_no_setup_or_format_change(df_src, out_df)
        out_df = add_complex_rosters(df_src, out_df)
        out_df = add_all_rosters(out_df)
        out_df = add_conformance_tat(df_src, out_df)
        out_df = add_rows_counts(df_src, out_df)
        out_df = add_unique_npi_counts(df_src, out_df)

        # 4) Friendly rename for final output
        final_df = to_friendly(out_df, FRIENDLY_HEADERS)

        # 5) Write to GCS or local
        write_output_any(final_df, OUT_URI)

        print("[INFO] Done.")
    except Exception as e:
        print("[ERROR]", type(e).__name__, "-", e)
        raise
