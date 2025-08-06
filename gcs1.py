import re
import pandas as pd
import numpy as np

def _norm(s: str) -> str:
    # lower, remove spaces/underscores/punct to compare robustly
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    norm_map = {col: _norm(col) for col in df.columns}
    for cand in candidates:
        c = _norm(cand)
        # exact normalized match
        for col, n in norm_map.items():
            if n == c:
                return col
        # contains match (e.g., "postedtimestamp" inside "prmspostedtimestamp")
        for col, n in norm_map.items():
            if c in n:
                return col
    return None

def add_conformance_tat(df_src: pd.DataFrame) -> pd.DataFrame:
    out = df_src.copy()

    # Possible names (adjust/add as needed)
    start_candidates = [
        "prms_posted_timestamp", "prms_posted_time", "posted_timestamp",
        "file_dropped_time", "file_dropped_timestamp", "prms_posted_ts"
    ]
    end_candidates = [
        "file_ingestion_timestamp", "ingestion_timestamp", "file_ingested_timestamp",
        "file_ingestion_time", "file_returned_timestamp", "update_timestamp",
        "processed_timestamp", "file_returned_time"
    ]

    start_col = _find_col(out, start_candidates)
    end_col   = _find_col(out, end_candidates)

    if not start_col or not end_col:
        print(f"[WARN] Conformance TAT skipped. start={start_col}, end={end_col}")
        out["Conformance TAT"] = None
        return out

    # Clean obvious placeholders
    out[start_col] = out[start_col].replace(["NaT", "None", ""], pd.NA)
    out[end_col]   = out[end_col].replace(["NaT", "None", ""], pd.NA)

    # Parse -> drop timezone (Excel cannot handle tz)
    start = pd.to_datetime(out[start_col], errors="coerce", utc=True).dt.tz_localize(None)
    end   = pd.to_datetime(out[end_col],   errors="coerce", utc=True).dt.tz_localize(None)

    tat = (end - start).round("S")
    tat = tat.where(tat >= pd.Timedelta(0))  # no negative durations

    # Format as DD/HH/MM/SS
    comps = tat.dt.components
    tat_str = (
        comps["days"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["hours"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["minutes"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["seconds"].astype("Int64").astype(str).str.zfill(2)
    ).where(~tat.isna(), None)

    out["Conformance TAT"] = tat_str
    return out
