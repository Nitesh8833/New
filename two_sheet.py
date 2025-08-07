# ------------------------------------------------------------------
# ### >>> REPLACE FROM HERE  (everything below)  <<<               #
# ------------------------------------------------------------------

# -------- which friendly columns go to which sheet ---------------
SHEET1_NAME = "summary"
SHEET2_NAME = "roster-KPIs"

SHEET1_COLS = [
    "Business Team", "Group Type", "Roster ID", "Provider Entity",
    "Parent Transaction Type", "Transaction Type",
    "Conformance TAT", "# of rows in", "# of rows out",
    "# of unique NPI's in Input", "# of unique NPI's in Output",
]

SHEET2_COLS = [
    "# New Roster Formats", "# Changed Roster Formats",
    "# of Rosters with no Set up or Format Change",
    "# Complex Rosters", "All Rosters",
]

# ------------- Excel utilities -----------------------------------
def autosize_and_freeze(ws, df: pd.DataFrame):
    from openpyxl.utils import get_column_letter

    for idx, col in enumerate(df.columns, start=1):
        series = df[col].astype("string").fillna("")
        max_len = max(series.map(len).max(), len(str(col)))
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)
    ws.freeze_panes = "A2"

def write_multi_sheet_to_gcs(
    df: pd.DataFrame,
    gcs_uri: str,
    sheet1_cols: List[str],
    sheet2_cols: List[str],
    sheet1_name: str = SHEET1_NAME,
    sheet2_name: str = SHEET2_NAME,
) -> None:
    """Write df into two sheets then upload to GCS."""
    try:
        from google.cloud import storage
    except ImportError:
        raise ImportError("pip install google-cloud-storage") from None

    # Ensure requested columns exist (fill with NA if not)
    df1 = df.copy()
    for c in sheet1_cols:
        if c not in df1.columns:
            df1[c] = pd.NA
    df1 = df1[sheet1_cols]

    df2 = df.copy()
    for c in sheet2_cols:
        if c not in df2.columns:
            df2[c] = pd.NA
    df2 = df2[sheet2_cols]

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name=sheet1_name, index=False)
        autosize_and_freeze(writer.sheets[sheet1_name], df1)

        df2.to_excel(writer, sheet_name=sheet2_name, index=False)
        autosize_and_freeze(writer.sheets[sheet2_name], df2)
    buffer.seek(0)

    # ---- upload
    if not gcs_uri.startswith("gs://"):
        raise ValueError("OUT_URI must start with 'gs://'")

    bucket_name, blob_name = gcs_uri[5:].split("/", 1)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(
        buffer,
        content_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    print(f"[INFO] Uploaded Excel with 2 sheets to {gcs_uri}")


# ---------------- MAIN PIPELINE -----------------------------------
def run_pipeline_to_gcs(out_uri: str = OUT_URI) -> pd.DataFrame:
    if "result" not in globals():
        raise ValueError("DataFrame 'result' not found in notebook globals.")
    src = globals()["result"].copy()
    print(f"[INFO] Using DataFrame 'result' with shape={src.shape}")

    # === KPI build sequence ===
    df = src.copy()
    df = add_new_roster_formats(df)
    df = add_changed_roster_formats(df)
    df = add_no_setup_or_format_change(df)
    df = add_complex_rosters(df)
    df = add_all_rosters(df)
    df = add_conformance_tat(df)
    df = add_rows_counts(src, df)
    df = add_unique_npi_counts(src, df)

    # Friendly headers then excel-safe
    df = apply_friendly_headers(df, FRIENDLY_HEADERS)
    df = make_excel_safe(df)

    # Write two-sheet Excel and upload
    write_multi_sheet_to_gcs(df, out_uri, SHEET1_COLS, SHEET2_COLS)
    return df


if __name__ == "__main__":
    run_pipeline_to_gcs(OUT_URI)
