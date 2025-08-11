# add friendly display names
FRIENDLY_HEADERS.update({
    "case_number": "Case Number",
    "roster_date": "Roster Date",
})

# put the new columns where you want them in Sheet1
SHEET1_COLS = [
    "Business Team", "Group Type", "Roster ID", "Provider Entity",
    "Parent Transaction Type", "Transaction Type",
    "Case Number", "Roster Date",          # ← added here
    "Conformance TAT", "# of rows in", "# of rows out",
]
import pandas as pd

def add_case_and_roster_date(summary_df: pd.DataFrame, src_df: pd.DataFrame) -> pd.DataFrame:
    out = summary_df.copy()

    # case_number (take first non-null as string)
    if "case_number" in src_df.columns:
        _case = src_df["case_number"].dropna().astype(str)
        case_val = _case.iloc[0] if not _case.empty else None
    else:
        case_val = None

    # roster_date (normalize to YYYY-MM-DD)
    if "roster_date" in src_df.columns:
        _r = pd.to_datetime(src_df["roster_date"], errors="coerce")
        r_ok = _r.dropna()
        roster_val = r_ok.iloc[0].date().isoformat() if not r_ok.empty else None
    else:
        roster_val = None

    out["case_number"] = case_val
    out["roster_date"] = roster_val
    return out
# result = your source dataframe (the one you showed in screenshots)
# sheet1_df = your existing summary dataframe (after you’ve built KPIs and TAT)

sheet1_df = add_case_and_roster_date(sheet1_df, result)

# then your existing rename + column order:
sheet1_df = sheet1_df.rename(columns=FRIENDLY_HEADERS)
sheet1_df = sheet1_df.reindex(columns=SHEET1_COLS)
