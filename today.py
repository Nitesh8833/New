"""
emory_converter.py
──────────────────
Convert the “source” Emory Healthcare Providers worksheet into the
flattened “converted” layout.


Date: 2025-08-04
"""

from pathlib import Path
import re
import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# 1.  Helpers for parsing the raw sheet
# ──────────────────────────────────────────────────────────────────────────────
def _find_row(df: pd.DataFrame, col0_value: str) -> int:
    """
    Find the first row where column-0 equals (case-insensitive) `col0_value`.
    Raises ValueError if not found.
    """
    mask = df.iloc[:, 0].astype(str).str.strip().str.lower() == col0_value.lower()
    idx = mask.idxmax() if mask.any() else None
    if idx is None or not mask.any():
        raise ValueError(f"Row with value “{col0_value}” not found.")
    return int(idx)


def load_affiliation_lookup(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the “Affiliations / Tax ID Number” block at the top of the sheet
    and return a lookup table with columns:  code, name, tax_id.
    """
    start = _find_row(df_raw, "Affiliations") + 1
    rows = []
    for i in range(start, len(df_raw)):
        name, tax_id = df_raw.iloc[i, 0], df_raw.iloc[i, 1]
        if pd.isna(name) or pd.isna(tax_id):
            break                                # reached blank line / next section
        name = str(name).strip()
        tax_id = str(tax_id).strip()
        # pull the 3-4 letter abbreviation inside parentheses
        m = re.search(r"\(([^)]+)\)\s*$", name)
        code = m.group(1) if m else None
        rows.append((code, name, tax_id))
    return pd.DataFrame(rows, columns=["code", "name", "tax_id"])


def load_provider_table(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Locate the provider header row (“LAST NAME …”) and return it as a DataFrame.
    """
    header_row = _find_row(df_raw, "last name")
    providers = df_raw.iloc[header_row + 1 :].copy()
    providers.columns = df_raw.iloc[header_row].tolist()
    providers = providers.dropna(how="all").reset_index(drop=True)
    return providers


# ──────────────────────────────────────────────────────────────────────────────
# 2.  Transformation logic
# ──────────────────────────────────────────────────────────────────────────────
def parse_affiliation_cell(cell: str) -> tuple[str | None, str | None]:
    """
    Split a cell like “EMCF - 12-3456791” into (code, tax_id).
    Returns (None, None) if the cell is empty.
    """
    if pd.isna(cell) or not str(cell).strip():
        return None, None
    m = re.match(r"\s*([A-Za-z]+)\s*-\s*([0-9\-]+)", str(cell))
    if m:
        return m.group(1).strip(), m.group(2).strip()
    # fallback – treat first token as the code
    parts = str(cell).split()
    return (parts[0].strip(), None) if parts else (None, None)


def enrich_providers(providers: pd.DataFrame, aff_lookup: pd.DataFrame) -> pd.DataFrame:
    """
    Add Tax ID, Hospital Affiliation Name, and Hospital Affiliation Code columns.
    """
    def _enrich(row):
        code, tax_id = parse_affiliation_cell(row["AFFILIATIONS"])
        # direct match → otherwise allow for trailing “A” mismatch (TEC ↔ TECA)
        name = aff_lookup.loc[aff_lookup["code"] == code, "name"]
        if name.empty and code and code.endswith("A"):
            name = aff_lookup.loc[aff_lookup["code"] == code[:-1], "name"]
        return pd.Series(
            {
                "Tax ID": tax_id,
                "HOSPITAL AFFILIATION NAME": name.iloc[0] if not name.empty else None,
                "HOSPITAL AFFILIATION CODE": code,
            }
        )

    enriched = providers.apply(_enrich, axis=1)
    out = pd.concat([providers.drop(columns=["AFFILIATIONS"]), enriched], axis=1)
    # guarantee missing columns exist
    for col in ("PROVIDER NUMBER", "EFFECTIVE DATE"):
        if col not in out.columns:
            out[col] = pd.NA
    # final column order
    cols = [
        "LAST NAME",
        "FIRST NAME",
        "MIDDLE INITIAL",
        "SUFFIX",
        "TITLE",
        "PRACTICING SPECIALTY",
        "Tax ID",
        "HOSPITAL AFFILIATION NAME",
        "HOSPITAL AFFILIATION CODE",
        "PROVIDER NUMBER",
        "EFFECTIVE DATE",
    ]
    return out[cols]


# ──────────────────────────────────────────────────────────────────────────────
# 3.  Orchestrator
# ──────────────────────────────────────────────────────────────────────────────
def convert(source_file: Path, target_file: Path) -> None:
    """Main driver: read → transform → write."""
    df_raw = pd.read_excel(source_file, sheet_name=0, header=None)

    aff_lookup = load_affiliation_lookup(df_raw)
    providers = load_provider_table(df_raw)
    converted = enrich_providers(providers, aff_lookup)

    with pd.ExcelWriter(target_file, engine="openpyxl") as xlw:
        converted.to_excel(xlw, sheet_name="converted", index=False)

    print(f"✔️  Converted file written to: {target_file.resolve()}")


# ──────────────────────────────────────────────────────────────────────────────
# 4.  Run when executed directly
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    SRC = Path(r"C:\Users\Nitesh\Downloads\Emory_Healthcare_Providers.xlsx")
    DST = Path(r"C:\Users\Nitesh\Downloads\Emory_Healthcare_output01.xlsx")
    convert(SRC, DST)
