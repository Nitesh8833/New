def to_friendly(df: pd.DataFrame, columns_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Rename canonical -> friendly and keep only the mapped + KPI columns.
    Drops any raw columns not in the mapping or KPI list.
    """
    if not isinstance(df, pd.DataFrame):
        print("[ERROR] to_friendly received non-DataFrame, returning empty DataFrame.")
        return pd.DataFrame()

    # Rename canonical → friendly
    df2 = df.rename(columns=columns_mapping)

    # Only keep mapped canonical columns + KPI columns already present
    friendly_cols = [columns_mapping[c] for c in columns_mapping if c in df.columns]
    kpi_cols = [c for c in df2.columns if c.startswith("#") or c == "Conformance TAT"]

    keep_cols = [c for c in friendly_cols + kpi_cols if c in df2.columns]
    return df2[keep_cols]
