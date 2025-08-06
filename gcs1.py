def run_pipeline_and_write(out_uri: str) -> pd.DataFrame:
    df_src = get_source_dataframe()
    out_df = extract_and_rename(df_src, MAPPING, list(MAPPING.values()))
    print(f"[INFO] Prepared output (canonical) shape: {out_df.shape}")

    # KPI steps, always verifying return type
    for step in [
        add_new_roster_formats,
        add_changed_roster_formats,
        add_no_setup_or_format_change,
        add_complex_rosters,
        add_all_rosters,
        add_conformance_tat,
        add_rows_counts,
        add_unique_npi_counts,
    ]:
        tmp = step(df_src, out_df) if step.__code__.co_argcount == 2 else step(out_df)
        out_df = tmp if isinstance(tmp, pd.DataFrame) else out_df

    # Friendly + Excel safe
    final_df = to_friendly(out_df, FRIENDLY_HEADERS)
    final_df = make_excel_safe(final_df)

    write_output_any(final_df, out_uri)
    print("[INFO] Done.")
    return final_df
