def _find_col(df: pd.DataFrame, candidates):
    norm = lambda s: re.sub(r'[^a-z0-9]+', '', str(s).lower())
    by_norm = {norm(c): c for c in df.columns}
    # exact normalized match
    for c in candidates:
        k = norm(c)
        if k in by_norm:
            return by_norm[k]
    # relaxed: substring
    for c in candidates:
        k = norm(c)
        for col in df.columns:
            if k and k in norm(col):
                return col
    return None

def add_all_rosters(df: pd.DataFrame, new_col_name: str = "All Rosters") -> pd.DataFrame:
    """
    Per-roster count. Works with 'roster_id', 'Roster ID', 'roster id', etc.
    """
    out = df.copy()
    rid = _find_col(out, ["roster_id", "Roster ID", "roster id", "rosterid"])
    if not rid:
        print("[WARN] Roster ID column not found; 'All Rosters' will be 1.")
        out[new_col_name] = 1
        return out

    # robust count per roster_id
    counts = out.groupby(out[rid]).transform("size")
    out[new_col_name] = counts.fillna(0).astype(int)
    return out
