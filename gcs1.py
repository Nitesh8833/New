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



def add_all_rosters(df: pd.DataFrame, new_col_name: str = "All Rosters") -> pd.DataFrame:
    """
    Adds a KPI column '# All Rosters' counting rows per roster_id.
    Handles cases where roster_id is missing or invalid.
    """
    out = df.copy()
    rid = _find_col(out, ["roster_id", "Roster ID", "roster id", "rosterid"])
    
    if not rid or rid not in out.columns:
        print("[WARN] No roster_id column found. Setting 'All Rosters' to 1.")
        out[new_col_name] = 1
        return out
    
    # Ensure column is string (for groupby compatibility)
    out[rid] = out[rid].astype(str)

    try:
        counts = out.groupby(rid)[rid].transform("size")
        out[new_col_name] = counts.fillna(0).astype(int)
    except Exception as e:
        print(f"[ERROR] add_all_rosters failed: {e}")
        out[new_col_name] = 1
    
    return out
