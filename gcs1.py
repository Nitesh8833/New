import pandas as pd

def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) tz-aware pandas datetime columns
    for col in df.columns[df.select_dtypes(include=['datetimetz']).columns]:
        df[col] = df[col].dt.tz_localize(None)

    # 2) tz-aware python datetimes stored as object dtype
    for col in df.columns[df.dtypes.eq('object')]:
        has_tz = df[col].map(lambda x: hasattr(x, 'tzinfo') and x.tzinfo is not None).any()
        if has_tz:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)

    # 3) tz-aware DatetimeIndex
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    return df
