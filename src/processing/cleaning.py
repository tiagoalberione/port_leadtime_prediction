import re
import unicodedata

import pandas as pd


TZ_OFFSET_PATTERN = re.compile(r"(?:Z|[+-]\d{2}(?::?\d{2})?)$")
DEFAULT_LOCAL_TZ = "America/Sao_Paulo"


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Convert column names to snake_case without accents."""
    new_columns = []

    for col in df.columns:
        col = unicodedata.normalize("NFKD", str(col)).encode("ascii", "ignore").decode("utf-8")
        col = col.strip().lower()
        col = re.sub(r"[^a-z0-9]+", "_", col)
        col = re.sub(r"_+", "_", col).strip("_")
        new_columns.append(col)

    df = df.copy()
    df.columns = new_columns
    return df


def normalize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize common string representations of missing values."""
    df = df.copy()

    missing_tokens = {
        "": pd.NA,
        " ": pd.NA,
        "nan": pd.NA,
        "NaN": pd.NA,
        "none": pd.NA,
        "None": pd.NA,
        "null": pd.NA,
        "NULL": pd.NA,
        "<NA>": pd.NA,
    }

    object_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in object_cols:
        df[col] = df[col].replace(missing_tokens)

    return df


def trim_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace and carriage returns from string columns."""
    df = df.copy()

    object_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in object_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.replace(r"[\r\n\t]+", " ", regex=True)
            .str.strip()
        )

    df = normalize_missing_values(df)
    return df


def parse_mixed_datetime_series(
    s: pd.Series,
    local_tz: str = DEFAULT_LOCAL_TZ,
) -> pd.Series:
    """
    Parse a datetime series containing a mix of timezone-aware and timezone-naive strings.

    Supported examples:
    - '2020-10-30 18:00:00-03'
    - '2025-12-31 23:00:00.0000000'

    Output:
    - pandas datetime64[ns]
    - timezone removed after conversion to the local timezone
    - final values represent local wall-clock time
    """
    s = s.copy()

    # Normalize whitespace and hidden characters first
    s = (
        s.astype("string")
        .str.replace(r"[\r\n\t]+", " ", regex=True)
        .str.strip()
    )

    # Normalize common textual null values
    s = s.replace(
        {
            "": pd.NA,
            " ": pd.NA,
            "nan": pd.NA,
            "NaN": pd.NA,
            "none": pd.NA,
            "None": pd.NA,
            "null": pd.NA,
            "NULL": pd.NA,
            "<NA>": pd.NA,
        }
    )

    # Detect values that already contain explicit timezone information
    has_tz = s.str.contains(TZ_OFFSET_PATTERN, na=False)

    result = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    # Parse timezone-aware values, convert to local timezone, then drop timezone
    if has_tz.any():
        aware = pd.to_datetime(s[has_tz], errors="coerce", utc=True)
        aware = aware.dt.tz_convert(local_tz).dt.tz_localize(None)
        result.loc[has_tz] = aware

    # Parse timezone-naive values directly as local wall-clock time
    if (~has_tz).any():
        naive = pd.to_datetime(s[~has_tz], errors="coerce")
        result.loc[~has_tz] = naive

    return result


def parse_datetime_columns(
    df: pd.DataFrame,
    datetime_cols: list[str],
    local_tz: str = DEFAULT_LOCAL_TZ,
) -> pd.DataFrame:
    """Parse selected datetime columns using mixed-timezone support."""
    df = df.copy()

    for col in datetime_cols:
        if col in df.columns:
            df[col] = parse_mixed_datetime_series(df[col], local_tz=local_tz)

    return df