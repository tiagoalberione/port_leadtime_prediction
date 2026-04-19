import re
import unicodedata
import pandas as pd


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


def trim_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace from string columns."""
    df = df.copy()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "null": pd.NA})
    return df


def parse_datetime_columns(df: pd.DataFrame, datetime_cols: list[str]) -> pd.DataFrame:
    """Parse selected columns to datetime."""
    df = df.copy()
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df