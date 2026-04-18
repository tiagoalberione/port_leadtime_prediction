from pathlib import Path

import pandas as pd

from src.config.paths import INTERIM_ESTADIA_DIR, PROCESSED_ESTADIA_DIR
from src.utils.io_utils import save_parquet


def load_estadia_interim(
    input_path: Path = INTERIM_ESTADIA_DIR / "estadia_raw_consolidated.parquet",
) -> pd.DataFrame:
    """Load consolidated interim estadia dataset."""
    return pd.read_parquet(input_path)


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names:
    - lower case
    - strip spaces
    - replace spaces with underscore
    """
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" - ", " ", regex=False)
        .str.replace(" ", "_", regex=False)
    )
    return df

def parse_mixed_datetime_column(series: pd.Series) -> pd.Series:
    """Parse mixed naive/timezone-aware datetime strings into UTC."""
    s = series.astype("string").str.strip()
    s = s.mask(s.eq(""), pd.NA)

    has_tz = s.str.contains(r"(?:Z|[+-]\d{2}(?::\d{2})?)$", na=False)
    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]")

    if has_tz.any():
        result.loc[has_tz] = pd.to_datetime(
            s.loc[has_tz],
            errors="coerce",
            utc=True,
        )

    if (~has_tz).any():
        parsed_naive = pd.to_datetime(
            s.loc[~has_tz],
            errors="coerce",
        )
        result.loc[~has_tz] = (
            parsed_naive
            .dt.tz_localize(
                "America/Sao_Paulo",
                ambiguous="NaT",
                nonexistent="NaT",
            )
            .dt.tz_convert("UTC")
        )

    return result


def create_basic_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create basic calculated columns.

    Extend this function according to your business rules.
    """
    df = df.copy()

    # Treat datetime columns and standardize them in UTC.
    date_cols = [
        "estadia_chegada_no_porto",
        "estadia_atracacao",
        "estadia_desatracacao",
        "estadia_saida_do_porto",
    ]

    # Some date columns have defined timezone and other don't. 
    # This function handles both scenarios and import the dates, converting them accordingly.
    for col in date_cols:
        df[col] = parse_mixed_datetime_column(df[col])

    df["ano_referencia"] = df["estadia_chegada_no_porto"].dt.year
    df["mes_referencia"] = df["estadia_chegada_no_porto"].dt.month

    return df


def process_estadia(df: pd.DataFrame) -> pd.DataFrame:
    """Main processing logic for estadia_embarcacao."""
    df = standardize_column_names(df)
    df = create_basic_columns(df)
    return df


def save_estadia_processed(df: pd.DataFrame) -> Path:
    """Save processed estadia dataset."""
    output_path = PROCESSED_ESTADIA_DIR / "estadia_processed.parquet"
    save_parquet(df, output_path)
    return output_path


def run_estadia_processing() -> pd.DataFrame:
    """Full processing pipeline for estadia_embarcacao."""
    df = load_estadia_interim()
    df = process_estadia(df)
    save_estadia_processed(df)
    return df
