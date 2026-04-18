from pathlib import Path

import pandas as pd

from src.config.paths import INTERIM_PORTO_DIR, PROCESSED_PORTO_DIR
from src.utils.io_utils import save_parquet


def load_porto_interim(
    input_path: Path = INTERIM_PORTO_DIR / "porto_raw_consolidated.parquet",
) -> pd.DataFrame:
    """Load consolidated interim porto dataset."""
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


def process_porto(df: pd.DataFrame) -> pd.DataFrame:
    """Main processing logic for porto."""
    df = standardize_column_names(df)
    return df


def save_porto_processed(df: pd.DataFrame) -> Path:
    """Save processed porto dataset."""
    output_path = PROCESSED_PORTO_DIR / "porto_processed.parquet"
    save_parquet(df, output_path)
    return output_path


def run_porto_processing() -> pd.DataFrame:
    """Full processing pipeline for porto."""
    df = load_porto_interim()
    df = process_porto(df)
    save_porto_processed(df)
    return df
