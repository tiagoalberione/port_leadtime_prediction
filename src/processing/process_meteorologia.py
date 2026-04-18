from pathlib import Path

import pandas as pd

from src.config.paths import INTERIM_METEOROLOGIA_DIR, PROCESSED_METEOROLOGIA_DIR
from src.utils.io_utils import save_parquet


def load_meteorologia_interim(
    input_path: Path = INTERIM_METEOROLOGIA_DIR / "meteorologia_raw_consolidated.parquet",
) -> pd.DataFrame:
    """Load consolidated interim meteorologia dataset."""
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


def process_meteorologia(df: pd.DataFrame) -> pd.DataFrame:
    """Main processing logic for meteorologia."""
    df = standardize_column_names(df)
    return df


def save_meteorologia_processed(df: pd.DataFrame) -> Path:
    """Save processed meteorologia dataset."""
    output_path = PROCESSED_METEOROLOGIA_DIR / "meteorologia_processed.parquet"
    save_parquet(df, output_path)
    return output_path


def run_meteorologia_processing() -> pd.DataFrame:
    """Full processing pipeline for meteorologia."""
    df = load_meteorologia_interim()
    df = process_meteorologia(df)
    save_meteorologia_processed(df)
    return df
