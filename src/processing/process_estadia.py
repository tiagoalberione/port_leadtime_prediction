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
        .str.replace(" ", "_", regex=False)
    )
    return df


def create_basic_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create basic calculated columns.
    Extend this function according to your business rules.
    """
    df = df.copy()

    # Example placeholder:
    # df["ano_referencia"] = pd.to_datetime(df["data_evento"]).dt.year

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