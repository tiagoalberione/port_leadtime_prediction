from pathlib import Path
from typing import Iterable

import pandas as pd


def list_csv_files(folder: Path) -> list[Path]:
    """List all CSV files in a folder."""
    return sorted(folder.glob("*.csv"))


def read_csv_flexible(
    file_path: Path,
    sep: str = ";",
    encoding: str = "utf-8",
    low_memory: bool = False,
) -> pd.DataFrame:
    """
    Read a CSV file with basic flexibility.

    This function can be extended later to try multiple encodings
    and separators automatically.
    """
    return pd.read_csv(
        file_path,
        sep=sep,
        encoding=encoding,
        low_memory=low_memory,
    )


def concat_dataframes(dataframes: Iterable[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate multiple DataFrames into a single DataFrame."""
    return pd.concat(list(dataframes), ignore_index=True)


def save_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """Save DataFrame as parquet."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def save_csv(df: pd.DataFrame, output_path: Path) -> None:
    """Save DataFrame as CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")