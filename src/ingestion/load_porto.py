from pathlib import Path

import pandas as pd

from src.config.paths import INTERIM_PORTO_DIR, RAW_PORTO_DIR
from src.utils.io_utils import (
    concat_dataframes,
    list_csv_files,
    read_csv_flexible,
    save_parquet,
)


def load_porto_files(
    input_dir: Path = RAW_PORTO_DIR,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """
    Load all CSV files from the porto folder and concatenate them.
    """
    csv_files = list_csv_files(input_dir)

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files were found in folder: {input_dir}"
        )

    dataframes = []
    for file_path in csv_files:
        df_file = read_csv_flexible(
            file_path=file_path,
            sep=sep,
            encoding=encoding,
        )

        # Track source file for traceability
        #df_file["source_file"] = file_path.name

        dataframes.append(df_file)

    return concat_dataframes(dataframes)


def save_porto_raw_consolidated(df: pd.DataFrame) -> Path:
    """
    Save consolidated raw porto DataFrame in interim layer.
    """
    output_path = INTERIM_PORTO_DIR / "porto_raw_consolidated.parquet"
    save_parquet(df, output_path)
    return output_path


def run_porto_ingestion(
    input_dir: Path = RAW_PORTO_DIR,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """
    Full ingestion pipeline for porto.
    """
    df_porto = load_porto_files(
        input_dir=input_dir,
        sep=sep,
        encoding=encoding,
    )

    save_porto_raw_consolidated(df_porto)
    return df_porto
