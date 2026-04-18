from pathlib import Path

import pandas as pd

from src.config.paths import INTERIM_METEOROLOGIA_DIR, RAW_METEOROLOGIA_DIR
from src.utils.io_utils import (
    concat_dataframes,
    list_csv_files,
    read_csv_flexible,
    save_parquet,
)


def load_meteorologia_files(
    input_dir: Path = RAW_METEOROLOGIA_DIR,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """
    Load all CSV files from the meteorologia folder and concatenate them.
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


def save_meteorologia_raw_consolidated(df: pd.DataFrame) -> Path:
    """
    Save consolidated raw porto DataFrame in interim layer.
    """
    output_path = INTERIM_METEOROLOGIA_DIR / "meteorologia_raw_consolidated.parquet"
    save_parquet(df, output_path)
    return output_path


def run_meteorologia_ingestion(
    input_dir: Path = RAW_METEOROLOGIA_DIR,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """
    Full ingestion pipeline for porto.
    """
    df_meteorologia = load_meteorologia_files(
        input_dir=input_dir,
        sep=sep,
        encoding=encoding,
    )

    save_meteorologia_raw_consolidated(df_meteorologia)
    return df_meteorologia
