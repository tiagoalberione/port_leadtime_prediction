from pathlib import Path

import pandas as pd

from src.config.paths import INTERIM_ESTADIA_DIR, RAW_ESTADIA_DIR
from src.utils.io_utils import (
    concat_dataframes,
    list_csv_files,
    read_csv_flexible,
    save_parquet,
)


def load_estadia_files(
    input_dir: Path = RAW_ESTADIA_DIR,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """
    Load all CSV files from the estadia_embarcacao folder
    and concatenate them into a single DataFrame.
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

    df_estadia = concat_dataframes(dataframes)
    return df_estadia


def save_estadia_raw_consolidated(df: pd.DataFrame) -> Path:
    """
    Save consolidated raw estadia DataFrame in interim layer.
    """
    output_path = INTERIM_ESTADIA_DIR / "estadia_raw_consolidated.parquet"
    save_parquet(df, output_path)
    return output_path


def run_estadia_ingestion(
    input_dir: Path = RAW_ESTADIA_DIR,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """
    Full ingestion pipeline for estadia_embarcacao.
    """
    df_estadia = load_estadia_files(
        input_dir=input_dir,
        sep=sep,
        encoding=encoding,
    )

    save_estadia_raw_consolidated(df_estadia)
    return df_estadia