from pathlib import Path

import pandas as pd

from src.io_utils import load_csv_files_from_dir
from src.processing.cleaning import standardize_column_names, trim_string_columns


PORT_NUMERIC_COLUMNS = ["latitude", "longitude"]


def load_port_files(input_dir: Path) -> pd.DataFrame:
    """Load raw port reference files from directory."""
    return load_csv_files_from_dir(
        input_dir=input_dir,
        add_source_file=False,
        min_columns=5,
    )


def process_ports(df: pd.DataFrame, coord_decimals: int = 5) -> pd.DataFrame:
    """Clean and standardize port reference data."""
    df = df.copy()

    df = standardize_column_names(df)
    df = trim_string_columns(df)

    required_cols = [
        "port",
        "port_name",
        "city",
        "state",
        "region",
        "latitude",
        "longitude",
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in ports data: {missing_cols}")

    for col in PORT_NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["latitude_r"] = df["latitude"].round(coord_decimals)
    df["longitude_r"] = df["longitude"].round(coord_decimals)

    df = (
        df.sort_values(["port"])
        .drop_duplicates(subset=["port"], keep="first")
        .reset_index(drop=True)
    )

    return df