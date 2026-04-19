from pathlib import Path

import pandas as pd

from src.io_utils import load_csv_files_from_dir
from src.processing.cleaning import standardize_column_names, trim_string_columns


WEATHER_NUMERIC_COLUMNS = [
    "latitude",
    "longitude",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "rain_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
]


def load_weather_files(input_dir: Path) -> pd.DataFrame:
    """Load raw weather files from directory."""
    return load_csv_files_from_dir(
        input_dir=input_dir,
        add_source_file=False,
        min_columns=5,
    )


def process_weather(df: pd.DataFrame, coord_decimals: int = 5) -> pd.DataFrame:
    """Clean and standardize daily weather data."""
    df = df.copy()

    df = standardize_column_names(df)
    df = trim_string_columns(df)

    if "date" not in df.columns:
        raise ValueError("Column 'date' not found in weather data.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.floor("D")

    for col in WEATHER_NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "latitude" not in df.columns or "longitude" not in df.columns:
        raise ValueError("Weather data must contain 'latitude' and 'longitude'.")

    df["latitude_r"] = df["latitude"].round(coord_decimals)
    df["longitude_r"] = df["longitude"].round(coord_decimals)

    df = (
        df.sort_values(["latitude_r", "longitude_r", "date"])
        .drop_duplicates(subset=["latitude_r", "longitude_r", "date"], keep="last")
        .reset_index(drop=True)
    )

    return df