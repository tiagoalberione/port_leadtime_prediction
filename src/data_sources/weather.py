"""Leitura e preparação da base diária de clima usada no TCC."""

from pathlib import Path

import pandas as pd

from src.io_utils import load_csv_files_from_dir
from src.processing.cleaning import standardize_column_names, trim_string_columns


COORD_DECIMALS = 5
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
    """Carrega os CSVs brutos de clima diário.

    O clima entra no Capítulo 3 como enriquecimento descritivo da permanência em
    porto. A leitura é separada da transformação para manter a origem dos dados
    auditável.

    Parameters
    ----------
    input_dir:
        Diretório `data/raw/weather` com os arquivos climáticos.

    Returns
    -------
    pandas.DataFrame
        Arquivos climáticos concatenados, ainda em formato bruto.
    """
    return load_csv_files_from_dir(
        input_dir=input_dir,
        add_source_file=False,
        min_columns=5,
    )


def process_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa a série diária de clima por coordenada.

    A função padroniza nomes, converte a data para dia calendário, transforma
    medidas climáticas em numéricas e cria coordenadas arredondadas para merge
    com portos.

    Parameters
    ----------
    df:
        Dados brutos de clima diário.

    Returns
    -------
    pandas.DataFrame
        Série diária limpa, com uma linha por coordenada arredondada e data.

    Risco metodológico
    ------------------
    As variáveis climáticas sem defasagem são agregados realizados do dia inteiro.
    Elas podem apoiar EDA, mas não devem ser tratadas como conhecidas no momento
    de chegada da embarcação.
    """
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

    df["latitude_r"] = df["latitude"].round(COORD_DECIMALS)
    df["longitude_r"] = df["longitude"].round(COORD_DECIMALS)

    # Se a fonte trouxer duplicatas para a mesma coordenada/data, preservamos a
    # última linha após ordenação para manter o comportamento histórico do TCC.
    df = (
        df.sort_values(["latitude_r", "longitude_r", "date"])
        .drop_duplicates(subset=["latitude_r", "longitude_r", "date"], keep="last")
        .reset_index(drop=True)
    )

    return df