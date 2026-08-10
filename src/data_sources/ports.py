"""Leitura e preparação da referência de portos usada na base analítica."""

from pathlib import Path

import pandas as pd

from src.io_utils import load_csv_files_from_dir
from src.processing.cleaning import standardize_column_names, trim_string_columns


COORD_DECIMALS = 5
PORT_NUMERIC_COLUMNS = ["latitude", "longitude"]
REQUIRED_PORT_COLUMNS = [
    "port",
    "port_name",
    "city",
    "state",
    "region",
    "latitude",
    "longitude",
]


def load_port_files(input_dir: Path) -> pd.DataFrame:
    """Carrega os CSVs brutos da referência de portos.

    A referência de portos liga o bitrigrama da escala a cidade, estado, região e
    coordenadas. Essas informações sustentam a EDA regional e o merge com clima.

    Parameters
    ----------
    input_dir:
        Diretório `data/raw/ports` com os arquivos da fonte de portos.

    Returns
    -------
    pandas.DataFrame
        Arquivos de portos concatenados, ainda em formato bruto.
    """
    return load_csv_files_from_dir(
        input_dir=input_dir,
        add_source_file=False,
        min_columns=5,
    )


def process_ports(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa a referência de portos e mantém uma linha por código de porto.

    O Capítulo 3 precisa de uma referência simples e auditável para enriquecer a
    escala portuária com localização. Coordenadas arredondadas a 5 casas são
    usadas como chave de ligação com a base diária de clima.

    Parameters
    ----------
    df:
        Dados brutos de portos.

    Returns
    -------
    pandas.DataFrame
        Referência limpa de portos com coordenadas numéricas, coordenadas
        arredondadas e coluna `port_display` para tabelas e gráficos.

    Risco metodológico
    ------------------
    Quando há mais de uma linha para o mesmo `port`, o pipeline mantém a primeira
    após ordenação por código. Mudar essa regra pode alterar a região, a
    coordenada climática e os resultados da EDA.
    """
    df = standardize_column_names(df)
    df = trim_string_columns(df)

    missing_cols = [col for col in REQUIRED_PORT_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in ports data: {missing_cols}")

    for col in PORT_NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["latitude_r"] = df["latitude"].round(COORD_DECIMALS)
    df["longitude_r"] = df["longitude"].round(COORD_DECIMALS)

    df = (
        df.sort_values(["port"])
        .drop_duplicates(subset=["port"], keep="first")
        .reset_index(drop=True)
    )

    df["port_display"] = (
        df["port"].astype("string").fillna("")
        + " - "
        + df["city"].astype("string").fillna("")
        + " - "
        + df["state"].astype("string").fillna("")
    )

    return df