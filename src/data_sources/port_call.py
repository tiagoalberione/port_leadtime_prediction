"""Leitura e preparacao inicial dos arquivos de estadia portuaria."""

from pathlib import Path

import pandas as pd

from src.io_utils import load_csv_files_from_dir
from src.processing.cleaning import (
    parse_datetime_columns,
    standardize_column_names,
    trim_string_columns,
)


PORT_CALL_DATETIME_COLUMNS = [
    "estadia_chegada_no_porto",
    "estadia_atracacao",
    "estadia_desatracacao",
    "estadia_saida_do_porto",
]

PORT_CALL_RENAME_MAP = {
    "duv": "port_call_id",
    "porto_bitrigrama": "port",
    "porto_nome": "port_name",
    "embarcacao_imo": "imo",
    "embarcacao_inscricao": "vessel_id",
    "embarcacao_nome": "vessel_name",
    "tripulantes_embarque": "boarding_crew",
    "tripulantes_transito": "transit_crew",
    "tripulantes_desembarque": "unboarding_crew",
    "tripulantes_total": "total_crew",
    "passageiros_embarque": "boarding_passengers",
    "passageiros_transito": "transit_passengers",
    "passageiros_desembarque": "unboarding_passengers",
    "passageiros_total": "total_passengers",
    "estadia_motivos_atracacao": "operation_type",
    "estadia_chegada_no_porto": "arrival_port_ts",
    "estadia_atracacao": "berthing_ts",
    "estadia_desatracacao": "unberthing_ts",
    "estadia_saida_do_porto": "departure_port_ts",
    "porto_origem_bitrigrama": "source_port",
    "porto_origem_nome": "source_port_name",
    "porto_destino_bitrigrama": "destination_port",
    "porto_destino_nome": "destination_port_name",
}


def load_port_call_files(input_dir: Path) -> pd.DataFrame:
    """Carrega os CSVs brutos de estadia de embarcacoes.

    A fonte de estadia e a base principal do Capitulo 3. Ela contem os eventos
    temporais que permitem construir uma linha por escala portuaria e calcular o
    tempo total de permanencia no porto.

    Parameters
    ----------
    input_dir:
        Diretorio `data/raw/estadia` com os arquivos CSV da fonte.

    Returns
    -------
    pandas.DataFrame
        Arquivos concatenados, ainda com nomes e valores brutos.
    """
    return load_csv_files_from_dir(
        input_dir=input_dir,
        add_source_file=False,
        min_columns=5,
    )


def rename_port_call_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia colunas de estadia para os nomes usados no projeto.

    Os nomes em ingles sao internos ao codigo para manter consistencia entre
    fontes. O mapeamento fica concentrado aqui para que o autor consiga auditar
    a passagem entre o layout original e a base analitica.

    Parameters
    ----------
    df:
        DataFrame com colunas ja padronizadas por `standardize_column_names`.

    Returns
    -------
    pandas.DataFrame
        Copia com as colunas conhecidas renomeadas.
    """
    return df.rename(columns=PORT_CALL_RENAME_MAP)


def process_port_call(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza os dados brutos de estadia.

    Esta etapa prepara a fonte principal antes da consolidacao por `port_call_id`:
    normaliza nomes, limpa textos, converte timestamps e aplica o dicionario de
    nomes do projeto.

    Parameters
    ----------
    df:
        Dados brutos concatenados da fonte de estadia.

    Returns
    -------
    pandas.DataFrame
        Dados de estadia padronizados, ainda podendo conter mais de uma linha por
        escala portuaria.

    Risco metodologico
    ------------------
    A conversao de datas define a escala temporal usada para calcular os targets.
    Por isso, timestamps com fuso sao convertidos para America/Sao_Paulo e
    timestamps sem fuso sao tratados como horario local informado pela fonte.
    """
    df = standardize_column_names(df)
    df = trim_string_columns(df)
    df = parse_datetime_columns(
        df,
        datetime_cols=PORT_CALL_DATETIME_COLUMNS,
        local_tz="America/Sao_Paulo",
    )
    return rename_port_call_columns(df)
