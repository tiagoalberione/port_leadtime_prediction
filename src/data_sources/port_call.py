from pathlib import Path
import pandas as pd

from src.io_utils import load_csv_files_from_dir
from src.processing.cleaning import (
    standardize_column_names,
    trim_string_columns,
    parse_datetime_columns,
)


def load_port_call_files(input_dir: Path) -> pd.DataFrame:
    """Load raw estadia files from directory."""
    return load_csv_files_from_dir(input_dir=input_dir, add_source_file=False)


def rename_port_call_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename raw estadia columns to project standard."""
    rename_map = {
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
        "porto_destino_nome": "destination_port_name"
    }
    return df.rename(columns=rename_map)


def process_port_call(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize estadia data."""
    df = standardize_column_names(df)
    df = trim_string_columns(df)

    datetime_cols = [
        "estadia_chegada_no_porto",
        "estadia_atracacao",
        "estadia_desatracacao",
        "estadia_saida_do_porto",
    ]
    df = parse_datetime_columns(df, datetime_cols)
    df = rename_port_call_columns(df)

    return df


def load_estadia_files(input_dir: Path) -> pd.DataFrame:
    """Backward-compatible alias for legacy imports."""
    return load_port_call_files(input_dir)


def process_estadia(df: pd.DataFrame) -> pd.DataFrame:
    """Backward-compatible alias for legacy imports."""
    return process_port_call(df)
