"""Consolidacao dos registros brutos em uma linha por escala portuaria."""

import pandas as pd


TEXT_COLUMNS = [
    "port_call_id",
    "port",
    "port_name",
    "imo",
    "vessel_id",
    "vessel_name",
    "operation_type",
    "source_port",
    "source_port_name",
    "destination_port",
    "destination_port_name",
]

TIMESTAMP_AGGREGATIONS = {
    "arrival_port_ts": "min",
    "berthing_ts": "min",
    "unberthing_ts": "max",
    "departure_port_ts": "max",
}

DISPLAY_COLUMN_PAIRS = {
    "port_display": ("port", "port_name"),
    "source_port_display": ("source_port", "source_port_name"),
    "destination_port_display": ("destination_port", "destination_port_name"),
}


def build_master_calls(df_port_call: pd.DataFrame) -> pd.DataFrame:
    """Consolida os eventos brutos em uma linha por `port_call_id`.

    O Porto Sem Papel pode trazer mais de um registro associado a mesma escala.
    Para o TCC, cada escala precisa representar uma unica observacao. Por isso,
    esta funcao mantem os atributos textuais do primeiro registro e consolida os
    timestamps de forma coerente com a sequencia temporal da escala:

    - primeira chegada ao porto;
    - primeira atracacao;
    - ultima desatracacao;
    - ultima saida do porto.

    Parameters
    ----------
    df_port_call:
        DataFrame ja limpo e com os nomes de colunas padronizados.

    Returns
    -------
    pandas.DataFrame
        Base com uma linha por `port_call_id`, usada posteriormente nos testes
        de qualidade e no calculo do tempo total de permanencia.

    Risco metodologico
    ------------------
    A escolha de minimos para eventos iniciais e maximos para eventos finais
    define a duracao analisada. Ela deve continuar alinhada a interpretacao de
    permanencia total no porto, da chegada a saida.
    """
    if "port_call_id" not in df_port_call.columns:
        raise ValueError("Column 'port_call_id' not found in port call data.")

    required_cols = TEXT_COLUMNS + list(TIMESTAMP_AGGREGATIONS)

    # Alguns arquivos historicos podem nao conter todos os atributos textuais.
    # Trabalhamos apenas com as colunas realmente disponiveis, sem inventar dados.
    available_cols = [col for col in required_cols if col in df_port_call.columns]
    df = df_port_call[available_cols].copy()

    agg_dict = {
        col: "first"
        for col in TEXT_COLUMNS
        if col in df.columns
    }

    # Os minimos representam os primeiros eventos e os maximos os ultimos.
    for col, aggregation in TIMESTAMP_AGGREGATIONS.items():
        if col in df.columns:
            agg_dict[col] = aggregation

    master = (
        df.groupby("port_call_id", as_index=False)
        .agg(agg_dict)
        .sort_values(["port", "arrival_port_ts"], na_position="last")
        .reset_index(drop=True)
    )

    # Colunas de exibicao facilitam a leitura das tabelas e graficos da EDA.
    for display_col, (code_col, name_col) in DISPLAY_COLUMN_PAIRS.items():
        if code_col in master.columns and name_col in master.columns:
            master[display_col] = (
                master[code_col].astype("string").fillna("")
                + " - "
                + master[name_col].astype("string").fillna("")
            )

    return master
