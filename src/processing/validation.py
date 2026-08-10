"""Validacao de qualidade da base consolidada de escalas portuarias."""

import pandas as pd


EVENT_TIMESTAMP_COLUMNS = [
    "arrival_port_ts",
    "berthing_ts",
    "unberthing_ts",
    "departure_port_ts",
]

DURATION_COLUMNS = [
    "tmp_wait_for_berthing_h",
    "tmp_operation_h",
    "tmp_post_operation_h",
    "tmp_total_port_stay_h",
]


def add_event_presence_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Cria flags de presenca dos quatro eventos temporais principais.

    Uma escala so pode entrar na EDA de duracoes se tiver chegada, atracacao,
    desatracacao e saida. As flags deixam transparente quantas linhas sao
    descartadas por falta de cada evento.

    Parameters
    ----------
    df:
        Base consolidada com uma linha por `port_call_id`.

    Returns
    -------
    pandas.DataFrame
        Copia com colunas `has_*` para cada timestamp principal.
    """
    df = df.copy()

    for col in EVENT_TIMESTAMP_COLUMNS:
        df[f"has_{col}"] = df[col].notna()

    return df


def add_temporal_consistency_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Marca escalas com ordem temporal impossivel entre eventos.

    As flags identificam casos em que um evento posterior aparece antes do evento
    anterior. Esses registros nao sao usados para os targets porque gerariam
    duracoes negativas ou incoerentes.

    Parameters
    ----------
    df:
        Base consolidada com timestamps principais.

    Returns
    -------
    pandas.DataFrame
        Copia com flags booleanas de inconsistencia temporal.
    """
    df = df.copy()

    df["flag_arrival_after_berthing"] = df["arrival_port_ts"] > df["berthing_ts"]
    df["flag_berthing_after_unberthing"] = df["berthing_ts"] > df["unberthing_ts"]
    df["flag_unberthing_after_departure"] = df["unberthing_ts"] > df["departure_port_ts"]

    return df


def add_duration_check_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula duracoes temporarias usadas apenas no controle de qualidade.

    As colunas `tmp_*` permitem encontrar duracoes negativas ou extremamente
    longas antes da criacao dos targets finais. Elas sao mantidas na base de QC
    para auditoria do Capitulo 3.

    Parameters
    ----------
    df:
        Base consolidada com timestamps principais.

    Returns
    -------
    pandas.DataFrame
        Copia com duracoes temporarias em horas e flags de valores negativos.
    """
    df = df.copy()

    df["tmp_wait_for_berthing_h"] = (
        df["berthing_ts"] - df["arrival_port_ts"]
    ).dt.total_seconds() / 3600

    df["tmp_operation_h"] = (
        df["unberthing_ts"] - df["berthing_ts"]
    ).dt.total_seconds() / 3600

    df["tmp_post_operation_h"] = (
        df["departure_port_ts"] - df["unberthing_ts"]
    ).dt.total_seconds() / 3600

    df["tmp_total_port_stay_h"] = (
        df["departure_port_ts"] - df["arrival_port_ts"]
    ).dt.total_seconds() / 3600

    for col in DURATION_COLUMNS:
        df[f"flag_negative_{col}"] = df[col] < 0

    return df


def add_extreme_duration_flags(
    df: pd.DataFrame,
    max_wait_days: int = 30,
    max_operation_days: int = 30,
    max_total_days: int = 60,
) -> pd.DataFrame:
    """Marca duracoes longas demais para a base analitica final.

    Os limites sao regras de qualidade pragmaticas para reduzir influencia de
    registros possivelmente incorretos ou escalas atipicas demais para o escopo
    do TCC.

    Parameters
    ----------
    df:
        Base com duracoes temporarias em horas.
    max_wait_days:
        Limite maximo, em dias, para espera ate atracacao.
    max_operation_days:
        Limite maximo, em dias, para operacao atracada.
    max_total_days:
        Limite maximo, em dias, para permanencia total no porto.

    Returns
    -------
    pandas.DataFrame
        Copia com flags de duracoes extremas.

    Risco metodologico
    ------------------
    Esses limites removem caudas extremas. Qualquer mudanca neles altera a base
    empirica e deve ser documentada no texto do TCC.
    """
    df = df.copy()

    df["flag_wait_too_long"] = df["tmp_wait_for_berthing_h"] > (24 * max_wait_days)
    df["flag_operation_too_long"] = df["tmp_operation_h"] > (24 * max_operation_days)
    df["flag_total_too_long"] = df["tmp_total_port_stay_h"] > (24 * max_total_days)

    return df


def define_eda_eligibility(
    df: pd.DataFrame,
    min_arrival_date: str = "2023-01-01",
) -> pd.DataFrame:
    """Define quais escalas entram na EDA e na criacao dos targets.

    A elegibilidade exige eventos completos, ordem temporal coerente, chegada a
    partir de `min_arrival_date` e duracoes dentro dos limites definidos nas
    etapas anteriores.

    Parameters
    ----------
    df:
        Base consolidada com flags de qualidade.
    min_arrival_date:
        Data minima de chegada considerada no recorte empirico do TCC.

    Returns
    -------
    pandas.DataFrame
        Copia com `flag_arrival_before_min_date` e `eligible_for_eda`.

    Risco metodologico
    ------------------
    Este filtro define a populacao efetivamente analisada. Ele nao deve ser
    alterado silenciosamente porque muda todas as estatisticas do Capitulo 3.
    """
    df = df.copy()

    min_arrival_ts = pd.Timestamp(min_arrival_date)
    df["flag_arrival_before_min_date"] = df["arrival_port_ts"] < min_arrival_ts

    df["eligible_for_eda"] = (
        df["has_arrival_port_ts"]
        & df["has_berthing_ts"]
        & df["has_unberthing_ts"]
        & df["has_departure_port_ts"]
        & ~df["flag_arrival_before_min_date"].fillna(False)
        & ~df["flag_arrival_after_berthing"].fillna(False)
        & ~df["flag_berthing_after_unberthing"].fillna(False)
        & ~df["flag_unberthing_after_departure"].fillna(False)
        & ~df["flag_negative_tmp_wait_for_berthing_h"].fillna(False)
        & ~df["flag_negative_tmp_operation_h"].fillna(False)
        & ~df["flag_negative_tmp_post_operation_h"].fillna(False)
        & ~df["flag_negative_tmp_total_port_stay_h"].fillna(False)
        & ~df["flag_wait_too_long"].fillna(False)
        & ~df["flag_operation_too_long"].fillna(False)
        & ~df["flag_total_too_long"].fillna(False)
    )

    return df


def build_quality_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Resume os resultados do controle de qualidade das escalas.

    A tabela gerada permite reportar no TCC quantos registros existem no bruto,
    quantos identificadores unicos aparecem e quantos registros foram excluidos
    por cada criterio de qualidade.

    Parameters
    ----------
    df:
        Base consolidada apos todas as flags de qualidade.

    Returns
    -------
    pandas.DataFrame
        Tabela com pares `metric`/`value` para auditoria e exportacao.
    """
    summary = pd.DataFrame(
        {
            "metric": [
                "total_rows",
                "unique_port_call_id",
                "missing_arrival_port_ts",
                "missing_berthing_ts",
                "missing_unberthing_ts",
                "missing_departure_port_ts",
                "arrival_before_min_date",
                "arrival_after_berthing",
                "berthing_after_unberthing",
                "unberthing_after_departure",
                "negative_wait_for_berthing",
                "negative_operation",
                "negative_post_operation",
                "negative_total_port_stay",
                "wait_too_long",
                "operation_too_long",
                "total_too_long",
                "eligible_for_eda",
            ],
            "value": [
                len(df),
                df["port_call_id"].nunique(),
                (~df["has_arrival_port_ts"]).sum(),
                (~df["has_berthing_ts"]).sum(),
                (~df["has_unberthing_ts"]).sum(),
                (~df["has_departure_port_ts"]).sum(),
                df["flag_arrival_before_min_date"].fillna(False).sum(),
                df["flag_arrival_after_berthing"].fillna(False).sum(),
                df["flag_berthing_after_unberthing"].fillna(False).sum(),
                df["flag_unberthing_after_departure"].fillna(False).sum(),
                df["flag_negative_tmp_wait_for_berthing_h"].fillna(False).sum(),
                df["flag_negative_tmp_operation_h"].fillna(False).sum(),
                df["flag_negative_tmp_post_operation_h"].fillna(False).sum(),
                df["flag_negative_tmp_total_port_stay_h"].fillna(False).sum(),
                df["flag_wait_too_long"].fillna(False).sum(),
                df["flag_operation_too_long"].fillna(False).sum(),
                df["flag_total_too_long"].fillna(False).sum(),
                df["eligible_for_eda"].fillna(False).sum(),
            ],
        }
    )

    return summary
