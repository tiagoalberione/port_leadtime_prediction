"""Criacao dos targets de duracao usados na EDA e na modelagem."""

import numpy as np
import pandas as pd


TARGET_COLUMNS = [
    "t_wait_for_berthing_h",
    "t_operation_h",
    "t_post_operation_h",
    "t_total_port_stay_h",
]


def filter_eligible_port_calls(df: pd.DataFrame) -> pd.DataFrame:
    """Mantem apenas escalas aprovadas pelo controle de qualidade.

    A funcao separa claramente a decisao de elegibilidade, feita em
    `validation.py`, da criacao dos targets finais. Assim, a base de QC continua
    auditavel e a base de targets recebe apenas registros metodologicamente
    validos para analise de duracao.

    Parameters
    ----------
    df:
        Base consolidada com a coluna booleana `eligible_for_eda`.

    Returns
    -------
    pandas.DataFrame
        Copia contendo somente registros elegiveis.

    Raises
    ------
    ValueError
        Quando a coluna de elegibilidade ainda nao foi criada.
    """
    if "eligible_for_eda" not in df.columns:
        raise ValueError("Column 'eligible_for_eda' not found in DataFrame.")

    return df[df["eligible_for_eda"]].copy()


def create_duration_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula os targets finais de duracao em horas.

    O target principal do TCC e `t_total_port_stay_h`, medido da chegada ao porto
    ate a saida. Os componentes de espera, operacao e pos-operacao ajudam a EDA
    a explicar de onde vem a permanencia total.

    Parameters
    ----------
    df:
        Base elegivel com timestamps completos e coerentes.

    Returns
    -------
    pandas.DataFrame
        Copia com targets de duracao em horas.

    Risco metodologico
    ------------------
    Estes targets usam eventos conhecidos apenas depois que a escala terminou.
    Eles sao variaveis-resposta, nao features disponiveis no instante da chegada.
    """
    df = df.copy()

    df["t_wait_for_berthing_h"] = (
        df["berthing_ts"] - df["arrival_port_ts"]
    ).dt.total_seconds() / 3600

    df["t_operation_h"] = (
        df["unberthing_ts"] - df["berthing_ts"]
    ).dt.total_seconds() / 3600

    df["t_post_operation_h"] = (
        df["departure_port_ts"] - df["unberthing_ts"]
    ).dt.total_seconds() / 3600

    df["t_total_port_stay_h"] = (
        df["departure_port_ts"] - df["arrival_port_ts"]
    ).dt.total_seconds() / 3600

    return df


def create_duration_targets_in_days(df: pd.DataFrame) -> pd.DataFrame:
    """Cria versoes em dias dos targets de duracao.

    As colunas em horas preservam a unidade de avaliacao dos modelos. As versoes
    em dias facilitam tabelas e interpretacoes descritivas no texto do TCC.

    Parameters
    ----------
    df:
        Base com os targets em horas ja calculados.

    Returns
    -------
    pandas.DataFrame
        Copia com colunas equivalentes terminadas em `_d`.
    """
    df = df.copy()

    for col in TARGET_COLUMNS:
        df[col.replace("_h", "_d")] = df[col] / 24

    return df


def create_log_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Cria versoes log-transformadas dos targets em horas.

    A transformacao `log1p` e util porque os tempos de permanencia costumam ter
    cauda longa. Ela preserva zeros e reduz a influencia visual e estatistica de
    valores muito altos em analises auxiliares.

    Parameters
    ----------
    df:
        Base com targets em horas nao negativos.

    Returns
    -------
    pandas.DataFrame
        Copia com colunas `log_*` para cada target em horas.

    Risco metodologico
    ------------------
    A escala log deve ser interpretada como transformacao auxiliar. Resultados
    finais em horas precisam voltar a escala original quando forem comunicados.
    """
    df = df.copy()

    for col in TARGET_COLUMNS:
        df[f"log_{col}"] = np.log1p(df[col])

    return df


def create_target_severity_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Cria flags descritivas de duracoes altas e extremas.

    As flags usam os quantis empiricos P75 e P90 da propria base elegivel para
    apoiar a EDA de severidade. Elas nao representam uma regra operacional real,
    apenas uma forma simples de segmentar a distribuicao observada.

    Parameters
    ----------
    df:
        Base com targets em horas.

    Returns
    -------
    pandas.DataFrame
        Copia com flags `*_high` e `*_extreme` para targets selecionados.

    Risco metodologico
    ------------------
    Como os cortes sao calculados com a base completa, estas flags sao
    descritivas. Elas nao devem ser usadas como features preditivas no Capitulo 4.
    """
    df = df.copy()

    cols_for_flags = [
        "t_wait_for_berthing_h",
        "t_operation_h",
        "t_total_port_stay_h",
    ]

    for col in cols_for_flags:
        p75 = df[col].quantile(0.75)
        p90 = df[col].quantile(0.90)

        df[f"{col}_high"] = df[col] > p75
        df[f"{col}_extreme"] = df[col] > p90

    return df


def build_target_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Resume a distribuicao dos targets finais.

    A tabela exportada apoia o Capitulo 3 ao mostrar media, mediana, dispersao e
    cauda dos tempos de permanencia e seus componentes.

    Parameters
    ----------
    df:
        Base elegivel com targets calculados em horas.

    Returns
    -------
    pandas.DataFrame
        Tabela com estatisticas descritivas por target.
    """
    rows = []

    for col in TARGET_COLUMNS:
        rows.append(
            {
                "target": col,
                "count": df[col].count(),
                "mean": df[col].mean(),
                "median": df[col].median(),
                "std": df[col].std(),
                "min": df[col].min(),
                "p25": df[col].quantile(0.25),
                "p75": df[col].quantile(0.75),
                "p90": df[col].quantile(0.90),
                "p95": df[col].quantile(0.95),
                "max": df[col].max(),
            }
        )

    return pd.DataFrame(rows)
