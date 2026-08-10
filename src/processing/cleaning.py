"""Limpezas basicas compartilhadas pela preparacao dos dados.

As funcoes deste modulo tratam problemas recorrentes dos arquivos brutos:
nomes de colunas com acentos, espacos invisiveis, marcadores textuais de nulo e
datas com ou sem fuso horario. Sao transformacoes de suporte para o Capitulo 3,
antes da consolidacao por escala portuaria.
"""

import re
import unicodedata

import pandas as pd


TZ_OFFSET_PATTERN = re.compile(r"(?:Z|[+-]\d{2}(?::?\d{2})?)$")
DEFAULT_LOCAL_TZ = "America/Sao_Paulo"
MISSING_TOKENS = {
    "": pd.NA,
    " ": pd.NA,
    "nan": pd.NA,
    "NaN": pd.NA,
    "none": pd.NA,
    "None": pd.NA,
    "null": pd.NA,
    "NULL": pd.NA,
    "<NA>": pd.NA,
}


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes de colunas para snake_case sem acentos.

    A padronizacao permite que o restante do pipeline use nomes estaveis,
    independentemente de acentos, maiusculas ou pontuacao nos arquivos originais.

    Parameters
    ----------
    df:
        DataFrame bruto ou intermediario.

    Returns
    -------
    pandas.DataFrame
        Copia do DataFrame com nomes de colunas normalizados.
    """
    new_columns = []

    for col in df.columns:
        col = (
            unicodedata.normalize("NFKD", str(col))
            .encode("ascii", "ignore")
            .decode("utf-8")
        )
        col = col.strip().lower()
        col = re.sub(r"[^a-z0-9]+", "_", col)
        col = re.sub(r"_+", "_", col).strip("_")
        new_columns.append(col)

    df = df.copy()
    df.columns = new_columns
    return df


def normalize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Converte marcadores textuais comuns de nulo para `pd.NA`.

    Esta etapa reduz diferencas artificiais entre arquivos, como celulas vazias,
    `NULL` ou `nan` em texto. Isso evita que valores ausentes sejam tratados como
    categorias reais na EDA ou em etapas posteriores.

    Parameters
    ----------
    df:
        DataFrame com possiveis colunas textuais.

    Returns
    -------
    pandas.DataFrame
        Copia com marcadores textuais de nulo normalizados.
    """
    df = df.copy()
    object_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in object_cols:
        df[col] = df[col].replace(MISSING_TOKENS)

    return df


def trim_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espacos e quebras de linha das colunas textuais.

    Os dados brutos podem conter quebras de linha ou tabulacoes dentro de campos
    textuais. A limpeza deixa chaves e categorias comparaveis entre arquivos.

    Parameters
    ----------
    df:
        DataFrame que sera limpo.

    Returns
    -------
    pandas.DataFrame
        Copia com colunas textuais aparadas e nulos normalizados.
    """
    df = df.copy()
    object_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in object_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.replace(r"[\r\n\t]+", " ", regex=True)
            .str.strip()
        )

    return normalize_missing_values(df)


def parse_mixed_datetime_series(
    s: pd.Series,
    local_tz: str = DEFAULT_LOCAL_TZ,
) -> pd.Series:
    """Converte uma serie de datas com mistura de formatos de fuso horario.

    Alguns arquivos trazem timestamps com offset explicito, como
    `2020-10-30 18:00:00-03`, enquanto outros trazem horario local sem fuso,
    como `2025-12-31 23:00:00.0000000`. Para comparar duracoes, os valores com
    fuso sao convertidos para `local_tz` e depois ficam sem timezone, como hora
    local de parede.

    Parameters
    ----------
    s:
        Serie textual ou mista com datas e horarios.
    local_tz:
        Fuso horario local usado para converter valores que ja tem offset.

    Returns
    -------
    pandas.Series
        Serie `datetime64[ns]` sem timezone.

    Risco metodologico
    ------------------
    Valores sem offset sao assumidos como horario local. Essa escolha deve ser
    mantida consistente porque os targets sao diferencas entre eventos da escala.
    """
    s = s.copy()

    s = (
        s.astype("string")
        .str.replace(r"[\r\n\t]+", " ", regex=True)
        .str.strip()
        .replace(MISSING_TOKENS)
    )

    has_tz = s.str.contains(TZ_OFFSET_PATTERN, na=False)
    result = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    # Valores com offset sao trazidos para o fuso local antes de remover o tz.
    if has_tz.any():
        aware = pd.to_datetime(s[has_tz], errors="coerce", utc=True)
        aware = aware.dt.tz_convert(local_tz).dt.tz_localize(None)
        result.loc[has_tz] = aware

    # Valores sem offset ja representam a hora local informada pela fonte.
    if (~has_tz).any():
        naive = pd.to_datetime(s[~has_tz], errors="coerce")
        result.loc[~has_tz] = naive

    return result


def parse_datetime_columns(
    df: pd.DataFrame,
    datetime_cols: list[str],
    local_tz: str = DEFAULT_LOCAL_TZ,
) -> pd.DataFrame:
    """Aplica a conversao de datas nas colunas selecionadas.

    A funcao mantem o pipeline explicito: cada fonte decide quais colunas sao
    timestamps relevantes, e esta rotina apenas aplica a regra comum de parsing.

    Parameters
    ----------
    df:
        DataFrame que contem as colunas de data.
    datetime_cols:
        Lista de colunas que devem ser convertidas quando existirem.
    local_tz:
        Fuso horario local usado para timestamps com offset explicito.

    Returns
    -------
    pandas.DataFrame
        Copia do DataFrame com as colunas de data convertidas.
    """
    df = df.copy()

    for col in datetime_cols:
        if col in df.columns:
            df[col] = parse_mixed_datetime_series(df[col], local_tz=local_tz)

    return df
