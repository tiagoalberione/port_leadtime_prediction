"""Criação de flags para os tipos de operação da escala portuária."""

import re
import unicodedata

import pandas as pd


# As chaves viram nomes de colunas booleanas. Os aliases preservam distinções
# analiticamente importantes, como Carga vs. Descarga e Embarque vs. Desembarque.
OPERATION_TYPE_PATTERNS = {
    "op_abastecimento_bunker": (
        "Abastecimento (Bunker)",
        "Bunker",
    ),
    "op_arribada": (
        "Arribada",
    ),
    "op_carga": (
        "Carga",
    ),
    "op_descarga": (
        "Descarga",
    ),
    "op_desembarque_passageiros": (
        "Desembarque de Passageiros",
        "Desembarque/Embarque de Passageiros",
    ),
    "op_embarque_passageiros": (
        "Embarque de Passageiros",
        "Desembarque/Embarque de Passageiros",
    ),
    "op_fundeio": (
        "Fundeio",
    ),
    "op_fundeio_ship_to_ship": (
        "Fundeio ship to ship",
        "Ship to ship",
    ),
    "op_navio_estado_com_operacao_comercial": (
        "Navio de Estado - com operação comercial",
    ),
    "op_navio_estado_marinha_brasil": (
        "Navio de Estado - Marinha do Brasil",
    ),
    "op_navio_estado_sem_operacao_comercial": (
        "Navio de Estado - sem operação comercial",
    ),
    "op_offshore": (
        "Off-shore",
        "Offshore",
    ),
    "op_reparo_manutencao": (
        "Reparo/Manutenção",
        "Reparo",
        "Manutenção",
    ),
    "op_retirada_residuos_com_operacao_comercial": (
        "Retirada de Resíduos - com operação comercial",
    ),
    "op_retirada_residuos_sem_operacao_comercial": (
        "Retirada de Resíduos - sem operação comercial",
    ),
    "op_solicitacao_certificado": (
        "Solicitação de certificado",
        "Certificado",
    ),
}


def _compact_text(value: object) -> str:
    """Normaliza texto para comparar rótulos separados ou concatenados.

    A fonte pode escrever combinações de operação com vírgulas ou tudo colado,
    por exemplo `Carga,Fundeio` e `CargaFundeio`. A normalização remove acentos,
    pontuação e espaços antes da busca por aliases.
    """
    if pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def _contains_alias(text: str, alias: str) -> bool:
    """Verifica um alias evitando falsos positivos conhecidos.

    `descarga` contém `carga`, e `desembarque` contém `embarque`. Essas exceções
    mantêm as flags separadas para análises do TCC.
    """
    if not alias:
        return False

    if alias in {"carga", "embarquedepassageiros"}:
        for match in re.finditer(re.escape(alias), text):
            if text[max(0, match.start() - 3):match.start()] != "des":
                return True
        return False

    return alias in text


def create_operation_type_flags(
    df: pd.DataFrame,
    source_col: str = "operation_type",
) -> pd.DataFrame:
    """Cria indicadores booleanos a partir de `operation_type`.

    O campo original é multirrótulo: uma mesma escala pode envolver carga,
    descarga, bunker, fundeio etc. As flags tornam essas combinações legíveis na
    EDA e podem ser avaliadas no Capítulo 4 porque o motivo declarado da escala é
    informação operacional disponível no registro de chegada.

    Parameters
    ----------
    df:
        Base com a coluna de tipos de operação.
    source_col:
        Nome da coluna textual de origem. No pipeline oficial é `operation_type`.

    Returns
    -------
    pandas.DataFrame
        Cópia com colunas `op_*` e `op_tipo_operacao_nao_mapeado`.

    Risco metodológico
    ------------------
    A master table usa o primeiro `operation_type` quando há mais de uma linha
    bruta para o mesmo `port_call_id`. A auditoria do segundo bloco mostrou que
    apenas 1 dos 79 conflitos brutos mudaria as flags após esta normalização;
    por isso a regra foi preservada neste refactor comportamental.
    """
    if source_col not in df.columns:
        raise ValueError(f"Column '{source_col}' not found in DataFrame.")

    df = df.copy()
    normalized_source = df[source_col].map(_compact_text)
    flag_cols = []

    for output_col, aliases in OPERATION_TYPE_PATTERNS.items():
        normalized_aliases = [_compact_text(alias) for alias in aliases]
        df[output_col] = normalized_source.map(
            lambda text: any(_contains_alias(text, alias) for alias in normalized_aliases)
        )
        flag_cols.append(output_col)

    has_source_value = df[source_col].notna() & normalized_source.ne("")
    has_any_flag = df[flag_cols].any(axis=1)
    df["op_tipo_operacao_nao_mapeado"] = has_source_value & ~has_any_flag

    return df