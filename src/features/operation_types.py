import re
import unicodedata

import pandas as pd


# Ajuste este dicionario quando encontrar novas formas de escrever as operacoes.
# As chaves viram nomes de colunas booleanas; os valores sao aliases buscados em
# operation_type.
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
    """Normalize text so comma-separated and concatenated labels are comparable."""
    if pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def _contains_alias(text: str, alias: str) -> bool:
    if not alias:
        return False

    # "descarga" contains "carga"; "desembarque" contains "embarque".
    # These exceptions keep the individual flags from being marked by accident.
    if alias in {"carga", "embarquedepassageiros"}:
        for match in re.finditer(re.escape(alias), text):
            if text[max(0, match.start() - 3):match.start()] != "des":
                return True
        return False

    return alias in text


def create_operation_type_flags(
    df: pd.DataFrame,
    source_col: str = "operation_type",
    patterns: dict[str, tuple[str, ...]] | None = None,
    add_unmapped_col: bool = True,
) -> pd.DataFrame:
    """Create boolean indicator columns from a multi-label operation_type column."""
    if source_col not in df.columns:
        raise ValueError(f"Column '{source_col}' not found in DataFrame.")

    df = df.copy()
    patterns = patterns or OPERATION_TYPE_PATTERNS
    normalized_source = df[source_col].map(_compact_text)
    flag_cols = []

    for output_col, aliases in patterns.items():
        normalized_aliases = [_compact_text(alias) for alias in aliases]
        df[output_col] = normalized_source.map(
            lambda text: any(_contains_alias(text, alias) for alias in normalized_aliases)
        )
        flag_cols.append(output_col)

    if add_unmapped_col:
        has_source_value = df[source_col].notna() & normalized_source.ne("")
        has_any_flag = df[flag_cols].any(axis=1)
        df["op_tipo_operacao_nao_mapeado"] = has_source_value & ~has_any_flag

    return df
