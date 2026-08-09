"""Nomes dos arquivos produzidos durante a preparação da base analítica.

O módulo foi mantido propositalmente pequeno. Decisões de modelagem, como
features, datas de corte e modelos, ficam explícitas no notebook do Capítulo 4
para que possam ser lidas e explicadas diretamente no experimento.
"""

from src.paths import INTERIM_DIR, PROCESSED_DIR


# Etapas intermediárias da preparação dos dados de estadia portuária.
PORT_CALL_CLEAN_FILE = INTERIM_DIR / "port_call_clean.parquet"
MASTER_PORT_CALLS_FILE = INTERIM_DIR / "master_port_calls.parquet"
MASTER_PORT_CALLS_QC_FILE = INTERIM_DIR / "master_port_calls_qc.parquet"
TARGET_BASE_FILE = INTERIM_DIR / "target_base.parquet"
INTERNAL_FEATURES_FILE = INTERIM_DIR / "internal_features.parquet"

# Referências externas usadas para enriquecer a base.
PORTS_CLEAN_FILE = INTERIM_DIR / "ports_clean.parquet"
WEATHER_CLEAN_FILE = INTERIM_DIR / "weather_clean.parquet"
WEATHER_FEATURES_FILE = INTERIM_DIR / "weather_features.parquet"

# Base final utilizada pela EDA e pela modelagem do Capítulo 4.
EDA_BASE_FILE = PROCESSED_DIR / "eda_base.parquet"
