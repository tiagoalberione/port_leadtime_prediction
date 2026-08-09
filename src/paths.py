"""Caminhos usados pelo projeto.

Este módulo concentra apenas os diretórios realmente necessários para reconstruir
a base analítica e salvar as tabelas finais do TCC. A ideia é evitar caminhos e
pastas que não façam parte do fluxo acadêmico final.
"""

from pathlib import Path


# Raiz do repositório. `src/paths.py` está dois níveis abaixo da raiz.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Dados de entrada e bases intermediárias produzidas pelo pipeline de preparação.
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

# Saídas tabulares usadas na análise e no texto do TCC.
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
TABLES_DIR = OUTPUTS_DIR / "tables"
