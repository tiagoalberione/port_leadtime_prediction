from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data folders
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

# Raw datasets
RAW_ESTADIA_DIR = RAW_DIR / "estadia_embarcacao"
RAW_DUV_DIR = RAW_DIR / "duv"
RAW_AGENCIA_DIR = RAW_DIR / "agencia"
RAW_PORTO_DIR = RAW_DIR / "porto"
RAW_METEOROLOGIA_DIR = RAW_DIR / "meteorologia"

# Interim datasets
INTERIM_ESTADIA_DIR = INTERIM_DIR / "estadia_embarcacao"
INTERIM_DUV_DIR = INTERIM_DIR / "duv"
INTERIM_AGENCIA_DIR = INTERIM_DIR / "agencia"
INTERIM_PORTO_DIR = INTERIM_DIR / "porto"
INTERIM_METEOROLOGIA_DIR = INTERIM_DIR / "meteorologia"

# Processed datasets
PROCESSED_ESTADIA_DIR = PROCESSED_DIR / "estadia_embarcacao"
PROCESSED_DUV_DIR = PROCESSED_DIR / "duv"
PROCESSED_AGENCIA_DIR = PROCESSED_DIR / "agencia"
PROCESSED_PORTO_DIR = PROCESSED_DIR / "porto"
PROCESSED_METEOROLOGIA_DIR = PROCESSED_DIR / "meteorologia"


def ensure_directories() -> None:
    """Create project directories if they do not exist."""
    folders = [
        RAW_ESTADIA_DIR,
        RAW_DUV_DIR,
        RAW_AGENCIA_DIR,
        RAW_PORTO_DIR,
        RAW_METEOROLOGIA_DIR,
        INTERIM_ESTADIA_DIR,
        INTERIM_DUV_DIR,
        INTERIM_AGENCIA_DIR,
        INTERIM_PORTO_DIR,
        INTERIM_METEOROLOGIA_DIR,
        PROCESSED_ESTADIA_DIR,
        PROCESSED_DUV_DIR,
        PROCESSED_AGENCIA_DIR,
        PROCESSED_PORTO_DIR,
        PROCESSED_METEOROLOGIA_DIR
    ]

    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)
