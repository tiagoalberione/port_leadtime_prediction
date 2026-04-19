from pathlib import Path
# TODO: Criar função para log, print formatado, medir tempo

def ensure_directories(paths: list[Path]) -> None:
    """Create directories if they do not exist."""
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)

