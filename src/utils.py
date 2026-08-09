"""Funções utilitárias pequenas e compartilhadas pelo projeto."""

from pathlib import Path


def ensure_directories(paths: list[Path]) -> None:
    """Garante que os diretórios informados existam.

    Esta função é usada pelos pipelines antes de salvar arquivos intermediários
    ou tabelas. Ela não altera arquivos existentes; apenas cria pastas ausentes.

    Parameters
    ----------
    paths:
        Lista de caminhos de diretórios que devem existir.

    Returns
    -------
    None
        Os diretórios são criados diretamente no sistema de arquivos.
    """
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
