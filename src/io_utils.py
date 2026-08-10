"""Funcoes simples de leitura dos CSVs brutos usados no TCC.

Os dados baixados para o Capitulo 3 vieram de fontes publicas com separadores e
codificacoes nem sempre uniformes. Este modulo centraliza apenas a logica minima
para encontrar arquivos CSV e tentar le-los de forma reprodutivel.
"""

from pathlib import Path

import pandas as pd


DEFAULT_SEPARATORS = [";", ","]
DEFAULT_ENCODINGS = ["utf-8", "latin1", "cp1252"]


def list_csv_files(input_dir: Path) -> list[Path]:
    """Lista os arquivos CSV de um diretorio de entrada.

    A funcao existe para tornar explicito quais arquivos brutos entram em cada
    etapa do pipeline do Capitulo 3. A ordenacao deixa a concatenacao
    reprodutivel entre execucoes.

    Parameters
    ----------
    input_dir:
        Diretorio que contem os CSVs baixados ou exportados manualmente.

    Returns
    -------
    list[pathlib.Path]
        Caminhos dos arquivos CSV encontrados, em ordem alfabetica.
    """
    return sorted(input_dir.glob("*.csv"))


def read_csv_robust(
    file_path: Path,
    separators: list[str] | None = None,
    encodings: list[str] | None = None,
    min_columns: int = 2,
) -> pd.DataFrame:
    """Le um CSV testando separadores e codificacoes comuns.

    Esta rotina existe porque os arquivos usados na base empirica podem chegar
    com `;` ou `,` e com codificacoes diferentes. Uma tentativa so e aceita
    quando produz pelo menos `min_columns` colunas, evitando tratar uma linha
    inteira como uma unica coluna por erro de separador.

    Parameters
    ----------
    file_path:
        Caminho do CSV bruto.
    separators:
        Separadores a testar. Quando omitido, usa os separadores padrao do
        projeto.
    encodings:
        Codificacoes a testar. Quando omitido, usa as codificacoes padrao do
        projeto.
    min_columns:
        Numero minimo de colunas para aceitar a leitura automaticamente.

    Returns
    -------
    pandas.DataFrame
        Conteudo do CSV lido com a primeira combinacao plausivel.

    Raises
    ------
    ValueError
        Quando nenhuma combinacao de separador/codificacao produz uma leitura
        confiavel.

    Risco metodologico
    ------------------
    Se um arquivo realmente tiver uma unica coluna valida, esta funcao rejeitara
    a leitura. No TCC, os arquivos esperados sao tabulares e possuem varias
    colunas, entao a rejeicao ajuda a detectar erro de importacao.
    """
    separators = separators or DEFAULT_SEPARATORS
    encodings = encodings or DEFAULT_ENCODINGS

    last_error = None
    best_candidate = None
    best_ncols = 0

    for encoding in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(
                    file_path,
                    sep=sep,
                    encoding=encoding,
                    engine="python",
                )

                ncols = df.shape[1]

                if ncols >= min_columns:
                    return df

                if ncols > best_ncols:
                    best_candidate = df
                    best_ncols = ncols

            except Exception as exc:
                last_error = exc

    if best_candidate is not None:
        raise ValueError(
            f"Could not confidently parse file '{file_path.name}'. "
            f"Best candidate had only {best_ncols} column(s). "
            f"Check separator/encoding manually."
        )

    raise ValueError(
        f"Could not read file '{file_path.name}'. Last error: {last_error}"
    )


def load_csv_files_from_dir(
    input_dir: Path,
    add_source_file: bool = True,
    min_columns: int = 2,
) -> pd.DataFrame:
    """Carrega e concatena todos os CSVs de um diretorio.

    A concatenacao e usada para juntar arquivos trimestrais ou exportacoes
    particionadas sem perder a rastreabilidade opcional do arquivo de origem.

    Parameters
    ----------
    input_dir:
        Diretorio com os CSVs de uma mesma fonte.
    add_source_file:
        Se verdadeiro, cria a coluna `source_file` com o nome do arquivo bruto.
    min_columns:
        Numero minimo de colunas aceito em cada CSV.

    Returns
    -------
    pandas.DataFrame
        Tabela unica formada pela concatenacao vertical dos arquivos.
    """
    files = list_csv_files(input_dir)

    if not files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")

    frames = []

    for file_path in files:
        df = read_csv_robust(
            file_path=file_path,
            min_columns=min_columns,
        )
        if add_source_file:
            df["source_file"] = file_path.name
        frames.append(df)

    return pd.concat(frames, ignore_index=True)
