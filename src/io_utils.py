from pathlib import Path
import pandas as pd


DEFAULT_SEPARATORS = [";", ","]
DEFAULT_ENCODINGS = ["utf-8", "latin1", "cp1252"]


def list_csv_files(input_dir: Path) -> list[Path]:
    """List CSV files inside a directory."""
    return sorted(input_dir.glob("*.csv"))


def read_csv_robust(
    file_path: Path,
    separators: list[str] | None = None,
    encodings: list[str] | None = None,
) -> pd.DataFrame:
    """Read a CSV file testing multiple separators and encodings."""
    separators = separators or DEFAULT_SEPARATORS
    encodings = encodings or DEFAULT_ENCODINGS

    last_error = None

    for encoding in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(
                    file_path,
                    sep=sep,
                    encoding=encoding,
                    engine="python",
                )
                return df
            except Exception as exc:
                last_error = exc

    raise ValueError(
        f"Could not read file '{file_path.name}'. Last error: {last_error}"
    )


def load_csv_files_from_dir(
    input_dir: Path,
    add_source_file: bool = False,
) -> pd.DataFrame:
    """Load and concatenate all CSV files from a directory."""
    files = list_csv_files(input_dir)

    if not files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")

    frames = []

    for file_path in files:
        df = read_csv_robust(file_path)
        if add_source_file:
            df["source_file"] = file_path.name
        frames.append(df)

    return pd.concat(frames, ignore_index=True)