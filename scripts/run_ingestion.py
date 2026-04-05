from src.config.paths import ensure_directories
from src.ingestion.load_estadia import run_estadia_ingestion


def main() -> None:
    ensure_directories()

    df_estadia = run_estadia_ingestion(
        sep=",",
        encoding="utf-8",
    )

    print("Ingestion finished successfully.")
    print(f"Rows: {len(df_estadia):,}")
    print(f"Columns: {len(df_estadia.columns)}")
    print("\nColumns found:")
    for col in df_estadia.columns:
        print(f"- {col}")


if __name__ == "__main__":
    main()