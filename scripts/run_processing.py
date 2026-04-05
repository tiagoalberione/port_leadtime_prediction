from src.processing.process_estadia import run_estadia_processing


def main() -> None:
    df_estadia = run_estadia_processing()

    print("Processing finished successfully.")
    print(f"Rows: {len(df_estadia):,}")
    print(f"Columns: {len(df_estadia.columns)}")
    print("\nSample:")
    print(df_estadia.head())


if __name__ == "__main__":
    main()