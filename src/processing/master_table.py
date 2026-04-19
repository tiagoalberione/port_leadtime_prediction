import pandas as pd


def build_master_calls(df_port_call: pd.DataFrame) -> pd.DataFrame:
    """Build a master table with one row per port call."""
    required_cols = [
        "port_call_id",
        "port",
        "port_name",
        "imo",
        "vessel_id",
        "vessel_name",
        "operation_type",
        "source_port",
        "source_port_name",
        "destination_port",
        "destination_port_name",   
        "arrival_port_ts",
        "berthing_ts",
        "unberthing_ts",
        "departure_port_ts"
    ]

    available_cols = [col for col in required_cols if col in df_port_call.columns]

    df = df_port_call[available_cols].copy()

    agg_dict = {}
    text_cols = [
        "port_call_id",
        "port",
        "port_name",
        "imo",
        "vessel_id",
        "vessel_name",
        "operation_type",
        "source_port",
        "source_port_name",
        "destination_port", 
        "destination_port_name"
    ]

    for col in text_cols:
        if col in df.columns:
            agg_dict[col] = "first"

    if "arrival_port_ts" in df.columns:
        agg_dict["arrival_port_ts"] = "min"

    if "berthing_ts" in df.columns:
        agg_dict["berthing_ts"] = "min"

    if "unberthing_ts" in df.columns:
        agg_dict["unberthing_ts"] = "max"

    if "departure_port_ts" in df.columns:
        agg_dict["departure_port_ts"] = "max"

    master = (
        df.groupby("port_call_id", as_index=False)
        .agg(agg_dict)
        .sort_values(["port", "arrival_port_ts"], na_position="last")
        .reset_index(drop=True)
    )

    return master