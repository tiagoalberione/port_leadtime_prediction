import pandas as pd


def build_master_calls(df_port_call: pd.DataFrame) -> pd.DataFrame:
    """Build master table with one row per port call."""
    agg_dict = {
        "port": "first",
        "imo": "first",
        "vessel_id": "first",
        "vessel_name": "first",
        "port_call_reasons": "first",
        "arrival_port_ts": "min",
        "berthing_ts": "min",
        "unberthing_ts": "min",
        "departure_port_ts": "min",
        "source_port": "first",
        "source_port_name": "first",
        "destination_port": "first",
        "destination_port_name": "first",
    }

    master = (
        df_port_call
        .groupby("port_call_id", as_index=False)
        .agg(agg_dict)
    )

    return master