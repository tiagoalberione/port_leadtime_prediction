from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd


BRAZIL_STATE_NAMES = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapa",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceara",
    "DF": "Distrito Federal",
    "ES": "Espirito Santo",
    "GO": "Goias",
    "MA": "Maranhao",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Para",
    "PB": "Paraiba",
    "PR": "Parana",
    "PE": "Pernambuco",
    "PI": "Piaui",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondonia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "Sao Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins",
}

BRAZIL_STATE_REGIONS = {
    "AC": "Norte",
    "AL": "Nordeste",
    "AP": "Norte",
    "AM": "Norte",
    "BA": "Nordeste",
    "CE": "Nordeste",
    "DF": "Centro-Oeste",
    "ES": "Sudeste",
    "GO": "Centro-Oeste",
    "MA": "Nordeste",
    "MT": "Centro-Oeste",
    "MS": "Centro-Oeste",
    "MG": "Sudeste",
    "PA": "Norte",
    "PB": "Nordeste",
    "PR": "Sul",
    "PE": "Nordeste",
    "PI": "Nordeste",
    "RJ": "Sudeste",
    "RN": "Nordeste",
    "RS": "Sul",
    "RO": "Norte",
    "RR": "Norte",
    "SC": "Sul",
    "SP": "Sudeste",
    "SE": "Nordeste",
    "TO": "Norte",
}


class PortoForecastBuilder:
    geocoding_base_url = "https://geocoding-api.open-meteo.com/v1/search"
    forecast_base_url = "https://api.open-meteo.com/v1/forecast"

    daily_variables = [
        "temperature_2m_mean",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "rain_sum",
        "precipitation_hours",
        "wind_speed_10m_max",
        "wind_gusts_10m_max",
        "wind_direction_10m_dominant",
    ]

    def __init__(
        self,
        forecast_days_ahead: int = 15,
        country_code: str = "BR",
        timeout_seconds: int = 30,
    ) -> None:
        self.forecast_days_ahead = forecast_days_ahead
        self.country_code = country_code
        self.timeout_seconds = timeout_seconds

    def _get_json(self, base_url: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{base_url}?{urlencode(params, doseq=True)}"
        with urlopen(url, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    def _prepare_locations(self, df_locations: pd.DataFrame) -> pd.DataFrame:
        df = df_locations.copy()
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
        )

        required_columns = {"cidade", "estado"}
        missing_columns = required_columns.difference(df.columns)
        if missing_columns:
            raise ValueError(
                "Input DataFrame is missing required columns: "
                f"{sorted(missing_columns)}"
            )

        locations = df[["cidade", "estado"]].dropna().copy()
        locations["cidade"] = locations["cidade"].astype("string").str.strip()
        locations["estado"] = locations["estado"].astype("string").str.strip().str.upper()
        locations["regiao"] = locations["estado"].map(BRAZIL_STATE_REGIONS)

        return locations.drop_duplicates(ignore_index=True)

    def _geocode(self, city: str, state: str) -> tuple[float, float] | None:
        state_name = BRAZIL_STATE_NAMES.get(state, state)
        payload = self._get_json(
            self.geocoding_base_url,
            {
                "name": f"{city}, {state_name}, Brazil",
                "count": 10,
                "language": "pt",
                "countryCode": self.country_code,
                "format": "json",
            },
        )

        candidates = payload.get("results", [])
        if not candidates:
            return None

        for candidate in candidates:
            if candidate.get("admin1", "").strip().lower() == state_name.lower():
                return candidate["latitude"], candidate["longitude"]

        best = candidates[0]
        return best["latitude"], best["longitude"]

    def _fetch_forecast(self, latitude: float, longitude: float) -> pd.DataFrame:
        today = date.today()
        end_date = today + timedelta(days=self.forecast_days_ahead)

        payload = self._get_json(
            self.forecast_base_url,
            {
                "latitude": latitude,
                "longitude": longitude,
                "forecast_days": self.forecast_days_ahead + 1,
                "daily": self.daily_variables,
                "timezone": "America/Sao_Paulo",
                "wind_speed_unit": "kmh",
                "precipitation_unit": "mm",
            },
        )

        daily = payload.get("daily", {})
        if not daily or "time" not in daily:
            return pd.DataFrame()

        forecast_df = pd.DataFrame(daily)
        forecast_df["data"] = pd.to_datetime(forecast_df["time"]).dt.date
        forecast_df = forecast_df.drop(columns=["time"])
        forecast_df = forecast_df.loc[
            (forecast_df["data"] >= today) & (forecast_df["data"] <= end_date)
        ]

        return forecast_df

    def build_forecast(self, df_locations: pd.DataFrame) -> pd.DataFrame:
        locations = self._prepare_locations(df_locations)
        forecast_frames = []

        for row in locations.itertuples(index=False):
            coordinates = self._geocode(row.cidade, row.estado)
            if coordinates is None:
                continue

            latitude, longitude = coordinates
            forecast_df = self._fetch_forecast(latitude, longitude)
            if forecast_df.empty:
                continue

            forecast_df["cidade"] = row.cidade
            forecast_df["estado"] = row.estado
            forecast_df["regiao"] = row.regiao
            forecast_frames.append(
                forecast_df[
                    [
                        "cidade",
                        "estado",
                        "regiao",
                        "data",
                        "temperature_2m_mean",
                        "temperature_2m_max",
                        "temperature_2m_min",
                        "precipitation_sum",
                        "rain_sum",
                        "precipitation_hours",
                        "wind_speed_10m_max",
                        "wind_gusts_10m_max",
                        "wind_direction_10m_dominant",
                    ]
                ]
            )

        if not forecast_frames:
            return pd.DataFrame(
                columns=[
                    "cidade",
                    "estado",
                    "regiao",
                    "data",
                    "temperature_2m_mean",
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum",
                    "rain_sum",
                    "precipitation_hours",
                    "wind_speed_10m_max",
                    "wind_gusts_10m_max",
                    "wind_direction_10m_dominant",
                ]
            )

        return pd.concat(forecast_frames, ignore_index=True)
