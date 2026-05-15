"""
Open-Meteo Historical Weather Ingestion
Source: https://archive-api.open-meteo.com
Coverage: Hourly weather for NYC, November 2025 → January 2026
No API key required — fully open source weather data
"""

import os
import json
import calendar
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
from ingestion.base_ingestion import BaseIngestion

load_dotenv()


class WeatherIngestion(BaseIngestion):

    def __init__(self):
        super().__init__(source_name="weather")
        self.base_url = os.getenv("WEATHER_API_BASE_URL")
        self.lat = os.getenv("WEATHER_LAT")
        self.lon = os.getenv("WEATHER_LON")
        self.timezone = os.getenv("WEATHER_TIMEZONE")

    def fetch(self, year: str, month: str) -> Path:
        """
        Fetch hourly weather for the entire month from Open-Meteo archive API.
        Builds start_date and end_date automatically from year/month.
        """
        # Calculate first and last day of the month automatically
        last_day = calendar.monthrange(int(year), int(month))[1]
        start_date = f"{year}-{month}-01"
        end_date = f"{year}-{month}-{last_day:02d}"

        logger.info(f"Fetching weather: {start_date} → {end_date}")

        params = {
            "latitude": self.lat,
            "longitude": self.lon,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": self.timezone,

            # Hourly variables — chosen specifically to enrich taxi trip analysis
            "hourly": [
                "temperature_2m",        # Air temp in Celsius at 2m height
                "precipitation",         # Rain + drizzle in mm
                "snowfall",              # Snow in cm
                "windspeed_10m",         # Wind speed at 10m height
                "weathercode",           # WMO weather interpretation code
                "visibility",            # Visibility in metres — affects driving
            ]
        }

        response = requests.get(self.base_url, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()

        # Convert to DataFrame — one row per hour
        hourly = data["hourly"]
        df = pd.DataFrame(hourly)

        # Rename time column for clarity
        df.rename(columns={"time": "datetime_hour"}, inplace=True)

        # Add location metadata
        df["latitude"] = data["latitude"]
        df["longitude"] = data["longitude"]
        df["timezone"] = data["timezone"]

        # Save as parquet — consistent format across all sources
        local_path = self.local_dir / f"weather_{year}-{month}.parquet"
        df.to_parquet(local_path, index=False)

        logger.success(f"Weather data fetched: {len(df)} hourly records")
        return local_path

    def process(self, local_path: Path, year: str, month: str) -> dict:
        """
        Validate weather data and extract stats.
        Key check: confirm we have exactly 24 * days_in_month rows.
        A partial month would silently corrupt downstream weather joins.
        """
        df = pd.read_parquet(local_path)

        expected_hours = calendar.monthrange(int(year), int(month))[1] * 24
        actual_hours = len(df)

        if actual_hours != expected_hours:
            logger.warning(
                f"Weather completeness check: expected {expected_hours} hours, "
                f"got {actual_hours}. Possible API gap."
            )
        else:
            logger.success(f"Weather completeness confirmed: {actual_hours} hourly records")

        return {
            "row_count": str(actual_hours),
            "expected_hours": str(expected_hours),
            "data_year": year,
            "data_month": month,
            "location": f"lat={self.lat},lon={self.lon}",
            "completeness_check": str(actual_hours == expected_hours)
        }


if __name__ == "__main__":
    months = [("2025", "11"), ("2025", "12"), ("2026", "01")]

    ingestion = WeatherIngestion()
    for year, month in months:
        ingestion.run(year, month)