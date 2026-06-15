# src/dagster_essentials/defs/jobs.py
import dagster as dg

trips_by_week = dg.AssetSelection.assets(["trips_by_week"])

weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    selection=trips_by_week,
)


rainfall_hour_report = dg.AssetSelection.assets(["rainfall_hour_report"])

daily_rainfall_update_job = dg.define_asset_job(
    name="daily_rainfall_update_job",
    selection=rainfall_hour_report,
)

