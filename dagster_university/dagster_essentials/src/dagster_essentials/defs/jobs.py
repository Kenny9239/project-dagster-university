# src/dagster_essentials/defs/jobs.py
import dagster as dg

from dagster_essentials.defs.partitions import monthly_partition, weekly_partition

trips_by_week = dg.AssetSelection.assets("trips_by_week")

trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition, # partitions added here
    selection=dg.AssetSelection.all() - trips_by_week
)

weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    partitions_def=weekly_partition,
    selection=trips_by_week,
)

rainfall_hour_report = dg.AssetSelection.assets(["rainfall_hour_report"])

daily_rainfall_update_job = dg.define_asset_job(
    name="daily_rainfall_update_job",
    selection=rainfall_hour_report,
)

