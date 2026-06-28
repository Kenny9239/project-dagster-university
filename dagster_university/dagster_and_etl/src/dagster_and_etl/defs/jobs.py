# src/dagster_and_etl/defs/jobs.py
import dagster as dg

import dagster_and_etl.defs.assets as assets

import_dynamic_partition_job = dg.define_asset_job(
    name="import_dynamic_partition_job",
    selection=[
        assets.import_dynamic_partition_file,
        assets.duckdb_dynamic_partition_table,
    ],
)

# src/dagster_and_etl/defs/jobs.py
asteroid_job = dg.define_asset_job(
    name="asteroid_job",
    selection=[
        assets.asteroids,
        assets.asteroids_file,
        assets.duckdb_table_asteroids,
    ],
)
