import dagster as dg

from dagster_and_etl.defs.jobs import asteroid_job
# src/dagster_and_etl/defs/schedules.py

@dg.schedule(job=asteroid_job, cron_schedule="0 6 * * *")
def date_range_schedule(context):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")

    return dg.RunRequest(
        run_config={
            "ops": {
                "asteroids": {
                    "config": {
                        "date": scheduled_date,
                    },
                },
            },
        },
    )
