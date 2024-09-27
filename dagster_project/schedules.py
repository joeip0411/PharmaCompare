"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import ScheduleDefinition, build_schedule_from_partitioned_job, define_asset_job
from dagster_dbt import build_schedule_from_dbt_selection

from .assets import *

cw_pricing_data_job = define_asset_job("cw_pricing_data_job", 
                                       selection=["product_prices_staging", 
                                                  "product_prices_db", 
                                                  "product_description"])

cw_pricing_data_schedule = build_schedule_from_partitioned_job(
    cw_pricing_data_job,
    hour_of_day=2,
    minute_of_hour=5,
)

schedules = [
#     build_schedule_from_dbt_selection(
#         [dbt_project_dbt_assets],
#         job_name="materialize_dbt_models",
#         cron_schedule="0 0 * * *",
#         dbt_select="fqn:*",
#     ),
]