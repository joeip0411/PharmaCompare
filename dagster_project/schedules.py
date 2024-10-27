"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import build_schedule_from_partitioned_job, define_asset_job

cw_pricing_data_job = define_asset_job("cw_pricing_data_job", 
                                       selection=["product_prices_staging", 
                                                  "product_prices_db", 
                                                  "product_description"])

cw_pricing_data_schedule = build_schedule_from_partitioned_job(
    cw_pricing_data_job,
    hour_of_day=2,
    minute_of_hour=5,
)