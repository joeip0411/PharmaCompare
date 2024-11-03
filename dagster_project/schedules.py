from dagster import ScheduleDefinition, define_asset_job

cw_pricing_data_job = define_asset_job("cw_pricing_data_job", 
                                       selection=["product_prices_staging"])

cw_pricing_data_schedule = ScheduleDefinition(
    job=cw_pricing_data_job,
    cron_schedule="5 2 * * *",
    execution_timezone="Australia/Sydney",
)