import os

from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_dbt import DbtCliResource
from dagster_snowflake_pandas import SnowflakePandasIOManager

from dagster_project import assets

from .project import dbt_project_project
from .schedules import cw_pricing_data_job, cw_pricing_data_schedule
from .sensors import cw_analytics_data_job, price_data_drop_sensor

sf_schema = os.getenv("SNOWFLAKE_USER") if os.getenv("DBT_ENV") == 'dev' else "PUBLIC"
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[cw_pricing_data_job, cw_analytics_data_job],
    schedules=[cw_pricing_data_schedule],
    sensors=[price_data_drop_sensor],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project_project),
        "sf_io_manager": SnowflakePandasIOManager(
            account=EnvVar("SNOWFLAKE_ACCOUNT"),
            user=EnvVar("SNOWFLAKE_USER"),
            password=EnvVar("SNOWFLAKE_PASSWORD"),
            database="CHEMIST_WAREHOUSE",
            role="ACCOUNTADMIN",
            warehouse="ENGINEERING_WH",
            schema=sf_schema,
        ),
    },
)
