import os

from dagster import Definitions, EnvVar
from dagster_dbt import DbtCliResource
from dagster_snowflake_pandas import SnowflakePandasIOManager

from .assets import *
from .project import dbt_project_project
from .schedules import *

sf_schema = os.getenv("SNOWFLAKE_USER") if os.getenv("DBT_ENV") == 'dev' else "PUBLIC"

defs = Definitions(
    assets=[dbt_project_dbt_assets, product_prices_staging, product_prices_db, product_description, price_temp],
    jobs=[cw_pricing_data_job],
    schedules=[cw_pricing_data_schedule],
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