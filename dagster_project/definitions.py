from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import *
from .project import dbt_project_project
from .schedules import *

defs = Definitions(
    assets=[dbt_project_dbt_assets, product_prices, product_description, product_price_export],
    jobs=[cw_pricing_data_job],
    schedules=[cw_pricing_data_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project_project),
    },
)