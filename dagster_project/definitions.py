from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import *
from .project import dbt_project_project
from .schedules import schedules

defs = Definitions(
    assets=[dbt_project_dbt_assets, product_prices, product_description],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project_project),
    },
)