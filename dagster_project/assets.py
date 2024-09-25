import time

import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, Output, asset
from dagster_dbt import DbtCliResource, dbt_assets

from .project import dbt_project_project
from .util import *


@dbt_assets(manifest=dbt_project_project.manifest_path)
def dbt_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(compute_kind="python", description="All product prices found on https://www.chemistwarehouse.com.au/categories")
def product_prices() -> Output:

    response = ECS_CLIENT.run_task(
        cluster=ECS_CLUSTER_NAME,
        launchType='FARGATE',
        taskDefinition=ECS_TASK_DEFINITION,
        overrides={
            'containerOverrides':[PRICE_CONTAINER_OVERRIDES],
        },
        networkConfiguration=NETWORK_CONFIGURATION,
    )

    tasks = response.get('tasks', [])
    if not tasks:
        raise Exception(f"Failed to start task: {response}")

    task_arn = tasks[0]['taskArn']
    print(f"Started ECS task: {task_arn}")

    res = check_ecs_task_status(task_arn)
    return res

@asset(deps=[product_prices], 
       compute_kind="python", 
       description="All product description for product listed in the price table. Can be NULL if description is not available on chemistwarehouse.com")
def product_description() -> Output:

    response = ECS_CLIENT.run_task(
        cluster=ECS_CLUSTER_NAME,
        launchType='FARGATE',
        taskDefinition=ECS_TASK_DEFINITION,
        overrides={
            'containerOverrides':[PRODUCT_CONTAINER_OVERRIDES],
        },
        networkConfiguration=NETWORK_CONFIGURATION,
    )

    tasks = response.get('tasks', [])
    if not tasks:
        raise Exception(f"Failed to start task: {response}")

    task_arn = tasks[0]['taskArn']
    print(f"Started ECS task: {task_arn}")

    res = check_ecs_task_status(task_arn)
    return res

def check_ecs_task_status(task_arn:str):

    # Poll for the task status
    task_status = None
    wait_interval = 10  # Time (seconds) to wait between status checks

    while task_status not in ('STOPPED', 'FAILED'):
        # Describe the task to get its current status
        task_description = ECS_CLIENT.describe_tasks(
            cluster=ECS_CLUSTER_NAME,
            tasks=[task_arn],
        )

        # Get the status of the task
        task_info = task_description['tasks'][0]
        task_status = task_info['lastStatus']
        print(f"Current task status: {task_status}")

        # If task is stopped, check for failures
        if task_status == 'STOPPED':
            if task_info['containers'][0]['exitCode'] == 0:
                print(f"Task {task_arn} completed successfully.")
                return Output(value=task_arn, metadata={"taskArn": task_arn})
            else:
                raise Exception(f"Task {task_arn} failed with exit code {task_info['containers'][0]['exitCode']}.")

        # Wait for a few seconds before checking the status again
        time.sleep(wait_interval)

    raise Exception(f"Unexpected task failure for task {task_arn}.")

@asset(description="daily product price data export into S3", 
       partitions_def=DailyPartitionsDefinition(start_date='2024-09-25', 
                                                end_offset=1))
def product_price_export(context: AssetExecutionContext):

    supabase = SUPABASE_CLIENT
    time_window = context.partition_time_window
    window_start = time_window.start.strftime("%Y-%m-%d")
    window_end = time_window.end.strftime("%Y-%m-%d")

    response = supabase.from_("price_test").select("*")\
        .gte("created_at_utc", window_start).lt("created_at_utc", window_end)\
        .limit(10)\
        .execute()

    df = pd.DataFrame(response.data)

    df.to_csv("C:\\Users\\JoeIp\\Desktop\\test_df.csv", index = False)

    return Output(value=df, metadata={"rows": len(df), "output_path": "C:\\Users\\JoeIp\\Desktop\\test_df.csv"})




