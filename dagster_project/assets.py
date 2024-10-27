import json
import os
import subprocess
import time
from io import StringIO
from pathlib import Path

import boto3
import pandas as pd
import psycopg2
from dagster import (
    AssetDep,
    AssetExecutionContext,
    AutomationCondition,
    DailyPartitionsDefinition,
    Output,
    TimeWindowPartitionMapping,
    asset,
)
from dagster_dbt import DbtCliResource, dbt_assets
from psycopg2.extras import execute_batch

from .project import dbt_project_project
from .util import (
    ECS_CLIENT,
    ECS_CLUSTER_NAME,
    ECS_TASK_DEFINITION,
    NETWORK_CONFIGURATION,
    PRICE_CONTAINER_OVERRIDES,
    PRODUCT_CONTAINER_OVERRIDES,
    SUPABASE_CLIENT,
)

daily_partition_def = DailyPartitionsDefinition(start_date='2024-09-26', timezone='Australia/Sydney')
depends_on_past = TimeWindowPartitionMapping(start_offset=-1, end_offset=-1)

# @dbt_assets(manifest=dbt_project_project.manifest_path, partitions_def=daily_partition_def)
# def dbt_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
#     time_window = context.partition_time_window
#     dbt_vars = {
#         "start_date": time_window.start.isoformat(),
#         "end_date": time_window.end.isoformat(),
#     }
#     dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]

#     yield from dbt.cli(dbt_build_args, context=context).stream()

@asset(compute_kind="s3", 
       description="All product prices found on https://www.chemistwarehouse.com.au/categories, stored in S3",
       group_name='sourcing')
def product_prices_staging() -> Output:

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


@asset(compute_kind='postgres', 
        description="All product prices found on https://www.chemistwarehouse.com.au/categories, stored in postgres",
        deps=[product_prices_staging],
        partitions_def=daily_partition_def,
        group_name='transaction')
def product_prices_db(context: AssetExecutionContext):
    file = context.partition_key + '.csv'

    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name = 'ap-southeast-2',
    )

    response = s3_client.get_object(Bucket=os.getenv('S3_PRICE_RAW_BUCKET'), Key=file)
    csv_content = response['Body'].read().decode('utf-8')
    csv_data = StringIO(csv_content)
    df = pd.read_csv(csv_data)
    
    conn = psycopg2.connect(dbname='postgres',
                            host=os.getenv('PG_HOST'),
                            user=os.getenv('PG_USER'),
                            password=os.getenv('PG_PASSWORD'),
                            port=5432)
    
    cur = conn.cursor()
    
    product_price_table = os.getenv('PRODUCT_PRICE_TABLE')

    upsert_sql = f"""
    INSERT INTO {product_price_table} ({', '.join(df.columns)})
    VALUES (%s, %s, %s)
    ON CONFLICT (data_id, created_at_utc) DO NOTHING;
    """
    vals = [tuple(row) for row in df.to_numpy()]

    execute_batch(cur, upsert_sql, vals)
    conn.commit()
    

@asset(deps=[product_prices_db], 
       compute_kind="postgres", 
       description="All product description for product listed in the price table. Can be NULL if description is not available on chemistwarehouse.com",
       group_name='transaction')
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

@asset(compute_kind='snowflake',
        metadata = {"partition_expr": "created_at_utc"},
        io_manager_key='sf_io_manager',
        partitions_def=daily_partition_def,
        description='Raw historical product prices',
        group_name='analytics',
        )
def bronze_price(context: AssetExecutionContext) -> pd.DataFrame:
    file = context.partition_key + '.csv'

    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name = 'ap-southeast-2',
    )

    response = s3_client.get_object(Bucket=os.getenv('S3_PRICE_RAW_BUCKET'), Key=file)

    price_raw = pd.read_csv(response['Body'])

    return price_raw



def run_dbt_command(dbt_cmd:list, context:AssetExecutionContext):
    dbt_project_dir=Path(__file__).joinpath("..", "..", "dbt_project").resolve()
    
    result = subprocess.run(
        dbt_cmd,
        cwd=dbt_project_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception

@asset(compute_kind='dbt', 
        description="conformed price data",
        deps=[bronze_price],
        group_name='analytics')
def silver_price(context: AssetExecutionContext):

    dbt_deps = ["dbt", "deps"]
    run_dbt_command(dbt_cmd=dbt_deps, context=context)

    dbt_build_args = ["dbt", "run", "--select", "silver_price"]
    run_dbt_command(dbt_cmd=dbt_build_args, context=context)

@asset(compute_kind='dbt', 
        description="cumulative price data",
        deps=[silver_price, AssetDep('cumulative_price', partition_mapping=depends_on_past)],
        group_name='analytics',
        partitions_def=daily_partition_def,
        automation_condition=AutomationCondition.eager())
def cumulative_price(context: AssetExecutionContext):

    dbt_deps = ["dbt", "deps"]
    run_dbt_command(dbt_cmd=dbt_deps, context=context)

    time_window = context.partition_time_window
    dbt_vars = {
        "start_date": time_window.start.isoformat(),
        "end_date": time_window.end.isoformat(),
    }

    dbt_build_args = ["dbt", "build", "--select", "cumulative_price", "--vars", json.dumps(dbt_vars)]

    run_dbt_command(dbt_cmd=dbt_build_args, context=context)