import csv
import os
import time
from io import StringIO

import boto3
from dagster import AssetExecutionContext, DailyPartitionsDefinition, Output, asset
from dagster_dbt import DbtCliResource, dbt_assets

from .project import dbt_project_project
from .util import *


@dbt_assets(manifest=dbt_project_project.manifest_path)
def dbt_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(compute_kind="python", description="All product prices found on https://www.chemistwarehouse.com.au/categories, stored in S3")
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

daily_partition_def = DailyPartitionsDefinition(start_date='2024-09-26', timezone='Australia/Sydney')

@asset(compute_kind='python', 
        description="All product prices found on https://www.chemistwarehouse.com.au/categories, stored in postgres",
        deps=[product_prices_staging],
        partitions_def=daily_partition_def)
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
    csv_file = StringIO(csv_content)
    csv_reader = csv.DictReader(csv_file)
    rows = [row for row in csv_reader]

    supabase = SUPABASE_CLIENT
    product_price_table = os.getenv('PRODUCT_PRICE_TABLE')
    res = supabase.table(product_price_table).insert(rows).execute()
    

@asset(deps=[product_prices_db], 
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




