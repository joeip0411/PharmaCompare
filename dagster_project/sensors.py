import os

from dagster import AssetSelection, RunRequest, SkipReason, define_asset_job, sensor
from dagster_aws.s3.sensor import get_s3_keys

cw_analytics_data_job = define_asset_job("cw_analytics_data_job", 
                                         selection=AssetSelection.groups('analytics'),
                                       )


cw_transaction_db_job = define_asset_job("cw_transaction_db_job", 
                                         selection=AssetSelection.groups('transaction'))

@sensor(jobs=[cw_transaction_db_job, cw_analytics_data_job],
        minimum_interval_seconds=60*60)
def price_datadrop_sensor(context):
    s3_bucket=os.getenv('S3_PRICE_RAW_BUCKET')
    since_key=context.cursor or None
    new_s3_keys=get_s3_keys(s3_bucket, since_key=since_key)

    if not new_s3_keys:
        return SkipReason("No new s3 files found.")
    
    last_key=new_s3_keys[-1]
    context.update_cursor(last_key)
    partition_key=last_key.split('.')[0]

    return [RunRequest(job_name='cw_transaction_db_job',run_key=last_key, partition_key=partition_key),
            RunRequest(job_name='cw_analytics_data_job',run_key=last_key, partition_key=partition_key),]