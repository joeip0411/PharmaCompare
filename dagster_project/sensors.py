from dagster import (
    AssetKey,
    AssetSelection,
    MultiAssetSensorEvaluationContext,
    RunRequest,
    define_asset_job,
    multi_asset_sensor,
)

cw_analytics_data_job = define_asset_job("cw_analytics_data_job", 
                                         selection=AssetSelection.groups('analytics'),
                                       )

@multi_asset_sensor(
        monitored_assets=[AssetKey('product_prices_db')],
        job=cw_analytics_data_job,
)
def price_data_drop_sensor(context: MultiAssetSensorEvaluationContext):
    run_requests = []
    for (
        partition,
        materializations_by_asset,
    ) in context.latest_materialization_records_by_partition_and_asset().items():
        if set(materializations_by_asset.keys()) == set(context.asset_keys):
            run_requests.append(RunRequest(partition_key=partition))
            for asset_key, materialization in materializations_by_asset.items():
                context.advance_cursor({asset_key: materialization})
    return run_requests
