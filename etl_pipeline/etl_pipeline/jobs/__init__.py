from dagster import AssetSelection, define_asset_job


raw_layer_job = define_asset_job(
    name="raw_layer_job",
    selection=AssetSelection.groups("raw")
)

mart_layer_job = define_asset_job(
    name="mart_layer_job",
    selection=AssetSelection.groups("mart")
)