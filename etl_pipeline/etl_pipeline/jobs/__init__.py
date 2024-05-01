from dagster import AssetSelection, define_asset_job

from ..partitions import monthly_partitions


raw_layer_job = define_asset_job(
    name="raw_layer_job",
    partitions_def=monthly_partitions,
    selection=AssetSelection.groups("raw")
)

mart_layer_job = define_asset_job(
    name="mart_layer_job",
    partitions_def=monthly_partitions,
    selection=AssetSelection.groups("mart")
)