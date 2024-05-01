import pandas as pd

from dagster import asset, Output, MetadataValue
from dagster import AssetExecutionContext

from ..partitions import monthly_partitions


GROUP_NAME = "raw"

@asset(
    name="bronze_olist_orders_dataset",
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "ecom"],
    compute_kind="SQL",
    partitions_def=monthly_partitions,
    group_name=GROUP_NAME
)
def bronze_olist_orders_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """
        Load table 'olist_orders_dataset'
        from MySQL database as pandas DataFrame and save to MinIO
    """
    sql_stm = "SELECT * FROM olist_orders_dataset"
    
    try:
        partition_date_str = context.asset_partition_key_for_output()
        partition_by = "order_purchase_timestamp"
        sql_stm += f" WHERE CAST({partition_by} AS DATE) = '{partition_date_str}'"
        context.log.info(f"Partition by {partition_by} = {partition_date_str}")
    except Exception:
        context.log.info(f"olist_orders_dataset has no partition key!")
    
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    context.log.info(f"Extract table 'olist_orders_dataset' from MySQL Success")

    return Output(
        value=pd_data,
        metadata={
            "table": MetadataValue.text("olist_orders_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        },
    )


@asset(
    name="bronze_olist_order_items_dataset",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="SQL",
    group_name=GROUP_NAME
)
def bronze_olist_order_items_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """
        Load table 'olist_order_items_dataset'
        from MySQL database as pandas DataFrame and save to MinIO
    """
    sql_stm = "SELECT * FROM olist_order_items_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    context.log.info(f"Extract table 'olist_order_items_dataset' from MySQL Success")
    
    return Output(
        value=pd_data,
        metadata={
            "table": MetadataValue.text("olist_order_items_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        }
    )
    
    
@asset(
    name="bronze_olist_order_payments_dataset",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="SQL",
    group_name=GROUP_NAME
)
def bronze_olist_order_payments_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """
        Load table 'olist_order_payments_dataset'
        from MySQL database as pandas DataFrame and save to MinIO
    """
    sql_stm = "SELECT * FROM olist_order_payments_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    context.log.info(f"Extract table 'olist_order_payments_dataset' from MySQL Success")

    return Output(
        value=pd_data,
        metadata={
            "table": MetadataValue.text("olist_order_payments_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        }
    )
    
    
@asset(
    name="bronze_olist_products_dataset",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="SQL",
    group_name=GROUP_NAME
)
def bronze_olist_products_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """
        Load table 'olist_products_dataset'
        from MySQL database as pandas DataFrame and save to MinIO
    """
    sql_stm = "SELECT * FROM olist_products_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    context.log.info(f"Extract table 'olist_products_dataset' from MySQL Success")

    return Output(
        value=pd_data,
        metadata={
            "table": MetadataValue.text("olist_products_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        }
    )