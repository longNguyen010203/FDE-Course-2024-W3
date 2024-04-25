import pandas as pd

from dagster import multi_asset, MetadataValue
from dagster import AssetExecutionContext, Output
from dagster import AssetIn, AssetOut
from dagster import MonthlyPartitionsDefinition

from . import constants


GROUP_NAME = "mart"

@multi_asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    outs={
        "olist_orders_dataset": AssetOut(
            key_prefix=["warehouse", "gold"],
            io_manager_key="psql_io_manager",
            metadata={
                "primary_keys": [
                    "order_id"
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                ]
            },
            group_name=GROUP_NAME,
        )
    },
    name="olist_orders_dataset",
    required_resource_keys={"psql_io_manager"},
    compute_kind="postgres",
    partitions_def=MonthlyPartitionsDefinition(start_date=constants.START_DATE)
)
def olist_orders_dataset(context: AssetExecutionContext,
                         bronze_olist_orders_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    """ Load data from MinIO to PostgreSQL Database """
    
    pd_data: pd.DataFrame = bronze_olist_orders_dataset
    context.log.info("Load data to Data Warehouse PostgreSQL Success !!")
    
    return Output(
        value=pd_data,
        metadata={
            "schema": MetadataValue.text("gold"),
            "table": MetadataValue.text("olist_orders_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        }
    )
    
    
@multi_asset(
    ins={
        "bronze_olist_order_items_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    outs={
        "olist_order_items_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "gold"],
            metadata={
                "primary_keys": [
                    "order_id", 
                    "order_item_id", 
                    "product_id", 
                    "seller_id"
                ],
                "foreign_keys": [
                    "order_id",
                    "product_id"
                ],
                "columns": [
                    "order_id",
                    "order_item_id",
                    "product_id",
                    "seller_id",
                    "shipping_limit_date",
                    "price",
                    "freight_value",
                    "created_at",
                    "updated_at",
                ],
            },
            group_name=GROUP_NAME,
        )
    },
    name="olist_order_items_dataset",
    required_resource_keys={"psql_io_manager"},
    compute_kind="postgres",
)
def olist_order_items_dataset(context: AssetExecutionContext,
                         bronze_olist_order_items_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    """ Load data from MinIO to PostgreSQL Database """
    
    pd_data: pd.DataFrame = bronze_olist_order_items_dataset
    context.log.info("Load data to Data Warehouse PostgreSQL Success !!")
    
    return Output(
        value=pd_data,
        metadata={
            "schema": MetadataValue.text("gold"),
            "table": MetadataValue.text("olist_order_items_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        }
    )
    
    
@multi_asset(
    ins={
        "bronze_olist_order_payments_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    outs={
        "olist_order_payments_dataset": AssetOut(
            key_prefix=["warehouse", "gold"],
            io_manager_key="psql_io_manager",
            metadata={
                "primary_keys": [
                    "order_id", 
                    "payment_sequential"
                ],
                "columns": [
                    "order_id",
                    "payment_sequential",
                    "payment_type",
                    "payment_installments",
                    "payment_value"
                ]
            },
            group_name=GROUP_NAME,
        )
    },
    name="olist_order_payments_dataset",
    required_resource_keys={"psql_io_manager"},
    compute_kind="postgres",
)
def olist_order_payments_dataset(context: AssetExecutionContext,
                         bronze_olist_order_payments_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    """ Load data from MinIO to PostgreSQL Database """
    
    pd_data: pd.DataFrame = bronze_olist_order_payments_dataset
    context.log.info("Load data to Data Warehouse PostgreSQL Success !!")
    
    return Output(
        value=pd_data,
        metadata={
            "schema": MetadataValue.text("gold"),
            "table": MetadataValue.text("olist_order_payments_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        }
    )
    
    
@multi_asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    outs={
        "olist_products_dataset": AssetOut(
            key_prefix=["warehouse", "gold"],
            io_manager_key="psql_io_manager",
            metadata={
                "primary_keys": [
                    "product_id"
                ],
                "columns": [
                    "product_id",
                    "product_category_name",
                    "product_name_lenght",
                    "product_description_lenght",
                    "product_photos_qty",
                    "product_weight_g",
                    "product_length_cm",
                    "product_height_cm", 
                    "product_width_cm"
                ]
            },
            group_name=GROUP_NAME
        )
    },
    name="olist_products_dataset",
    required_resource_keys={"psql_io_manager"},
    compute_kind="postgres",
)
def olist_products_dataset(context: AssetExecutionContext,
                         bronze_olist_products_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    """ Load data from MinIO to PostgreSQL Database """
    
    pd_data: pd.DataFrame = bronze_olist_products_dataset
    context.log.info("Load data to Data Warehouse PostgreSQL Success !!")
    
    return Output(
        value=pd_data,
        metadata={
            "schema": MetadataValue.text("gold"),
            "table": MetadataValue.text("olist_products_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        }
    )