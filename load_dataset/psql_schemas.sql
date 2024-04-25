DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;

CREATE TABLE gold.olist_orders_dataset (
    order_id varchar(32) NULL,
    customer_id varchar(32) NULL,
    order_status varchar(16) NULL,
    order_purchase_timestamp varchar(32) NULL,
    order_approved_at varchar(32) NULL,
    order_delivered_carrier_date varchar(32) NULL,
    order_delivered_customer_date varchar(32) NULL,
    order_estimated_delivery_date varchar(32) NULL
);

CREATE TABLE gold.olist_products_dataset (
    product_id varchar(32) NULL,
    product_category_name varchar(64) NULL,
    product_name_lenght float NULL,
    product_description_lenght float NULL,
    product_photos_qty float NULL,
    product_weight_g float NULL,
    product_length_cm float NULL,
    product_height_cm float NULL, 
    product_width_cm float NULL
);

CREATE TABLE gold.olist_order_items_dataset (
    order_id varchar(32) NULL,
    order_item_id smallint NULL,
    product_id varchar(32) NULL,
    seller_id varchar(32) NULL,
    shipping_limit_date varchar(32) NULL,
    price numeric(10,2) NULL,
    freight_value numeric(10,2) NULL,
    created_at TIMESTAMP DEFAULT NOW() NULL,
    updated_at TIMESTAMP DEFAULT NOW() NULL
);

CREATE TABLE gold.olist_order_payments_dataset (
    order_id varchar(32) NULL,
    payment_sequential int NULL,
    payment_type varchar(16) NULL,
    payment_installments int NULL,
    payment_value numeric(10,2) NULL
);
