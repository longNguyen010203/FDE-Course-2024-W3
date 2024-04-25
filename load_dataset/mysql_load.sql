LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_order_items_dataset.csv' 
INTO TABLE olist_order_items_dataset 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_order_payments_dataset.csv' 
-- INTO TABLE olist_order_payments_dataset 
-- FIELDS TERMINATED BY ',' 
-- LINES TERMINATED BY '\n' 
-- IGNORE 1 ROWS;

-- LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_orders_dataset.csv'
-- INTO TABLE olist_orders_dataset 
-- FIELDS TERMINATED BY ',' 
-- LINES TERMINATED BY '\n' 
-- IGNORE 1 ROWS;

-- LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_products_dataset.csv' 
-- INTO TABLE olist_products_dataset 
-- FIELDS TERMINATED BY ',' 
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;

-- LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/product_category_name_translation.csv' 
-- INTO TABLE product_category_name_translation 
-- FIELDS TERMINATED BY ',' 
-- LINES TERMINATED BY '\n' 
-- IGNORE 1 ROWS;
