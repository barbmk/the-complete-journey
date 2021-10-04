# CREATE EXTERNAL SCHEMA

create_schema_q="""
CREATE EXTERNAL SCHEMA STAGE
FROM DATA CATALOG DATABASE '{}'
iam_role '{}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
"""


# DROP EXTERNAL TABLES

drop_transactions= 'DROP TABLE stage.transactions'
drop_campaign_desc='DROP TABLE stage.campaign_desc'
drop_campaign_table='DROP TABLE stage.campaign_table'
drop_coupon='DROP TABLE stage.coupon'
drop_hh_demographic='DROP TABLE stage.hh_demographic'
drop_product='DROP TABLE stage.product'


# CREATE EXTERNAL TABLES

create_transactions= """
CREATE EXTERNAL TABLE stage.transactions (
    "household_key" character varying(45),
    "basket_id" character varying(45),
    "day" int,
    "product_id" character varying(45),
    "quantity" int,
    "sales_value" double precision,
    "store_id" character varying(45),
    "retail_disc" double precision,
    "trans_time" int,
    "week_no" int,
    "coupon_disc" double precision,
    "coupon_match_disc" double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 's3://dunnhumby-1/stage/transaction_data/transaction_data.csv' 
TABLE PROPERTIES ('skip.header.line.count'='1');
"""

create_campaign_desc="""
CREATE EXTERNAL TABLE stage.campaign_desc(
    campaign character varying(45), 
    description character varying(45), 
    start_day int, 
    end_day int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 's3://dunnhumby-1/stage/campaign_desc/campaign_desc.csv' 
TABLE PROPERTIES ('skip.header.line.count'='1');
"""

create_campaign_table="""
CREATE EXTERNAL TABLE stage.campaign_table(
    household_key character varying(45), 
    campaign character varying(45), 
    description character varying(45)
    )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 's3://dunnhumby-1/stage/campaign_table/campaign_table.csv' 
TABLE PROPERTIES ('skip.header.line.count'='1');
"""

create_coupon="""
CREATE EXTERNAL TABLE stage.coupon(
    coupon_upc character varying(45), 
    product_id character varying(45), 
    campaign character varying(45)
    )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 's3://dunnhumby-1/stage/coupon/coupon.csv' 
TABLE PROPERTIES ('skip.header.line.count'='1');
"""

create_hh_demographic="""
CREATE EXTERNAL TABLE stage.hh_demographic(
    age_desc character varying(45), 
    marital_status_code character varying(45), 
    income_desc character varying(45), 
    homeowner_desc character varying(45), 
    hh_comp_desc character varying(45), 
    household_size_desc int, 
    kid_category_desc character varying(45), 
    household_key character varying(45)
    )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 's3://dunnhumby-1/stage/hh_demographic/hh_demographic.csv' 
TABLE PROPERTIES ('skip.header.line.count'='1');
"""

create_product="""
CREATE EXTERNAL TABLE stage.product(
    product_id character varying(45), 
    manufacturer character varying(45), 
    department character varying(45), 
    brand character varying(45), 
    commodity_desc character varying(45), 
    sub_commodity_desc character varying(45), 
    curr_size_of_product character varying(45)
    )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 's3://dunnhumby-1/stage/product/product.csv' 
TABLE PROPERTIES ('skip.header.line.count'='1');
"""


# DROP SCHEMA
drop_schema_q = 'DROP SCHEMA stage'


# QUERY LISTS

create_schema_query = [create_schema_q]
drop_schm_query = [drop_schema_q]
drop_table_queries = [drop_transactions, drop_campaign_desc, drop_campaign_table, drop_coupon, drop_hh_demographic, drop_product]
create_table_queries = [create_transactions, create_campaign_desc, create_campaign_table, create_coupon, create_hh_demographic, create_product]


