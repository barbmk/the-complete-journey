from pyspark.sql import SparkSession
import logging
from string import Template
import os
import datetime
import configparser
import prod_tables
import data_quality


def main(data, context, start_day, end_day):
    """Function that extracts data from S3 by creating external tables using Redshift Spectrum, transforms it and loads it to S3.
    """

    config = configparser.ConfigParser()
    config.read('./credentials/dwh.cfg')
    
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET_KEY']
    
    DB_USER = config['DB']['DB_USER']
    DB_PASSWORD = config['DB']['DB_PASSWORD']
    ENDPOINT = config['DB']['HOST']
    DB_PORT = config['DB']['DB_PORT']
    DB_NAME = config['DB']['DB_NAME']

    engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASSWORD, 
                                                                         ENDPOINT, DB_PORT, DB_NAME))
    session = sessionmaker(bind=engine)()
    

    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"). \
    enableHiveSupport().getOrCreate()

    prodObj = prod_tables.ProdTables(spark, start_day, end_day)
    qualObj = data_quality.DataQuality()
    
    # Get campaigns descriptions
    camp_desc_cols = ['description', 'campaign', 'start_day', 'end_day']
    camp_desc_df = prodObj.fetch_all_table(session, 'campaign_desc', camp_desc_cols)

    # Get campaigns table
    camp_table_cols = ['description', 'household_key', 'campaign']
    camp_table_df = prodObj.fetch_all_table(session, 'campaign_table', camp_table_cols)

    # Get coupons
    coupon_cols = ['coupon_upc', 'product_id', 'campaign']
    coupon_df = prodObj.fetch_all_table(session, 'coupon', coupon_cols)

    # Get household demographics data
    hh_cols = ['age_desc', 'marital_status_code', 'income_desc', 'homeowner_desc', 'hh_comp_desc', 'household_size_desc', 'kid_category_desc', 'household_key']
    hh_df = prodObj.fetch_all_table(session, 'hh_demographic', hh_cols)

    # Get products data
    product_cols = ['product_id', 'manufacturer', 'department', 'brand', 'commodity_desc', 'sub_commodity_desc', 'curr_size_of_product']
    product_df = prodObj.fetch_all_table(session, 'product', product_cols)

    # Get transactions
    trns = prodObj.fetch_transactions(session)

    # Add campaigns to transactions
    trns_prod = prodObj.prod_transactions(camp_desc_df, camp_table_df, coupon_df, trns)

    # Data quality check 
    camp_desc_df = qualObj.drop_null(camp_desc_df, 'campaign')
    camp_desc_df = qualObj.drop_dup(camp_desc_df, 'campaign')

    camp_table_df = qualObj.drop_null(camp_table_df, 'campaign')

    hh_df = qualObj.drop_null(hh_df, 'household_key')
    hh_df = qualObj.drop_dup(hh_df, 'household_key')

    product_df = qualObj.drop_null(product_df, 'product_id')
    product_df = qualObj.drop_dup(product_df, 'product_id')

    trns_prod = qualObj.drop_null(trns_prod, 'day')

    
    # Write CSV to S3
    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template('Starting to write CSV on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            camp_desc_path = 's3a://dunnhumby-1/prod/campaign_desc/campaign_desc.csv'
            prodObj.write_csv(camp_desc_df, camp_desc_path)
        except Exception as error:
            log_message = Template('Query failed due to $message.')
            logging.error(log_message.safe_substitute(message=error))

        try:
            camp_table_path = 's3a://dunnhumby-1/prod/campaign_table/campaign_table.csv'
            prodObj.write_csv(camp_table_df, camp_table_path)
        except Exception as error:
            log_message = Template('Query failed due to $message.')
            logging.error(log_message.safe_substitute(message=error))

        try:
            hh_path = 's3a://dunnhumby-1/prod/hh_demographic/hh_demographic.csv'
            prodObj.write_csv(hh_df, hh_path)
        except Exception as error:
            log_message = Template('Query failed due to $message.')
            logging.error(log_message.safe_substitute(message=error))

        try:
            product_path = 's3a://dunnhumby-1/prod/product/product.csv'
            prodObj.write_csv(product_df, product_path)
        except Exception as error:
            log_message = Template('Query failed due to $message.')
            logging.error(log_message.safe_substitute(message=error))

        try:
            trns_path = 's3a://dunnhumby-1/prod/transactions/day{}-day{}_transactions.csv'.format(str(start_day), str(end_day))
            prodObj.write_csv(trns_prod, trns_path)
        except Exception as error:
            log_message = Template('Query failed due to $message.')
            logging.error(log_message.safe_substitute(message=error))


    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)

if __name__ == '__main__':
    main('data', 'context', start_day, end_day)

    