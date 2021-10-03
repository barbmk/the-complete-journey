from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import os
import configparser
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2
import logging


class ProdTables():

    def __init__(self, spark, start_day, end_day):
        """
        Args:
                spark: SparkSession builder
                start_day (int): first day of the period that the data is going to be extracted for
                end_day (int): last day of the period that the data is going to be extracted for
        """

        self.start_day = start_day
        self.end_day = end_day
        self.spark = spark
    

    def fetch_all_table(self, session, table, cols):
        """
        Fetches all the data for a specific table and convert it in a PySpark dataframe
        Args:
                session: session: SQLAlchemy sessionmaker
                table (str): name of the table that the data is going to be extracted
                cols (list): list with the names of the columns used to create the dataframe
        """

        session.connection().connection.set_isolation_level(0)
        results = session.execute('SELECT * FROM stage.{}'.format(table)).fetchall()
        session.connection().connection.set_isolation_level(1)

        df_pd = pd.DataFrame(results, columns=cols)
        df_sp = self.spark.createDataFrame(df_pd)
        
        return df_sp


    def fetch_transactions(self, session):
        """
        Fetches all transactions data for the period between start_day and end_day, and converts it in a PySpark dataframe

        Args:
                session: session: SQLAlchemy sessionmaker
        """

        cols = ['household_key', 'basket_id', 'day', 'product_id', 'quantity', 'sales_value', 'store_id', 'retail_disc', 'trans_time', 'week_no', 'coupon_disc', 'coupon_match_disc']

        session.connection().connection.set_isolation_level(0)
        results = session.execute("SELECT * FROM stage.{} WHERE DAY >={} AND DAY <={}".format('transactions', str(self.start_day), str(self.end_day))).fetchall()
        session.connection().connection.set_isolation_level(1)

        df_pd = pd.DataFrame(results, columns=cols)
        df_sp = self.spark.createDataFrame(df_pd)
        
        return df_sp


    def prod_transactions(self, camp_desc_df, camp_table_df, coupon_df, trns):
        """
        Fetches all the data for a specific table and convert it in a PySpark dataframe
        Args:
                camp_desc_df (PySpark dataframe): campaigns descriptions data
                camp_table_df (PySpark dataframe): campaigns table data
                coupon_df (PySpark dataframe): coupons data
                trns (PySpark dataframe): transactions data
        """

        # Create one row per campaign's day
        camp_desc_df = camp_desc_df.withColumn('all_days', F.sequence(F.col('START_DAY').cast('int'), F.col('END_DAY').cast('int')))
        cmpgn_desc_day = camp_desc_df.select('*', F.explode('all_days').alias('DAY'))

        # Get campaigns only for the specific dates
        cmpgn_filt = cmpgn_desc_day.filter((cmpgn_desc_day['day'] >= self.start_day) & (cmpgn_desc_day['day'] <= self.end_day))

        # Join to have hh_key and product_id
        campaign_pids = cmpgn_filt.join(camp_table_df.select('household_key', 'campaign'), on='campaign', how='left')\
                                .join(coupon_df, on='campaign', how='left')

        # Split transactions in those with and without coupon discount
        trns_disc = trns.filter(trns['COUPON_DISC'] != '0')
        trns_no_disc = trns.filter(trns['COUPON_DISC'] == '0')

        # Join to add the campaigns column
        trns_disc = trns_disc.join(campaign_pids.select('household_key', 'product_id', 'day', 'campaign'), 
        on=['household_key', 'product_id', 'day'], how='left')
        
        # Create campaign column in transactions with no discount
        trns_no_disc = trns_no_disc.withColumn('campaign', F.lit(None))

        # Concat transactions with and without discount
        trns_concat = trns_disc.union(trns_no_disc)

        # Add basket with coupon discount
        bid_cm_list = trns_concat.filter(trns_concat['coupon_disc'] != 0).select('basket_id')\
                                .distinct().toPandas()['basket_id'].tolist()

        trns_concat = trns_concat.withColumn('basket_with_disc', 
                                            F.when(trns_concat['basket_id'].isin(bid_cm_list), True).otherwise(False))

        # Basket with campaign
        bid_cp_list = trns_concat.filter(~trns_concat['campaign'].isNull()).select('basket_id') \
                                .distinct().toPandas()['basket_id'].tolist()

        trns_concat = trns_concat.withColumn('basket_with_camp', 
                                            F.when(trns_concat['basket_id'].isin(bid_cp_list), True).otherwise(False))

        return trns_concat


    def write_csv(self, df, path):
        """
        Writes a PySpark's dataframe into a csv.

        Args:
                df: PySpark's dataframe
        """

        df.write.csv(path)
        
        return None


