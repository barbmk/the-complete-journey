# the-complete-journey

The Complete Journey is a group of datasets that collect all the purchases made during 71 weeks by 2500 households who are frequent buyers  at a retailer.

It contains the following tables:<br>
<p align="center">
  <img src="http://webanalyticsymas.com/wp-content/uploads/2021/10/tables.png" width="150">
</p>

![alt text](http://webanalyticsymas.com/wp-content/uploads/2021/10/tables.png)

The end use of the new data model is to build analytical dashboards in Tableau that will allow the business to: 
- Track the impact of having coupon discounts on their KPIs
- See overall performance of their campaigns

### Initial exploratory analysis

**Some findings**
- The average sales value per basket is much higher when the basket contains at least one discount coupon ($67 baskets with discount coupon vs $26 baskets without).
- However, the average sales value per basket is only higher for 8 departments of the 44 included in the data.  The departments are Cosmetics, Drug-GM, Nutrition, Seafood-PCKGD, meat-PCKGD, Deli, Grocery, and Floral. 
- The departments Grocery and Drug-GM account for most of the total sales. 50% and 13%, respectively. 
- Only 6% of the baskets had at least one discount coupon.

**Quality of the data**
- The coupon_redempt table only contains data about 434 households while the transaction_data table shows that many more household have used discounts (1,858 out of 2,500). For this reason I have used transactions_data and not coupon_redempt to do the analysis and create the new model.
- Campaigns only have data from day 224. However there were baskets using discount coupons from day 1. Therefore many products that show a discount coupon when they were purchased could not be matched to a campaign.
- Several campaigns run on the same days for the same households, which makes more difficult matching campaigns to the transactions data.
- The quality of the data was very high. It does not have any missing or duplicate values for key fields. 

### The new data model
The changes in the previous data model are:
- The addition of the field Campaign and the field  Basket With Campaign to the facts table (transaction_data), which facilitate the analysis of campaigns without having to join several different tables.
- The addition of the field Basket With Coupon to the facts table (transaction_data), which facilitate the segmentation of baskets with coupon discount vs baskets without.
- The suppression of the table Redeemed coupons as it is missing many of the used coupons, and also it can be calculated from the facts table when adding the field Campaign to it.
- The suppression of the table coupons as the only way to match coupon_upc to transaction_data is by joining it first to coupon_redempt, but this last table only shows data from 434 households of the 1,858 that used coupons.

### Steps of the pipeline
- I downloaded the datasets from Kaggle and uploaded them to S3. This step can be recreated with kaggle.py. Please note that it only runs in Kaggle's environment.
- Created a schema called stage and external tables using Redshift Spectrum to query the Kaggle datasets saved in S3. This step can be done by running create_tables.py
- The ETL runs in main.py. The first step is to select the start day and end day of the period that is going to be analysed. This way the tables that contain days can be filtered and we can work with smaller files, in addition to allowing daily updates of the data going forward.
- Different dataframes are created by querying the data from stage in S3. The query for transaction_data contais a WHERE statement to only fetch transactions between start_day and end_day.
- A new column containing lists with all the days between start_day and end_day per campaign is created on the table campaign_desc.
- Each value in the lists are converted into a row and the new dataframe is saved as cmpgn_desc_day.
- cmpgn_desc_day is joined to campaign_table and coupon as they contain fields that are necessary (household_key and product_id, respectivaly) to join campaigns to transaction_data.
- transaction_data is joined to the dataframe explained in the previous point by household_key, product_id, day and campaign.  As explained before, several campaigns run on the same days for the same households, which makes more difficult matching campaigns to the transactions data.
- Dropped duplicates and null values in primary key fields. As the final datasets would be saved as CSV files in a S3 bucket, there is no control of null or duplicate values.
- Wrote the new dataframes to S3 as CSV using PySpark.
- Spark was incorporated to do the data exploration and in all steps of the ETL pipeline.

The data should be updated once a day after the day ends. During a big promotion it can be updated more than once a day as the data for the specific day is overwritten in the fact table. 

### Tools:
The selection of tools used had the objective to achieve a low cost to host the data and run the pipeline.
- S3 to host the data of the Data Lake.
- Redshift Spectrum to create and query external tables that use the datasets saved in S3.

### Other scenarios

If the data was increased by 100x and needed to run on a daily basis by 7am, using EMR cluster with Airflow is the solution I would go for. 
If the database needed to be accessed by 100+ people, I would continue having the fact table in S3 and the dimension tables in Redshift, as recommended by AWS.

