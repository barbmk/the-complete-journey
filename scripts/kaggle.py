import boto3
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
import os
import logging


def main():
    """Downloads Dunnhumby's The Complete Journey datasets and uploads them to S3
    """

    datasets = ['campaign_desc', 'campaign_table', 'coupon', 'coupon_redempt', 
                'hh_demographic', 'product', 'transaction_data']

    api = KaggleApi()
    api.authenticate()
    
    config = configparser.ConfigParser()
    config.read('./credentials/dwh.cfg')

    os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['SECRET_KEY']

    directory = 'datasets'
    temp_location = '/tmp/{0}/'.format(directory)

    if not os.path.exists(temp_location):
        os.makedirs(temp_location)

    for dataset in datasets:

        api.dataset_download_file('frtgnn/dunnhumby-the-complete-journey',
                                file_name='{}.csv'.format(dataset),
                                path=temp_location)

        try:
            with zipfile.ZipFile('{}{}.csv.zip'.format(temp_location, dataset), 'r') as zip_ref:
                zip_ref.extractall('{}{}.csv'.format(temp_location, dataset))
                
            try:                
                df = spark.read.option("header",True).csv('{}{}.csv/{}.csv'.format(temp_location, dataset, dataset))
            
            except:
                df = spark.read.option("header",True).csv('{}{}.csv'.format(temp_location, dataset))

        except:
            df = spark.read.option("header",True).csv('{}{}.csv'.format(temp_location, dataset))
    
        df.write.option('header',True) \
          .mode('overwrite') \
          .csv('s3a://dunnhumby-1/stage/{}/{}.csv'.format(dataset, dataset))


if __name__ == "__main__":
    main()


