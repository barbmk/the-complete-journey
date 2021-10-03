from pyspark.sql import SparkSession
import logging

class DataQuality():

    def __init__(self):
        pass

    
    def drop_null(self, table, col):
        """Checks for null values in specific columns and drops the row if they are found
        """
        
        logging.info('Checking for null values in {}'.format(col))
        n_null = table.filter(table[col].isNull()).count()
        
        if n_null > 0:
            table = table.dropna(subset=col)
            logging.warning('Dropping {} rows with null values in {}'.format(str(n_null), col))
                    
        return table


    def drop_dup(self, table, col):
        """Checks for duplicate values in specific columns and drops the row if they are found
        """
            
        logging.info('Checking for dup values in {}'.format(col))
        dups = table.groupBy(col).count().where("count > 1").drop("count").collect()
        
        if len(dups) > 0:
            table = table.dropDuplicates([col])
            logging.warning('Dropping dup values {} in {}'.format(dups, col))
                    
        return table

