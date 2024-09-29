from pyspark.sql import functions as F

class Bronze:

    '''
    This layer is for data cleaning
    and schema validation
    '''

    def __init__(self, spark, logger):

        self.dirty_path = '''C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\dirty'''
        self.clean_path = '''C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\cleaned'''
        
        self.spark = spark
        self.logger = logger
        self.logger.info('Data Cleaning started!')

        self.read_dirty_csv()

   
    def read_dirty_csv(self):

        '''
        Reading dirty csv dims and fact
        '''
        
        try:

            self.dirty_customer = self.spark.read.format('csv')\
                            .option('inferSchema', 'true')\
                            .option('header', 'true')\
                            .load(self.dirty_path + '\\dim_customer_dirty.csv')
            
            self.dirty_product = self.spark.read.format('csv')\
                            .option('inferSchema', 'true')\
                            .option('header', 'true')\
                            .load(self.dirty_path + '\\dim_product_dirty.csv')
            
            self.dirty_sales_team = self.spark.read.format('csv')\
                            .option('inferSchema', 'true')\
                            .option('header', 'true')\
                            .load(self.dirty_path + '\\dim_sales_team_dirty.csv')
            
            self.dirty_store = self.spark.read.format('csv')\
                            .option('inferSchema', 'true')\
                            .option('header', 'true')\
                            .load(self.dirty_path + '\\dim_store_dirty.csv')
            
            self.dirty_fact_sales = self.spark.read.format('csv')\
                            .option('inferSchema', 'true')\
                            .option('header', 'true')\
                            .load(self.dirty_path + '\\fact_sales_dirty.csv')
            
            self.logger.info('Dirty CSV reading completed!')

            print(self.dirty_fact_sales.show())

        except Exception as e:

            self.logger.exception(e)
        