from Data_Reader.reader import Reader
from pyspark.sql import functions as F


class Combiner:

    '''
    This class is for making 
    a single dataframe out of all
    dims and fact tables
    '''

    def __init__(self, spark, logger):
        
        '''
        Read Data from 
        CSV files
        '''
        
        self.clean_path = '''C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\cleaned'''
     
        self.spark = spark

        self.logger = logger

        self.read = Reader(self.spark, self.logger)
        
        self.data_reader()

        self.combine()


    def data_reader(self):

        customer_path = self.clean_path + '\\dim_customer'
        self.dim_customer = self.read.reader('csv', customer_path)

        prod_path = self.clean_path + '\\dim_product'
        self.dim_product = self.read.reader('csv', prod_path)

        sales_path = self.clean_path + '\\dim_sales_team'
        self.dim_sales_team = self.read.reader('csv', sales_path)

        store_path = self.clean_path + '\\dim_store'
        self.dim_store = self.read.reader('csv', store_path)

        fact_path = self.clean_path + '\\fact_sales'

        self.fact_sales = self.read.reader('csv', fact_path)


        self.logger.info('--------Dim Customer----------')
        self.logger.info(self.dim_customer.show())

    
    def combine(self):

        '''
        This function will
        perform join on dims and facts
        tables to make a combined dataframe
        which will be used to derive the
        Customer data mart and sales team
        data mart later.
        '''

        self.cf = self.fact_sales.join(
                self.dim_customer
                , self.fact_sales['customer_id'] == self.dim_customer['customer_id']
                , 'inner'
                )
        
        self.logger.info('------------Cusotmer + Fact_Sales---------------')
        self.logger.info(self.cf.show())

        self.cp = self.cf.join(
                self.dim_product
                , self.cf['product_name'] == self.dim_product['name']
                , 'inner'
                )
        
        self.logger.info('------------Cusotmer + Product + Fact_Sales---------------')
        self.logger.info(self.cp.show())

        self.cs = self.cp.join(
                self.dim_store
                , self.cp['store_id'] == self.dim_store['id']
                , 'inner'
                )
        
        self.logger.info('------------Cusotmer + Product + Fact_Sales + Store---------------')
        self.logger.info(self.cs.show())

        self.cst = self.cs.join(
                self.dim_sales_team
                , self.cs['sales_person_id'] == self.dim_sales_team['id']
                , 'inner'
                )
        
        self.logger.info('------------Cusotmer + Product + Fact_Sales + Store + Sales_Team---------------')
        self.logger.info(self.cst.show())

