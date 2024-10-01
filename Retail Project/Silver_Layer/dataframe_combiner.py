from Data_Reader.reader import Reader

class Combiner:

    '''
    This class is for making 
    a single dataframe out of all
    dims and fact tables
    '''

    def __init__(self, logger, spark):
        
        '''
        Read Data from 
        CSV files
        '''
        
        self.clean_path = '''C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\cleaned'''
     
        self.spark = spark

        self.logger = logger

        self.reader = Reader(self.logger, self.spark)
        
        self.reader()


    def reader(self):

        self.dim_customer = self.reader('csv', self.clean_path + '//dim_customer')

        self.dim_product = self.reader('csv', self.clean_path + '//dim_product')

        self.dim_sales_team = self.reader('csv', self.clean_path + '//dim_sales_team')

        self.dim_store = self.reader('csv', self.clean_path + '//dim_store')

        self.fact_sales = self.reader('csv', self.clean_path + '//fact_sales')

    
    def combine(self):

        pass