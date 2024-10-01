class SilverTest:

    def __init__(self, spark, logger):
        
        self.spark = spark

        self.logger = logger

        self.customer_data_mart_schema = '''
        customer_id int
        ,first_name string 
        ,last_name string 
        ,address string
        ,pincode int
        ,phone_number int
        ,sales_date date
        ,total_cost int
        ,_corrupt_record string
        '''

        self.sales_data_mart_schema = '''
        sales_person_id int
        ,sales_person_first_name string 
        ,sales_person_last_name string 
        ,store_manager_name string
        ,manager_id double 
        ,is_manager string 
        ,sales_person_address string 
        ,sales_person_pincode int 
        ,sales_date date
        ,total_cost int 
        ,sales_month string
        ,store_id int
        ,_corrupt_record string
        '''


    def read_data_mart(self, mart_name):

        if mart_name == 'customer_data_mart':

            schema = self.customer_data_mart_schema
            path = 'C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\customer_data_mart'

        else:

            schema = self.sales_data_mart_schema
            path = 'C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\sales_team_data_mart'

        try:
            
            self.logger.info('----------Schema Validation-In Progress----------------')

            df = self.spark.read.format('parquet')\
            .option('header', 'true')\
            .schema(schema)\
            .option("columnNameOfCorruptRecord", "_corrupt_record")\
            .load(path)

        except Exception as e:
             
             self.logger.exception(e)


        try:     
            
            bad_records = df.filter(df["_corrupt_record"].isNotNull())
            # bad_records.show(truncate=False)

            assert bad_records.count() == 0

            self.logger.info(f'***********Schema Validation Successful for {mart_name}*************')
            
        except AssertionError as e:
             
             self.logger.exception(f'Bad records present in the {mart_name}, {e}')