from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from Spark_SQL_Connection.sql_config import DatabaseOperator
from Data_Writer.writer import Writer
from Data_Reader.reader import Reader
from S3_bucket.s3_read_write import S3

class Bronze:

    '''
    This layer is for data cleaning
    and schema validation
    '''

    def __init__(self, spark, logger):

        self.dirty_path = '''C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\dirty'''
        self.default_path = '''C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\generated_csv'''
        self.clean_path = '''C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\cleaned'''

        self.spark = spark
        self.logger = logger
        self.read = Reader(self.spark, self.logger)

        self.logger.info('Data Cleaning started!')

        self.read_dirty_csv()

        self.clean_csv()

        self.write_to_cleaned_dir()

        self.write_to_mysql()

   
    def read_dirty_csv(self):

        '''
        Reading dirty csv dims and fact
        '''
        
        try:
            
            self.dirty_customer = self.read.reader('csv', self.dirty_path + '\\dim_customer_dirty.csv')

            self.dim_product = self.read.reader('csv', self.default_path + '\\dim_product.csv')

            self.dim_sales_team = self.read.reader('csv', self.default_path + '\\dim_sales_team.csv')

            self.dim_store = self.read.reader('csv', self.default_path + '\\dim_store.csv')

            self.fact_sales = self.read.reader('csv', self.default_path + '\\fact_sales.csv')
            
            self.logger.info('Dirty CSV reading completed!')
            self.logger.info('-----------------FACT SALES---------------')
            self.logger.info(self.fact_sales.show())

        except Exception as e:

            self.logger.exception(e)
        

    
    def clean_csv(self):

        try:

            # Need to remove the records where id/first name/ last name is missing
            #  Remove if name is not a string and a number

            self.dirty_customer = (self.dirty_customer
                                .filter(F.col('customer_id').cast('int').isNotNull())
                                .filter(F.col('first_name').cast('string').isNotNull())
                                .filter(F.col('last_name').cast('string').isNotNull())
                                .filter(~F.col('first_name').rlike('[0-9]'))
                                .filter(~F.col('last_name').rlike('[0-9]'))
                                )
            
            # Need to check if both pincode and address are missing drop the row

            self.dirty_customer = (self.dirty_customer.filter(
                            ~(F.col('address').isNull() & F.col('pincode').isNull())))
            
            # Where address is numeric and pincode is null

            self.dirty_customer = self.dirty_customer.filter(
                        ~(F.col('address').rlike('^[0-9]+(\\.[0-9]+)?$') & F.col('pincode').isNull())
                    )

            # When pincode is valid and address is numeric

            self.dirty_customer = self.dirty_customer.withColumn(
                                'address',
                                F.when(
                                F.col('pincode').cast('int').isNotNull() & F.col('address').rlike('^[0-9]+(\\.[0-9]+)?$'),
                                    'sampleaddress1'
                                ).otherwise(F.col('address'))
                            )
            

            # Spliting Address with , or space( ) or -
            # Checking if the Pincode is present at the end

            self.dirty_customer = (self.dirty_customer
                                    .withColumn('address_split', F.split(F.col('address'), ',| |-'))
                                    .filter(F.col('address_split').getItem(F.size(F.col('address_split'))-1).cast('int').isNotNull())
                                )

            # check if Pincode is null or non-integer then place it from the address
            # or when pincode len is not 6 digits

            self.dirty_customer = self.dirty_customer.withColumn(
                                    'pincode',
                                    F.when(
                                        F.col('pincode').cast('int').isNull()  |  F.col('pincode').isNull() |
                                        (F.col('pincode').isNotNull() & (F.length(F.col('pincode')) != 6)),
                                        F.col('address_split').getItem(F.size(F.col('address_split'))-1)
                                    ).otherwise(F.col('pincode'))
                                )

            self.dirty_customer = self.dirty_customer.drop(self.dirty_customer.address_split)


            # Fix phone number
            self.dirty_customer = self.dirty_customer.withColumn(
                                    'phone_number',
                                    F.when(
                                        F.col('phone_number').cast('int').isNull() | 
                                        (F.col('phone_number').cast('int').isNotNull() & (F.length(F.col('phone_number'))!= 10)),
                                        '0000000000'
                                    ).otherwise(F.col('phone_number'))
                                )

            # Fix date
            self.dirty_customer = self.dirty_customer.withColumn(
                                    'customer_joining_date',
                                    F.when(
                                        F.col('customer_joining_date').cast('date').isNull(),
                                        '2021-01-01'
                                    ).otherwise(F.col('customer_joining_date'))
                                )

            # Writing a new cleaned dataframe
            self.cleaned_customer_df = self.dirty_customer.select(
                                self.dirty_customer.customer_id.cast(IntegerType()).alias('customer_id'),
                                self.dirty_customer.first_name.alias('first_name'),
                                self.dirty_customer.last_name.alias('last_name'),
                                self.dirty_customer.address.alias('address'),
                                self.dirty_customer.pincode.cast(IntegerType()).alias('pincode'),
                                self.dirty_customer.phone_number.cast(IntegerType()).alias('phone_number'),
                                self.dirty_customer.customer_joining_date.cast(DateType()).alias('customer_joining_date')
                            )

            self.logger.info('***************************************')
            self.logger.info(self.cleaned_customer_df.show())
            
        except Exception as e:

            self.logger.exception(e)


    def write_to_cleaned_dir(self):

        self.logger.info('------------Writing CSV to Clean directory--------------')
        self.data_object = [
            [self.cleaned_customer_df, 'dim_customer']
            ,[self.dim_product, 'dim_product']
            ,[self.dim_sales_team, 'dim_sales_team']
            ,[self.dim_store, 'dim_store']
            ,[self.fact_sales, 'fact_sales']
            ]

        wr = Writer(self.logger)

        for i in self.data_object:

            path = self.clean_path + '\\' + i[1]
            wr.writer(df=i[0], format='csv', mode='overwrite', path = path)
            
        self.logger.info('----------CSV Wrting Done---------------')
            


    def write_to_mysql(self):

        try:

            do = DatabaseOperator(self.logger, 'bronzedb' )

            s3 = S3(self.logger)

            data_object = [
                [self.cleaned_customer_df, 'dim_customer']
                ,[self.dim_product, 'dim_product']
                ,[self.dim_sales_team, 'dim_sales_team']
                ,[self.dim_store, 'dim_store']
                ,[self.fact_sales, 'fact_sales']
                ]
            
            self.logger.info('---------Attempting to write DatFrame on MySQL-----------')

            for i in data_object:

                do.write_dataframe(i[0], i[1])

            self.logger.info('---------Writing on MySQL completed-----------')

            self.logger.info('---------Attempting to write CSV on AWS-----------')

            for i in self.data_object:

                path = self.clean_path + '\\' + i[1] + '\\'
                s3_path = 'sales_data_processed/' + i[1] + '.csv'
                # print('S3 path: ', s3_path)
                # print('Local Path: ', path)

                s3.upload_to_s3(s3_path, path, 'csv')

            self.logger.info('---------AWS Writing Completed!-----------')

        except Exception as e:

            self.logger.exception(e)







