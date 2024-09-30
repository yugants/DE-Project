from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType

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

        self.clean_csv()

   
    def read_dirty_csv(self):

        '''
        Reading dirty csv dims and fact
        '''
        
        try:

            self.dirty_customer = self.spark.read.format('csv')\
                            .option('inferSchema', 'true')\
                            .option('header', 'true')\
                            .load(self.dirty_path + '\\dim_customer_dirty.csv')
            
            # self.dirty_product = self.spark.read.format('csv')\
            #                 .option('inferSchema', 'true')\
            #                 .option('header', 'true')\
            #                 .load(self.dirty_path + '\\dim_product_dirty.csv')
            
            # self.dirty_sales_team = self.spark.read.format('csv')\
            #                 .option('inferSchema', 'true')\
            #                 .option('header', 'true')\
            #                 .load(self.dirty_path + '\\dim_sales_team_dirty.csv')
            
            # self.dirty_store = self.spark.read.format('csv')\
            #                 .option('inferSchema', 'true')\
            #                 .option('header', 'true')\
            #                 .load(self.dirty_path + '\\dim_store_dirty.csv')
            
            # self.dirty_fact_sales = self.spark.read.format('csv')\
            #                 .option('inferSchema', 'true')\
            #                 .option('header', 'true')\
            #                 .load(self.dirty_path + '\\fact_sales_dirty.csv')
            
            self.logger.info('Dirty CSV reading completed!')

            print(self.dirty_customer.show())

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

            print('***************************************')
            print(self.cleaned_customer_df.show())
            
            # self.cleaned_customer_df.write.csv('./cleaned/dim_customers.csv', header=True, mode='overwrite')

            # self.writing_data()

          

        except Exception as e:

            self.logger.exception(e)


    def writing_data(self):

        pandas_df = self.cleaned_customer_df.toPandas()

        pandas_df.to_csv('./dim_customer.csv',  index=False)