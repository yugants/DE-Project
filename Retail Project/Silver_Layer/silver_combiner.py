from Data_Reader.reader import Reader
from pyspark.sql import functions as F
from Data_Writer.writer import Writer
from Silver_Layer.silver_data_mart_test import SilverTest


class Silver:

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

        self.write = Writer(self.logger)
        
        self.data_reader()

        self.combine()

        self.create_data_mart()

        self.schema_validation()


    def data_reader(self):

        try:

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

        except Exception as e:
             
             self.logger.exception(e)

    
    def combine(self):

        '''
        This function will
        perform join on dims and facts
        tables to make a combined dataframe
        which will be used to derive the
        Customer data mart and sales team
        data mart later.
        '''

        try:

                self.cf = self.fact_sales.join(
                        self.dim_customer
                        , self.fact_sales['customer_id'] == self.dim_customer['customer_id']
                        , 'inner'
                        ).drop(self.dim_customer['customer_id'])
                
                self.logger.info('------------Cusotmer + Fact_Sales---------------')
                self.logger.info(self.cf.printSchema())
                self.logger.info(self.cf.show())
                

                self.cp = self.cf.join(
                        self.dim_product
                        , self.cf['product_name'] == self.dim_product['name']
                        , 'inner'
                        ).drop(self.dim_product['id'], self.dim_product['name']
                        , self.dim_product['current_price'], self.dim_product['old_price']
                        , self.dim_product['created_date'], self.dim_product['updated_date'])
                
                self.logger.info('------------Cusotmer + Product + Fact_Sales---------------')
                self.logger.info(self.cp.printSchema())
                self.logger.info(self.cp.show())


                
                'Renaming the address in the dim_store'

                self.dim_store = self.dim_store\
                        .withColumnRenamed('address', 'store_address')
                
                self.logger.info('----------------------------------------')
                self.logger.info(self.dim_store.printSchema())

                self.cs = self.cp.join(
                        self.dim_store.alias('ds'),
                        self.cp['store_id'] == F.col('ds.id'),
                        'inner'
                ).drop(self.dim_store["id"], self.dim_store["store_pincode"], self.dim_store["store_opening_date"], self.dim_store["reviews"])

                                

                
                self.logger.info('------------Cusotmer + Product + Fact_Sales + Store---------------')
                self.logger.info(self.cs.printSchema())
                self.logger.info(self.cs.show())

                # Rename columns in dim_sales_team to avoid conflicts
                self.dim_sales_team_renamed = self.dim_sales_team \
                .withColumnRenamed("address", "st_address") \
                .withColumnRenamed("pincode", "st_pincode") \
                .withColumnRenamed("first_name", "st_first_name") \
                .withColumnRenamed("last_name", "st_last_name")

                self.cst = self.cs.join(
                        self.dim_sales_team_renamed.alias("st"),
                        F.col("st.id") == self.cs["sales_person_id"],
                        "inner"
                ).withColumn("sales_person_first_name", F.col("st_first_name")) \
                .withColumn("sales_person_last_name", F.col("st_last_name")) \
                .withColumn("sales_person_address", F.col("st_address")) \
                .withColumn("sales_person_pincode", F.col("st_pincode")) \
                .drop("st.id", "st_first_name", "st_last_name", "st_address", "st_pincode") 

                self.logger.info('------------Customer + Product + Fact_Sales + Store + Sales_Team---------------')
                self.logger.info(self.cst.show())


                self.logger.info(self.cst.printSchema())

                self.write.writer(self.cst, 'csv', 'overwrite', 'C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\mart')

        except Exception as e:
             
             self.logger.exception(e)


    def create_data_mart(self):

        '''
        This will create the
        cutomer and sales-team
        data marts
        '''

        try:
        
                self.logger.info('-------------Creating Sales Data Mart--------------------')

                final_sales_team_data_mart_df = self.cst.select("store_id",
                "sales_person_id","sales_person_first_name","sales_person_last_name", "store_manager_name", "manager_id","is_manager",
                "sales_person_address", "sales_person_pincode"
                ,"sales_date", "total_cost",
                F.expr("SUBSTRING(sales_date,1,7) as sales_month"))

                self.logger.info('----------------Writing Sales Data Mart--------------------')

                
                final_sales_team_data_mart_df.write.format("parquet")\
                .option("header", "true")\
                .mode("overwrite")\
                .partitionBy("sales_month","store_id")\
                .option("path", 'C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\sales_team_data_mart')\
                .save()

                
                self.logger.info('-------------Creating Customer Data Mart--------------------')

                final_customer_data_mart_df = self.cst\
                .select("customer_id",
                "first_name", "last_name","address",
                "pincode", "phone_number"
                ,"sales_date", "total_cost")
                
                # final_customer_data_mart_df.write.format("parquet")\
                # .option("header", "true")\
                # .mode("overwrite")\
                # .option("path", 'C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\customer_data_mart')\
                # .save()

                self.logger.info('-------------Writing Customer Data Mart--------------------')

                self.write.writer(final_customer_data_mart_df
                                , 'parquet'
                                , 'overwrite'
                                , 'C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\customer_data_mart'
                )

        except Exception as e:
             
             self.logger.exception(e)


    def schema_validation(self):

        '''
        This is for the Schema Validation
        of the data marts
        '''
         
        ob = SilverTest(self.spark, self.logger)

        self.logger.info('--------Schema Validation for Customer Data Mart----------------')
        ob.read_data_mart('customer_data_mart')

        self.logger.info('--------Schema Validation for Sales Team Data Mart----------------')
        ob.read_data_mart('sales_team_data_mart')
