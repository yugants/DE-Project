from Data_Reader.reader import Reader
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from Spark_SQL_Connection.sql_config import DatabaseOperator


class Gold:

    ''''
    This class is for creating use case 
    from the customer and sales team 
    data mart we have created in the bronze layer
    '''

    def __init__(self, spark, logger):
        
        self.spark = spark
        self.logger = logger
        self.read = Reader(self.spark, self.logger)
        self.customer_use_case()
        self.sales_team_use_case()
        self.write_sql()


    def customer_use_case(self):

        '''
        For customers we want to 
        identify the top customers
        with the highest monthly purchase
        '''

        try:
        
            self.customer_mart = self.read.reader('parquet', 'C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\customer_data_mart')

            self.customer_mart = self.customer_mart.withColumn(
                            'full_name', F.concat_ws(' ', F.col('first_name'), F.col('last_name'))
                            )
            
            # Define the window partition
            windowSpec = Window.partitionBy('full_name'
                                            , F.month(F.col('sales_date'))
                                            , F.year(F.col('sales_date'))
                                            )

            # Use the window to select non-aggregated columns along with your groupBy and aggregation
            self.customer_mart = self.customer_mart.withColumn('Monthly_Purchase', F.sum('total_cost').over(windowSpec)) \
                .withColumn('Month', F.month(F.col('sales_date')))\
                .withColumn('Year', F.year(F.col('sales_date')))

            # Now you can select the non-aggregated columns (like customer_id) and aggregated data
            self.final_customer_use_case = self.customer_mart.select('customer_id', 'full_name', 'Month', 'Year',\
                                'address', 'phone_number', 'Monthly_Purchase')\
                .distinct().orderBy(F.col('Monthly_Purchase').desc())
            
            self.logger.info('---------Final Customer Use Case Created!-----------')
            
        
        except Exception as e:

            self.logger.exception(e)
            

    def sales_team_use_case(self):

        '''
        For sales team we want to
        provide the incentive to 
        each member which will be 
        the 1% of their best month's
        sales amount
        '''
        try:

            self.sales_mart = self.read.reader('parquet', 'C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\\Retail Project\\Data\\actual_data\\sales_team_data_mart')

            self.sales_mart = self.sales_mart.withColumn(
                        'full_name', F.concat_ws(' ', F.col('sales_person_first_name'), F.col('sales_person_last_name'))
                        )

            windowSpec = Window.partitionBy( 'full_name', 'sales_month')

            # Use the window to select non-aggregated columns along with your groupBy and aggregation
            self.sales_mart = self.sales_mart.withColumn('monthly_sales', F.sum('total_cost').over(windowSpec))

            # Define the window to rank salespeople by their monthly sales
            rank_window = Window.partitionBy("full_name").orderBy(F.col("monthly_sales").desc())

            # Rank the sales and calculate incentives
            self.sales_mart = self.sales_mart \
                .withColumn("rnk", F.rank().over(rank_window)) \
                .withColumn("incentive", F.when(F.col("rnk") == 1, F.col("monthly_sales") * 0.01).otherwise(F.lit(0))) \
                .withColumn("incentive", F.round(F.col("incentive"), 2)) \
                .select("store_id", "sales_person_id", 'sales_person_address',
                        "full_name", "sales_month", "monthly_sales", "incentive", "rnk").distinct()

            # Show the result
            self.final_sales_usecase = self.sales_mart.where(F.col('rnk')==1).drop('rnk')

            self.logger.info('---------Final Sales Use Case Created!-----------')

        
        except Exception as e:

            self.logger.exception(e)


    def write_sql(self):
        
        try:
            
            self.logger.info('---------Attempting to write DatFrame on MySQL-----------')

            do = DatabaseOperator(self.logger, 'gold_db' )

            do.write_dataframe(self.final_customer_use_case, 'customer_monthly_purchase')

            do.write_dataframe(self.final_sales_usecase, 'sales_incentive_data')

            self.logger.info('---------Writing on MySQL completed-----------')

        except Exception as e:

            self.logger.exception(e)
