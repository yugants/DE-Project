import mysql.connector
from pyspark.sql import SparkSession


def get_spark_session():

    spark = SparkSession.builder\
        .appName("connector")\
        .config("spark.driver.extraClassPath", "./mysql-connector-java-8.0.26.jar")\
        .getOrCreate()
    
    return spark


spark = get_spark_session()

df = spark.read.format('csv')\
    .option('header', 'true')\
    .option('inferSchema', 'true')\
    .load('./fact_sales.csv')

print(df.show())

print(df.printSchema())