import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def get_spark_session():

    spark = SparkSession.builder.master("local[*]") \
        .appName("connector")\
        .getOrCreate()
    
    return spark

def get_mysql_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="demodb"
    )
    return connection

class DatabaseWriter:
    def __init__(self,url,properties):
        self.url = url
        self.properties = properties

    def write_dataframe(self,df,table_name):
        try:
            print("inside write_dataframe")
            df.write.jdbc(url=self.url,
                          table=table_name,
                          mode="append",
                          properties=self.properties)
            print(f"Data successfully written into {table_name} table ")
        except Exception as e:
            return {f"Message: Error occured {e}"}
        



database_name = "demodb"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
"user": "root",
"password": "root",
"driver": "com.mysql.cj.jdbc.Driver"
}

spark = get_spark_session()

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("salary", DoubleType(), True)
])

# Manually define data
data = [
    (1, "John Doe", 28, "New York", 50000.0),
    (2, "Jane Smith", 34, "Los Angeles", 65000.0),
    (3, "Mike Johnson", 45, "Chicago", 70000.0),
    (4, "Emily Davis", 22, "Houston", 48000.0),
    (5, "David Wilson", 38, "Philadelphia", 52000.0),
    (6, "Sophia Brown", 30, "Phoenix", 55000.0),
    (7, "James Miller", 42, "San Antonio", 60000.0),
    (8, "Isabella Garcia", 27, "San Diego", 63000.0),
    (9, "Ethan Martinez", 31, "Dallas", 47000.0),
    (10, "Ava Anderson", 29, "San Jose", 58000.0)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)