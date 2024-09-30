import mysql.connector
from pyspark.sql import SparkSession


def get_spark_session():

    spark = SparkSession.builder\
        .appName("connector")\
        .config("spark.driver.extraClassPath", "C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\Spark\Retail Project\\Spark SQL Connection\\mysql-connector-java-8.0.26.jar")\
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
print('=====================Spark Session Created============================')


# Create DataFrame
df = spark.read.format('csv')\
        .option('header', 'true')\
        .option('inferSchema', 'true')\
        .load('./dim_product.csv')

print('======================Dataframe============================')
# Show the DataFrame
print(df.show(truncate=False))


print('writing to the SQL')

dw = DatabaseWriter(url, properties)
dw.write_dataframe(df, 'dim_customer')

print('==================SUccessfully Written============================')