class DatabaseOperator:

    '''
    Class is used for reading & 
    writing a dataframe
    into the MySQL
    '''

    def __init__(self
                 ,logger
                 ,database_name):

        self.url = f"jdbc:mysql://localhost:3306/{database_name}"
        self.properties = {
                        "user": "root",
                        "password": "root",
                        "driver": "com.mysql.cj.jdbc.Driver"
                        }
        self.logger = logger
        self.logger.info('-------------Initiaiized the connection string----------------')

    
    def write_dataframe(self,df,table_name):
        try:
            self.logger.info("inside write_dataframe")
            df.write.jdbc(url=self.url,
                          table=table_name,
                          mode="append",
                          properties=self.properties)
            
            self.logger.info(f"Data successfully written into {table_name} table ")
        
        except Exception as e:
            self.logger.exception(f"Message: Error occured {e}")


    def read_dataframe(self, spark, table_name):
        try:
            self.logger.info("---------------Reading_Dataframe--------------------------")
            df = spark.read.jdbc(url=self.url,
                          table=table_name,
                          properties=self.properties)
            
            self.logger.info(f"Data successfully read from {table_name} table ")
            return df
        
        except Exception as e:

            self.logger.exception(f"Message: Error occured {e}")