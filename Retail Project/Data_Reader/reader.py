class Reader:

    def __init__(self, spark, logger):

        self.spark = spark

        self.logger = logger


    def reader(self, format, path):
        
        try:
            df = self.spark.read.format(format)\
                .option('header', 'true')\
                .option('inferSchema', 'true')\
                .load(path)
            
            self.logger.info(f'Read {path} in {format}')
            self.logger.info(df.show())
            return df

        except Exception as e:

            self.logger.exception(e)