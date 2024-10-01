class Reader:

    def __init__(self, logger, spark):

        self.spark = spark

        self.logger = logger


    def reader(self, format, path):

        df = self.spark.format(format)\
            .option('header', 'true')\
            .option('inferSchema', 'true')\
            .load(path)