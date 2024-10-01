from pyspark.sql import SparkSession
from Bronze_Layer.bronze_data_cleaner import Bronze
from Silver_Layer.dataframe_combiner import Combiner
import logging


def get_spark_session(logger):

    """For creating a spark session"""

    try:

        spark = SparkSession.builder\
            .appName('bronze_layer')\
            .config("spark.driver.extraClassPath", "./mysql-connector-java-8.0.26.jar")\
            .getOrCreate()
        
        logger.info('Spark session created!')
        return spark
    
    except Exception as e:
            logger.exception(e)


if __name__=='__main__':

    '''
    Setting the logger
    '''

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s -%(message)s")
    
    logger = logging.getLogger(__name__)

    spark = get_spark_session(logger)

    bronze_obj = Bronze(spark, logger)

    comb_obj = Combiner(spark, logger)

    spark.stop()