class Writer:

    def __init__(self, logger):
        
        self.logger = logger


    def writer(self, df, format, mode, path):

        try:
            
            self.logger.info('--------------Writing DatFrame---------------------')

            df.write.format(format)\
                    .option('header', 'true')\
                    .mode(mode)\
                    .option('path', path)\
                    .save()
            
            self.logger.info('--------------Writing Done!---------------------')

        except Exception as e:

            self.logger.exception(e)