class Writer:

    def __init__(self, logger):
        
        self.logger = logger


    def writer(self, df, format, mode, path):

        try:

            df.write.format(format)\
                    .option('header', 'true')\
                    .mode(mode)\
                    .option('path', path)\
                    .save()
            
            self.logger.info(f'Written {df} in {format}')

        except Exception as e:

            self.logger.exception(e)