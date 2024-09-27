import logging

# levels of logger

# logging.basicConfig(level=logging.DEBUG)

# output to a file
# logging.basicConfig(level=logging.DEBUG, filename='log.log', filemode='w')

# Customize the logging message for our use
logging.basicConfig(level=logging.DEBUG, filename='log.log', filemode='w',
                    format="%(asctime)s - %(levelname)s -%(message)s")

# x = 10
# logging.debug('debug')
# logging.info(f'info variable x = {x}')
# logging.warning('warning')
# logging.error('error')
# logging.critical('critical')

# Logging exceptions

# try:
#     1/0

# except Exception as e:
#     logging.exception('sample')

logger = logging.getLogger(__name__)
logger.info('testing custom logger!')