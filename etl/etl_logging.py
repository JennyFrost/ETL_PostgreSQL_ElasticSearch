import logging

# Create a custom logger
logger = logging.getLogger('etl_logger')
logging.basicConfig(level=logging.INFO)

c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('etl.log')
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.ERROR)

c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

logger.addHandler(c_handler)
logger.addHandler(f_handler)