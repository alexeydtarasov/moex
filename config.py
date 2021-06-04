import logging

LOG_LEVEL = logging.DEBUG

logging.basicConfig(format="[%(levelname)s] %(asctime)s | %(message)s", datefmt='%Y-%m-%d %H:%M:%S', level=LOG_LEVEL)
logger = logging.getLogger()

MOEX_TOKEN_PATH = '/home/denis/PycharmProjects/common_data/moex_token.txt'