import logging

logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(name='global')


def set_log_level(level=logging.INFO):
    logger.setLevel(logging.INFO)

def get_logger():
    return logger

if __name__ == '__main__':
    get_logger().info('hahaha')
