from loguru import logger as _logger
from config import MODE
import sys

class Logging_manager:
    _is_configured = False

    @classmethod
    def get_logger(cls):
        if not cls._is_configured:
            cls._configure()
        return _logger

    @classmethod
    def _configure(cls):

        _logger.remove()

        if MODE == 'prod':
            _logger.add(sys.stdout,
                        level='INFO',
                        format="<yellow>{time:YYYY-MM-DD HH:mm:ss}</yellow> | {level} | {message}",
                        enqueue=True)

            _logger.add("logs/logs_{time}.log",
                        level="WARNING",
                        rotation="1 week",
                        retention="1 month",
                        compression="zip",
                        enqueue=True)
        else:
            _logger.add(sys.stdout, level='DEBUG', enqueue=True)

        cls._is_configured = True