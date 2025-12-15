from loguru import logger as _logger
from config_file import MODE
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
                        format="<yellow>{time:YYYY-MM-DD HH:mm}</yellow> | {level} | {message}",
                        enqueue=True)

            _logger.add("logs/logs_{time}.log",
                        level="WARNING",
                        rotation="1 week",
                        retention="1 month",
                        compression="zip",
                        enqueue=True)
        else:
            _logger.add(sys.stdout,
                        level='DEBUG',
                        enqueue=True,
                        colorize=True,
                        format="<green>{time:HH:mm}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
                        )

        cls._is_configured = True