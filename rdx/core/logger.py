import logging


class Logger:
    def __init__(self) -> None:
        self.handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s,%(msecs)d %(levelname)-8s [%(pathname)s:%(lineno)d] %(message)s"
        )
        self.handler.setFormatter(formatter)

    def setup_logger(self, name, level=logging.DEBUG):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(self.handler)
        return logger

    def get_logger(self, logger_name: str = "console_logger"):
        return self.setup_logger(name=logger_name)
