import logging

from rich.logging import RichHandler


def setup_logger(name: str = "batboy", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = RichHandler(markup=True, show_path=False, show_time=True)
        formatter = logging.Formatter("%(message)s", datefmt="[%X]")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)

    logger.propagate = False
    return logger
