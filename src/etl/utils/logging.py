from rich.logging import RichHandler
import logging


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level="INFO",
        format="%(message)s",
        datefmt="%H:%M:%S",
        handlers=[RichHandler(rich_tracebacks=True)],
    )
    return logging.getLogger(name)