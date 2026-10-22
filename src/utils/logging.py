import logging
import os
import sys
import time
from contextlib import contextmanager

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger


@contextmanager
def step(logger: logging.Logger, name: str):
    start = time.perf_counter()
    logger.info("start | %s", name)
    try:
        yield
    except Exception:
        logger.exception("failed | %s | %.2fs", name, time.perf_counter() - start)
        raise
    logger.info("end | %s | %.2fs", name, time.perf_counter() - start)
