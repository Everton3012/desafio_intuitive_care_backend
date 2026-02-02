import logging

from pathlib import Path


LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

_CONFIGURED = False

def setup_logging(log_name: str, file_name: str, level: int = logging.INFO) -> logging.Logger:
    global _CONFIGURED

    logger = logging.getLogger(log_name)
    logger.setLevel(level)

    if not _CONFIGURED:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

        file_handler = logging.FileHandler(LOG_DIR / file_name, encoding="utf-8")
        file_handler.setFormatter(fmt)
        file_handler.setLevel(level)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(fmt)
        stream_handler.setLevel(level)

        root = logging.getLogger()
        root.setLevel(level)

        if not root.handlers:
            root.addHandler(file_handler)
            root.addHandler(stream_handler)

        _CONFIGURED = True

    return logger
