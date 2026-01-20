"""
Docstring for pdf_reader.config.log_config

You should update log config manually
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Any

from .config import LOG, PATH

"""
You decide to create loggers and update handlers
in `setup_listener()`
"""
sys_logger = logging.getLogger("sys")
cvt_fail_logger = logging.getLogger("sys.fail.cvt")
xtr_fail_logger = logging.getLogger("sys.fail.xtr")


def _create_handler(
    path: Path | None, level: logging._Level, fmt: str, filter: str
) -> logging.Handler:
    if path is None:
        handler = logging.StreamHandler()
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8")

    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(fmt))
    handler.addFilter(logging.Filter(filter))
    return handler


def setup_root_logger(queue: Any):
    root = logging.getLogger()
    root.setLevel(LOG["level"])
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()
    root.addHandler(logging.handlers.QueueHandler(queue))
    if LOG["to_console"]:
        root.addHandler(logging.StreamHandler())


def setup_listener(queue: Any) -> logging.handlers.QueueListener:
    sys_logger_handler = _create_handler(
        path=PATH["logs"] / "sys.log",
        level=logging.DEBUG,
        fmt="%(asctime)s [%(processName)s] %(levelname)s %(name)s: %(message)s",
        filter="sys",
    )
    cvt_fail_logger_handler = _create_handler(
        path=PATH["fail"] / "fail.cvt.log",
        level=logging.WARNING,
        fmt="%(message)s",
        filter="sys.fail.cvt",
    )
    xtr_fail_logger_handler = _create_handler(
        path=PATH["fail"] / "fail.xtr.log",
        level=logging.WARNING,
        fmt="%(message)s",
        filter="sys.fail.xtr",
    )

    handlers = [sys_logger_handler, cvt_fail_logger_handler, xtr_fail_logger_handler]

    listener = logging.handlers.QueueListener(
        queue, *handlers, respect_handler_level=True
    )
    listener.start()
    return listener
