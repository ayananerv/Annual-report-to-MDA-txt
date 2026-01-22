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
    path: Path | None, level: int, fmt: str, filter: str
) -> logging.Handler:
    if path is None:
        handler = logging.StreamHandler()
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8", mode="w")

    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(fmt, datefmt="%m-%d %H:%M:%S"))
    handler.addFilter(logging.Filter(filter))
    return handler


def setup_root_logger(queue: Any):
    root = logging.getLogger()
    root.setLevel(LOG["level"])
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()
    if queue:
        root.addHandler(logging.handlers.QueueHandler(queue))

    # 怀疑是否是第三方库产生了大量的 WARNING，从而堵塞了进程？
    logging.getLogger("pdfminer").setLevel(logging.ERROR)
    logging.getLogger("pdfplumber").setLevel(logging.ERROR)
    logging.getLogger("chardet").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)


def setup_listener(queue: Any) -> logging.handlers.QueueListener:
    sys_logger_handler = _create_handler(
        path=PATH["logs"] / "sys.csv",
        level=logging.WARNING,
        fmt="%(asctime)s, %(processName)s, %(funcName)s, %(levelname)s, %(name)s, %(message)s",
        filter="sys",
    )

    tqdm_handler = TqdmLoggingHandler()
    tqdm_handler.setFormatter(logging.Formatter("%(message)s"))


    handlers = [sys_logger_handler, tqdm_handler]

    listener = logging.handlers.QueueListener(
        queue, *handlers, respect_handler_level=True
    )
    listener.start()
    return listener



import logging
import tqdm

class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            # 关键点：使用 tqdm.write 代替 print 或 sys.stderr.write
            # 这能保证日志打印在进度条上方，而不会破坏进度条
            tqdm.tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)
