"""
Docstring for pdf_reader.config.log_config

You should update log config manually
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Tuple
import multiprocessing as mp

from .schema import JobConfig

"""
You decide to create loggers and update handlers
in `setup_listener()`
"""
sys_logger = logging.getLogger("sys")


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


def setup_root_logger(log_queue: mp.Queue):
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()
    root.addHandler(logging.handlers.QueueHandler(log_queue))

    # 怀疑是否是第三方库产生了大量的 WARNING，从而堵塞了进程？
    logging.getLogger("pdfminer").setLevel(logging.ERROR)
    logging.getLogger("pdfplumber").setLevel(logging.ERROR)
    logging.getLogger("chardet").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)


def setup_listener(
    log_path: Path, log_queue: mp.Queue
) -> logging.handlers.QueueListener:
    """
    You should defer log_listener.stop() manually in finally block
    """
    sys_logger_handler = _create_handler(
        path=log_path,
        level=logging.WARNING,
        fmt="%(asctime)s,%(process)d,%(funcName)s:%(lineno)d,%(levelname)s,%(name)s,%(message)s",
        filter="sys",
    )

    tqdm_handler = TqdmLoggingHandler()
    tqdm_handler.setFormatter(
        logging.Formatter("%(process)d>>%(levelname)s: %(message)s")
    )

    handlers = [sys_logger_handler, tqdm_handler]

    listener = logging.handlers.QueueListener(
        log_queue, *handlers, respect_handler_level=True
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


import pandas as pd
from typing import List


def read_log(log_path: Path) -> dict[int, List[str]]:
    # 1. 读取日志文件
    # 假设列之间通过逗号分隔且没有标题行
    if not log_path.exists():
        raise FileNotFoundError(log_path)
    elif log_path.stat().st_size == 0:
        return {}

    df = pd.read_csv(log_path, header=None)

    # 2. 清洗数据：以倒数第二列（索引 5）为准，保留最后一条记录
    # subset=5 表示文件名列，keep='last' 确保保留最新记录
    df_cleaned = df.drop_duplicates(subset=5, keep="last")

    # 3. 按照错误码（最后一列，索引 6）进行分类
    # 将结果转换为字典，Key 是错误码，Value 是文件名的列表
    error_map = df_cleaned.groupby(6)[5].apply(list).to_dict()

    # 现在 error_map 就是您需要的字典
    return error_map
