import pdf_reader.config as cg

import string
import random
from pathlib import Path
from typing import List, Tuple
import multiprocessing as mp
import logging


def test_cpu():
    assert cg.ENV["cpu"] == 16


def str_gen(length: int = 100) -> str:
    """生成指定长度的随机 alphanumeric 字符串"""
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


from pdf_reader.config import (
    sys_logger,
    cvt_fail_logger,
    xtr_fail_logger,
)

import re


def worker_with_logger(not_used_arg: str):
    str_len = 32

    for _ in range(50):
        stream = str_gen(str_len)
        stream_len = len(stream)
        sys_logger.info(f"write {stream_len} characters")

        if re.match(r"^[A-Z]", stream):
            cvt_fail_logger.warning(stream)
        if re.match(r"^[0-9]", stream):
            xtr_fail_logger.warning(stream)

    return 0


def test_logger():
    logging.info("日志模块加载中...")
    m = mp.Manager()
    log_queue = m.Queue()
    log_listener = cg.setup_listener(log_queue)
    logging.info("日志监听器已启动，日志模块加载完成")

    fp = ["" for _ in range(cg.ENV["cpu"])]
    try:
        with mp.Pool(
            processes=cg.ENV["cpu"],
            initializer=cg.setup_root_logger,
            initargs=(log_queue,),
        ) as pool:
            pool.map(worker_with_logger, fp)
    finally:
        log_listener.stop()
        m.shutdown()
