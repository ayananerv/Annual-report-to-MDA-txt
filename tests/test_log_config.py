from pdf_reader.config import config as cg

import string
import random
from pathlib import Path
from typing import List, Tuple
import multiprocessing as mp


def test_cpu():
    assert cg.ENV["cpu"] == 16


def str_gen(length: int = 100) -> str:
    """生成指定长度的随机 alphanumeric 字符串"""
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


from pdf_reader.config.config import (
    logger_root,
    logger_cnv_fail,
    logger_xtr_fail,
)

import re


def worker_with_logger(not_used_arg: str):
    str_len = 32

    for _ in range(50):
        stream = str_gen(str_len)
        stream_len = len(stream)
        logger_root.info(f"write {stream_len} characters")

        if re.match(r"^[A-Z]", stream):
            logger_cnv_fail.warning(stream)
        if re.match(r"^[0-9]", stream):
            logger_xtr_fail.warning(stream)

    return 0


def test_logger():
    print("载入日志模块...")
    m = mp.Manager()
    log_queue = m.Queue()
    log_listener = cg.setup_listener(log_queue)
    print("日志模块已载入")

    fp = ["" for _ in range(cg.ENV["cpu"])]
    try:
        with mp.Pool(
            processes=cg.ENV["cpu"],
            initializer=cg.worker_logger_initializer,
            initargs=(log_queue,),
        ) as pool:
            pool.map(worker_with_logger, fp)
    finally:
        log_listener.stop()
        m.shutdown()
