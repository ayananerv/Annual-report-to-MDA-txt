from pdf_reader.config import config as cg
from pdf_reader.util import BufferedWriter as bw
import config as cgt

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


def worker(filepath: Path) -> int:
    str_len = cgt.STR_LEN
    tot_len = 0
    writer = bw.BufferedWriter(filepath)

    for _ in range(cgt.WORKER_ROUND):
        stream = str_gen(str_len)
        writer.write(stream)
        tot_len += len(stream)

    writer.close()
    return tot_len


def test_writer():
    output_file = cgt.OUTPUT_DIR / "result.txt"
    output_files = [output_file for _ in range(cg.ENV["cpu"])]

    with mp.Pool(processes=cg.ENV["cpu"]) as pool:
        results: List[int] = pool.map(worker, output_files)

    total = sum(results)
    cg.merge_shards(output_file)

    assert total == output_file.stat().st_size
