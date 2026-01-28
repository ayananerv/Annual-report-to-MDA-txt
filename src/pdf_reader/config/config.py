"""
Default Static Config Item Area
"""

from pathlib import Path


PROJECT_ROOT = (
    Path(__file__)  # 当前文件路径对象，屏蔽OS差异
    .resolve()  # 当前文件的绝对路径，需要访问磁盘
    .parents[3]  # 0=../ 1=../../
)

HOME = Path().home()


PATH = {
    # Path 对象重写（Overwrite）了 / 运算符，变成了路径连接符
    "config": PROJECT_ROOT / "config",  # config/ 目录的绝对路径
    "logs": PROJECT_ROOT / "logs",  # logs/
    "util": PROJECT_ROOT / "util",  # util/
    "fail": PROJECT_ROOT / "logs" / "fail",  # fail/
    "input_dir": HOME / "Ubuntu" / "assets" / "pdf",
    "output_dir": HOME / "Ubuntu" / "assets" / "out",
}

import multiprocessing as mp


ENV = {
    "cpu": mp.cpu_count(),
}

import logging


LOG = {
    "level": logging.WARNING,
    "logfile": "standard_process.csv",
    "read_from": "",
}


IO = {
    "buffer_size": 1024 * 4,  # 4KB
    "batch_size": 10,
    "timeout": 200,
    "search_page_range": (3, 25),  # 0-indexed, so Page 4-25
    "use_increment": True,
}


LLM = {
    "enable": False,
    "keys": [],  # store secretly
    "base_url": "https://api.siliconflow.cn/v1",
    "model": "Pro/deepseek-ai/DeepSeek-V3",
    "sys_prompt": "",
    "temperature": 0.2,
    "rpm": 20,
    "tpm": 2000000,
}


PATTERNS = {
    "start_patterns": [
        r"^第([一二三四五六七八九十0-9]{1,3})(?:章|节)\s*(?:管理层讨论与分析|董事会报告|董事会工作报告|董事局报告|经营情况讨论与分析)\s*$",
        r"^\s*([一二三四五六七八九十0-9]+)[、，：:]\s*(?:管理层讨论与分析|董事会报告|董事会工作报告|董事局报告|经营情况讨论与分析)\s*$",
    ],
    "ending_patterns": [
        r"^\s*(?:第\s*)?(?:[一二三四五六七八九十0-9]+)\s*[章节]?\s*[：:]*\s*(?:重要事项|公司治理|财务报告|企业管治报告|监事会报告|回顾)",
        r"^\s*(?:重要事项|公司治理|财务报告|企业管治报告|监事会报告)\s*$",
    ],
}

OCR = {
    "use_ocr": True,
    "ocr_output": PROJECT_ROOT,
}
