"""
DataStructure Area
"""

from pathlib import Path
from typing import TypedDict, Tuple

PROJECT_ROOT = (
    Path(__file__)  # 当前文件路径对象，屏蔽OS差异
    .resolve()  # 当前文件的绝对路径，需要访问磁盘
    .parents[3]  # 0=../ 1=../../
)


class _PathMap(TypedDict):
    config: Path
    logs: Path
    util: Path
    fail: Path
    src: Path
    out: Path


PATH: _PathMap = {
    # Path 对象重写（Overwrite）了 / 运算符，变成了路径连接符
    "config": PROJECT_ROOT / "config",  # config/ 目录的绝对路径
    "logs": PROJECT_ROOT / "logs",  # logs/
    "util": PROJECT_ROOT / "util",  # util/
    "fail": PROJECT_ROOT / "logs" / "fail",  # fail/
    "src": PROJECT_ROOT / "assets" / "pdf",
    "out": PROJECT_ROOT / "assets" / "out",
}

import multiprocessing as mp


class _EnvMap(TypedDict):
    cpu: int


ENV: _EnvMap = {
    "cpu": 14,
}

import logging


class _LogMap(TypedDict):
    level: int
    to_console: bool


LOG: _LogMap = {
    "level": logging.WARNING,
    "to_console": False,
}


class _IoMap(TypedDict):
    buffer_size: int
    batch_size: int
    search_page_range: Tuple[int, int]


IO: _IoMap = {
    "buffer_size": 1024 * 4,  # 4KB
    "batch_size": 10,
    "search_page_range": (3, 25)  # 0-indexed, so Page 4-25
}


class _LlmMap(TypedDict):
    enable: bool
    key: str
    base_url: str
    model: str
    sys_prompt: str
    temperature: float


LLM: _LlmMap = {
    "enable": False,
    "key": "",  # store secretly
    "base_url": "https://api.siliconflow.cn/v1",
    "model": "Pro/deepseek-ai/DeepSeek-V3",
    "sys_prompt": "",  # say your prompt
    "temperature": 0.2,
}

import re


class _PatternMap(TypedDict):
    start_patterns: list[str]
    ending_patterns: list[str]


PATTERNS: _PatternMap = {
    "start_patterns": [
        r"^第([一二三四五六七八九十0-9]{1,3})(?:章|节)\s*(?:管理层讨论与分析|董事会报告|董事会工作报告|董事局报告|经营情况讨论与分析)\s*$",
        r"^\s*([一二三四五六七八九十0-9]+)[、，：:]\s*(?:管理层讨论与分析|董事会报告|董事会工作报告|董事局报告|经营情况讨论与分析)\s*$",
    ],
    "ending_patterns": [
        r"^\s*(?:第\s*)?(?:[一二三四五六七八九十0-9]+)\s*[章节]?\s*[：:]*\s*(?:重要事项|公司治理|财务报告|企业管治报告|监事会报告|回顾)",
        r"^\s*(?:重要事项|公司治理|财务报告|企业管治报告|监事会报告)\s*$",
    ],
}



"""
Function Definition Area
"""
