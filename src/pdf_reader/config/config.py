"""
DataStructure Area
"""

from pathlib import Path
from typing import TypedDict

_PROJECT_ROOT = (
    Path(__file__)  # 当前文件路径对象，屏蔽OS差异
    .resolve()  # 当前文件的绝对路径，需要访问磁盘
    .parents[3]  # 0=../ 1=../../
)


class _PathMap(TypedDict):
    config: Path
    logs: Path
    util: Path
    fail: Path


PATH: _PathMap = {
    # Path 对象重写（Overwrite）了 / 运算符，变成了路径连接符
    "config": _PROJECT_ROOT / "config",  # config/ 目录的绝对路径
    "logs": _PROJECT_ROOT / "logs",  # logs/
    "util": _PROJECT_ROOT / "util",  # util/
    "fail": _PROJECT_ROOT / "logs" / "fail",  # fail/
}

import multiprocessing as mp


class _EnvMap(TypedDict):
    cpu: int


ENV: _EnvMap = {
    "cpu": mp.cpu_count(),
}

import logging


class _LogMap(TypedDict):
    level: logging._Level
    to_console: bool


LOG: _LogMap = {
    "level": logging.DEBUG,
    "to_console": False,
}


class _IoMap(TypedDict):
    buffer_size: int
    batch_size: int


IO: _IoMap = {
    "buffer_size": 1024 * 4,  # 4KB
    "batch_size": 100,
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

_PATTERNS = {
    "start_patterns": [
        # 严格匹配模式 - 只使用这一个模式以确保提取精度
        r"^第[一二三四五六七八九十0-9]{1,3}(?:章|节)\s*(?:管理层讨论与分析|经营情况讨论与分析|董事会报告|董事局报告)\s*$",
        # 新增: 匹配"四、董事会报告"格式 (中文数字或阿拉伯数字+标点+关键词)
        r"^\s*[一二三四五六七八九十0-9]+[、，：:]\s*(?:管理层讨论与分析|经营情况讨论与分析|董事会报告|董事局报告)\s*$",
    ],
    "ending_patterns": [
        r"^第[四五六七八九十0-9]{1,3}(?:章|节)",
        r"^\s*(?:第\s*)?(?:[一二三四五六七八九十0-9]+)\s*[章节]?\s*[：:]*\s*(?:重要事项|公司治理|财务报告|企业管治报告|监事会报告)",
        r"^\s*(?:重要事项|公司治理|财务报告|企业管治报告|监事会报告)\s*$",
    ],
}

START_REGEX = re.compile("|".join(_PATTERNS["start_patterns"]))
END_REGEX = re.compile("|".join(_PATTERNS["ending_patterns"]))


"""
Function Definition Area
"""
