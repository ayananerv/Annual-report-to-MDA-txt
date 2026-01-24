"""
Dynamic Injection Config Area
"""

from dataclasses import dataclass, fields, asdict
from pathlib import Path
from typing import Tuple, Optional, Dict, Any, List

from . import config as cg


@dataclass(frozen=True)
class JobConfig:
    # PATH
    input_dir: Path
    output_dir: Path
    logs: Path
    config: Path

    # ENV
    cpu: int

    # LOG
    log_level: int
    logfile: str
    # read_from: str

    # IO
    buffer_size: int
    batch_size: int
    timeout: int
    search_page_range: Tuple[int, int]
    use_increment: bool

    # LLM
    enable: bool
    keys: List[str]
    base_url: str
    model: str
    sys_prompt: str
    temperature: float
    rpm: int
    tpm: int

    # PATTERNS
    start_patterns: List[str]
    ending_patterns: List[str]

    @classmethod
    def from_defaults(cls, overrides: Optional[Dict[str, Any]] = None) -> "JobConfig":
        overrides = overrides or {}

        valid_keys = {f.name for f in fields(cls)}
        for k in overrides:
            if k not in valid_keys:
                raise ValueError(f"Config Injection: Invalid configuration key: {k}")

        # 1. Set default values from static config
        defaults = {
            # PATH
            "input_dir": cg.PATH["input_dir"],
            "output_dir": cg.PATH["output_dir"],
            "logs": cg.PATH["logs"],
            "config": cg.PATH["config"],
            # ENV
            "cpu": cg.ENV["cpu"],
            # LOG
            "log_level": cg.LOG["level"],
            "logfile": cg.LOG["logfile"],
            # "read_from": cg.LOG["read_from"],
            # IO
            "buffer_size": cg.IO["buffer_size"],
            "batch_size": cg.IO["batch_size"],
            "timeout": cg.IO["timeout"],
            "search_page_range": cg.IO["search_page_range"],
            "use_increment": cg.IO["use_increment"],
            # LLM
            "enable": cg.LLM["enable"],
            "keys": cg.LLM["keys"],
            "base_url": cg.LLM["base_url"],
            "model": cg.LLM["model"],
            "sys_prompt": cg.LLM["sys_prompt"],
            "temperature": cg.LLM["temperature"],
            "rpm": cg.LLM["rpm"],
            "tpm": cg.LLM["tpm"],
            # PATTERNS
            "start_patterns": cg.PATTERNS["start_patterns"],
            "ending_patterns": cg.PATTERNS["ending_patterns"],
        }

        # 2. Override with provided values
        final_params = defaults.copy()
        final_params.update(overrides)

        return cls(**final_params)

    def display(self):
        d = asdict(self)

        import json

        print(json.dumps(d, indent=2, ensure_ascii=False, default=str))
