import logging
import multiprocessing as mp
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional


# Define Project Root relative to this file
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # src/../

@dataclass
class JobConfig:
    # Path Config (default relative)
    input_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "input")
    output_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "output")
    logs: Path = field(default_factory=lambda: PROJECT_ROOT / "logs")
    
    # Execution
    cpu: int = mp.cpu_count()
    batch_size: int = 10
    timeout: int = 200
    logfile: str = "process.log"
    
    # Logic
    search_page_range: tuple = (3, 25) # Page 4-25
    use_increment: bool = True
    
    # LLM
    llm_enable: bool = False
    llm_keys: List[str] = field(default_factory=list)
    llm_base_url: str = "https://api.siliconflow.cn/v1"
    llm_model: str = "Pro/deepseek-ai/DeepSeek-V3"
    llm_rpm: int = 20
    
    # OCR
    ocr_enable: bool = True
    ocr_output: Path = field(default_factory=lambda: PROJECT_ROOT / "ocr_tmp")

    # Patterns
    start_patterns: List[str] = field(default_factory=lambda: [
        r"^第([一二三四五六七八九十0-9]{1,3})(?:章|节)\s*(?:管理层讨论与分析|董事会报告|董事会工作报告|董事局报告|经营情况讨论与分析)\s*$",
        r"^\s*([一二三四五六七八九十0-9]+)[、，：:]\s*(?:管理层讨论与分析|董事会报告|董事会工作报告|董事局报告|经营情况讨论与分析)\s*$",
    ])
    ending_patterns: List[str] = field(default_factory=lambda: [
        r"^\s*(?:第\s*)?(?:[一二三四五六七八九十0-9]+)\s*[章节]?\s*[：:]*\s*(?:重要事项|公司治理|财务报告|企业管治报告|监事会报告|回顾)",
        r"^\s*(?:重要事项|公司治理|财务报告|企业管治报告|监事会报告)\s*$",
    ])

    @classmethod
    def from_defaults(cls, overrides: dict = None):
        if overrides is None:
            overrides = {}
        return cls(**overrides)

    def display(self):
        print("========== Configuration ==========")
        print(f"Input   : {self.input_dir}")
        print(f"Output  : {self.output_dir}")
        print(f"CPU     : {self.cpu}")
        print(f"LLM     : {self.llm_enable}")
        print("===================================")
