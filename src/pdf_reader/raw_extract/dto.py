from dataclasses import dataclass
from typing import Optional

@dataclass
class MDARange:
    start_page_idx: int
    end_page_idx: int
    chapter_num: int
    pattern_type: str
    start_line_text: str  # 用于首页定位切割点
    end_line_text: Optional[str] = None # 用于尾页定位切割点