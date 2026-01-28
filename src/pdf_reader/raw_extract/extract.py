def remove_duplicate_chars(text):
    if not text:
        return ""
    result = []
    if len(text) > 0:
        result.append(text[0])
    for i in range(1, len(text)):
        char = text[i]
        if char != text[i - 1]:
            result.append(char)
        elif ("a" <= char <= "z") or ("A" <= char <= "Z") or ("0" <= char <= "9"):
            result.append(char)
    return "".join(result)


def clean_special_chars(text):
    if not text:
        return ""
    return "".join(ch for ch in text if ch.isprintable() or ch in "\n\t")


def extract_text_for_content(page, aggressive_crop=True):
    width = page.width
    height = page.height

    # 1. Crop
    if aggressive_crop:
        try:
            # Top 10%, Bottom 10% crop
            cropped = page.crop((0, height * 0.1, width, height * 0.9))
        except ValueError:
            cropped = page
    else:
        cropped = page

    # 2. Table Filtering
    tables = cropped.find_tables()

    def not_within_tables(obj):
        obj_x0 = obj.get("x0", 0)
        obj_top = obj.get("top", 0)
        obj_x1 = obj.get("x1", 0)
        obj_bottom = obj.get("bottom", 0)
        cx = (obj_x0 + obj_x1) / 2
        cy = (obj_top + obj_bottom) / 2
        for table in tables:
            bbox = table.bbox
            if (bbox[0] <= cx <= bbox[2]) and (bbox[1] <= cy <= bbox[3]):
                return False
        return True

    try:
        if tables:
            filtered = cropped.filter(not_within_tables)
            text = filtered.extract_text()
        else:
            text = cropped.extract_text()
    except:
        text = cropped.extract_text()

    if not text:
        return ""
    text = clean_special_chars(text)
    text = remove_duplicate_chars(text)  # Always dedup content
    return text


from pdfplumber.pdf import PDF
from pdfplumber.page import Page

from .dto import MDARange
from ..config import JobConfig, sys_logger
from ..util.SysException import *


def extract_content_by_range(pdf: PDF, rng: MDARange, conf: JobConfig) -> str:
    """
    根据给定的物理坐标，执行内容的提取和清洗。
    """
    extracted_segments = []

    for i in range(rng.start_page_idx, rng.end_page_idx + 1):
        if i >= len(pdf.pages):
            break

        page = pdf.pages[i]
        # 基础提取
        content = extract_text_for_content(page, aggressive_crop=True)

        # --- 复杂的边界处理逻辑被封装在这里 ---

        # 1. 处理首页 (Head Trimming)
        if i == rng.start_page_idx:
            content = _trim_page_head(content, rng.start_line_text, conf)

        # 2. 处理尾页 (Tail Trimming)
        if i == rng.end_page_idx:
            content = _trim_page_tail(content, rng.chapter_num, rng.pattern_type, conf)
        if content:
            extracted_segments.append(content)

    return "\n".join(extracted_segments)


import re


# 将原来堆在主函数里的正则切分逻辑下沉到私有辅助函数
def _trim_page_head(content: str, raw_marker: str, conf: JobConfig) -> str:
    lines = content.split("\n")
    match_index = -1
    regexes = [re.compile(p) for p in conf.start_patterns]

    for idx, line in enumerate(lines):
        for regex in regexes:
            if regex.search(line.strip()):
                match_index = idx
                break
        if match_index != -1:
            break

        if match_index != -1:
            content = "\n".join(lines[match_index:])
    return content


from ..util.extract_util import *


def _trim_page_tail(
    content: str, chapter_num: int, p_type: str, conf: JobConfig
) -> str:
    """
    逻辑：找到下一章的标题（或通用结束符），并删除从该行开始（含该行）的所有内容。
    """
    if not content:
        return ""

    lines = content.split("\n")
    match_index = -1

    # 1. 重构下一章的正则匹配规则
    # 必须确保 chapter_num 是有效的，如果为 None 则只使用通用结束符
    combined_patterns = []

    if chapter_num is not None:
        next_num = chapter_num + 1
        # 需要 import number_to_chinese
        next_cn = number_to_chinese(next_num)

        patterns = []
        if p_type == "CHAPTER":
            patterns.append(rf"^第(?:{next_num}|{next_cn})(?:章|节)")
        else:
            patterns.append(rf"^\s*(?:{next_num}|{next_cn})[、，：:.]")
        combined_patterns.extend(patterns)

    # 2. 加入通用结束符 (FALLBACK)
    # 假设你的 config 里面有这个列表，类似于 ["审计报告", "财务报表附注"...]
    combined_patterns.extend(conf.ending_patterns)

    regexes = [re.compile(p) for p in combined_patterns]

    # 3. 逐行匹配
    for idx, line in enumerate(lines):
        line_clean = line.strip()
        # 太短的行通常不是标题，跳过以防误杀
        if len(line_clean) < 2:
            continue

        for regex in regexes:
            if regex.search(line_clean):
                match_index = idx
                break
        if match_index != -1:
            break

    # 4. 裁剪
    if match_index != -1:
        # 你的需求是 "delete start position above, include end position below all text"
        # 但对于 MD&A 结尾，通常意味着遇见 "下一章" 就要停止。
        # 所以应该是保留 match_index 之前的内容 ([:match_index])
        # 这样 "第五节 重要事项" 这行字本身也会被删掉，这通常是符合预期的。
        content = "\n".join(lines[:match_index])

    return content


def _extract_text_for_content2(page, aggressive_crop=True):
    # ... (保持你原有的逻辑不变) ...
    width = page.width
    height = page.height

    if aggressive_crop:
        try:
            cropped = page.crop((0, height * 0.1, width, height * 0.9))
        except ValueError:
            cropped = page
    else:
        cropped = page

    tables = cropped.find_tables()

    def not_within_tables(obj):
        obj_x0 = obj.get("x0", 0)
        obj_top = obj.get("top", 0)
        obj_x1 = obj.get("x1", 0)
        obj_bottom = obj.get("bottom", 0)
        cx = (obj_x0 + obj_x1) / 2
        cy = (obj_top + obj_bottom) / 2
        for table in tables:
            bbox = table.bbox
            if (bbox[0] <= cx <= bbox[2]) and (bbox[1] <= cy <= bbox[3]):
                return False
        return True

    try:
        if tables:
            filtered = cropped.filter(not_within_tables)
            text = filtered.extract_text()
        else:
            text = cropped.extract_text()
    except:
        text = cropped.extract_text()

    if not text:
        return ""
    text = clean_special_chars(text)
    text = remove_duplicate_chars(text)
    return text
