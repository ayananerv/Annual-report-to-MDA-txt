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


from pdfplumber.page import Page
from pdfplumber.pdf import PDF

from warnings import deprecated

from ..util.SysException import *


@deprecated("Use extract_text_for_detection2 instead")
def extract_text_for_detection(page: Page) -> str:
    # 1. 提取原始文本
    raw_text = page.extract_text()
    # 2. 剔除空白符
    cleaned_text = raw_text.strip() if raw_text else ""

    # 3. 判断有文本
    has_text = len(cleaned_text) > 0

    # 4. 判断其他对象
    has_images = len(page.images) > 0

    if has_text:
        return raw_text
    elif has_images:
        # 没有文字但是有图片，可OCR
        raise SysException(ErrorCode.UNREADABLE)
    else:
        # 可以正常解析，但是是空白页
        return ""


def extract_text_for_detection2(page: Page) -> str:
    raw_text = page.extract_text()
    return raw_text if raw_text else ""


import re

from ..config import JobConfig, sys_logger
from .dto import MDARange
from ..util.extract_util import *


def find_start(pdf: PDF, conf: JobConfig):

    regexes = [re.compile(p) for p in conf.start_patterns]
    start_search = conf.search_page_range[0]
    end_search = min(conf.search_page_range[1], len(pdf.pages))

    read_text = ""

    def has_chinese_char(text: str) -> bool:
        """
        检测字符串中是否包含中文字符。
        """
        if not text:
            return False

        # \u4e00-\u9fa5 覆盖了基本汉字范围
        pattern = re.compile(r"[\u4e00-\u9fa5]")

        # 只要找到一个匹配项，search 就会返回对象，否则返回 None
        if pattern.search(text):
            return True
        return False

    for i in range(start_search, end_search):
        page = pdf.pages[i]

        def search_in_text(text):
            if not text:
                return None
            lines = text.split("\n")
            for line in lines:
                line = line.strip()
                for r_idx, regex in enumerate(regexes):
                    match = regex.search(line)
                    if match:
                        try:
                            num = chinese_to_number(match.group(1))
                            ptype = "CHAPTER" if r_idx == 0 else "DIGIT"
                            return num, ptype, line
                        except Exception as e:
                            raise e
            return None

        # Method 1: Raw
        text_raw = extract_text_for_detection2(page)
        result = search_in_text(text_raw)
        if result:
            return (i, *result)

        # Method 2: Dedup
        text_dedup = remove_duplicate_chars(text_raw)
        if text_dedup == text_raw:
            continue
        result = search_in_text(text_dedup)
        if result:
            return (i, *result)

        read_text += text_dedup

    if has_chinese_char(read_text):
        raise SysException(ErrorCode.NO_START)
    else:
        raise SysException(ErrorCode.NO_CHINESE)


from typing import Tuple


def find_end_page(
    pdf: PDF, start_index: int, chapter_num: int, pattern_type: str, conf: JobConfig
) -> Tuple[int, str | None]:
    # --- 1. 预计算下一章的特征 (Preparation) ---
    # 我们不再需要像 find_start 那样去提取数字，而是根据已知章节号推算下一章
    next_num = chapter_num + 1
    next_cn = number_to_chinese(next_num)

    patterns = []
    # 根据开头是“第四节”还是“4、”来决定结尾找“第五节”还是“5、”
    if pattern_type == "CHAPTER":
        # 匹配: 第五节 / 第5节 / 第五章
        patterns.append(rf"^第(?:{next_num}|{next_cn})(?:章|节)")
    else:
        # 匹配: 5、 / 5. / 五、
        patterns.append(rf"^\s*(?:{next_num}|{next_cn})[、，：:.]")

    # 加入通用的结束词（如“审计报告”、“附注”等）
    combined = patterns + conf.ending_patterns
    regexes = [re.compile(p) for p in combined]

    end_search = len(pdf.pages)

    # --- 2. 页面遍历 (Scanning) ---
    for i in range(start_index, end_search):
        # 如果到了整个年报 2/3 的位置仍未找到，则认为没有找到
        if i > end_search * 2 // 3:
            raise SysException(ErrorCode.NO_ENDING)

        page = pdf.pages[i]

        # 内部闭包：复用查找逻辑
        def search_in_text(text):
            if not text:
                return None
            lines = text.split("\n")
            for line in lines:
                line = line.strip()
                # 性能优化：太短的行不可能是标题
                if len(line) < 2:
                    continue

                for regex in regexes:
                    if regex.search(line):
                        # 找到了！返回匹配的行文本，用于后续裁剪
                        return line
            return None

        # Method 1: Check Raw Text (优先检查原始文本)
        text_raw = extract_text_for_detection2(page)
        matched_line = search_in_text(text_raw)
        if matched_line:
            return i, matched_line

        # Method 2: Check Dedup Text (去重后检查)
        # 只有当原始文本里没找到，且去重后内容有变化时才进行尝试
        text_dedup = remove_duplicate_chars(text_raw)
        if text_dedup != text_raw:
            matched_line = search_in_text(text_dedup)
            if matched_line:
                return i, matched_line

    raise SysException(ErrorCode.NO_ENDING)


def locate_mda_section(pdf: PDF, conf: JobConfig) -> MDARange:
    """
    负责在 PDF 中寻找 MD&A 的物理坐标。
    如果找不到，直接抛出异常，中断流程。
    """
    # 1. 找开头
    start_idx, chap_num, p_type, s_line = find_start(pdf, conf)

    # 2. 找结尾
    end_idx, e_line = find_end_page(pdf, start_idx, chap_num, p_type, conf)
    # 返回纯数据对象
    return MDARange(
        start_page_idx=start_idx,
        end_page_idx=end_idx,
        chapter_num=chap_num,
        pattern_type=p_type,
        start_line_text=s_line,
        end_line_text=e_line,
    )
