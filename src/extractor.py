import re
import traceback
from pathlib import Path
from dataclasses import dataclass

import pdfplumber
from pdfplumber.pdf import PDF
from pdfplumber.page import Page
from pdfplumber.utils.exceptions import PdfminerException

from .config import JobConfig
from .utils import (
    ErrorCode, 
    SysException, 
    clean_special_chars, 
    remove_duplicate_chars, 
    chinese_to_number, 
    number_to_chinese
)
import logging

logger = logging.getLogger("sys")

@dataclass
class MDARange:
    start_page_idx: int
    end_page_idx: int
    chapter_num: int  # e.g., 4 means "Chapter 4"
    pattern_type: str  # "CHAPTER" or "DIGIT"
    start_line_text: str
    end_line_text: str | None


# --- LOCATING LOGIC ---

def extract_text_for_detection(page: Page) -> str:
    raw_text = page.extract_text()
    return raw_text if raw_text else ""


def find_start(pdf: PDF, conf: JobConfig):
    regexes = [re.compile(p) for p in conf.start_patterns]
    start_search = conf.search_page_range[0]
    end_search = min(conf.search_page_range[1], len(pdf.pages))

    read_text = ""
    has_chinese = False

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
                    except Exception:
                        pass
        return None

    for i in range(start_search, end_search):
        page = pdf.pages[i]
        
        # Method 1: Raw
        text_raw = extract_text_for_detection(page)
        if text_raw and re.search(r"[\u4e00-\u9fa5]", text_raw):
            has_chinese = True

        result = search_in_text(text_raw)
        if result:
            return (i, *result)

        # Method 2: Dedup
        text_dedup = remove_duplicate_chars(text_raw)
        if text_dedup != text_raw:
            result = search_in_text(text_dedup)
            if result:
                return (i, *result)
        
        read_text += text_dedup

    if not has_chinese:
        raise SysException(ErrorCode.NO_CHINESE)
    
    raise SysException(ErrorCode.NO_START)


def find_end_page(
    pdf: PDF, start_index: int, chapter_num: int, pattern_type: str, conf: JobConfig
) -> tuple[int, str | None]:
    
    next_num = chapter_num + 1
    next_cn = number_to_chinese(next_num)

    patterns = []
    if pattern_type == "CHAPTER":
        patterns.append(rf"^第(?:{next_num}|{next_cn})(?:章|节)")
    else:
        patterns.append(rf"^\s*(?:{next_num}|{next_cn})[、，：:.]")

    combined = patterns + conf.ending_patterns
    regexes = [re.compile(p) for p in combined]

    end_search = len(pdf.pages)

    for i in range(start_index, end_search):
        if i > end_search * 0.75: # Stop if we went too far (75% of doc)
             raise SysException(ErrorCode.NO_ENDING)

        page = pdf.pages[i]

        def search_in_text(text):
            if not text: return None
            for line in text.split("\n"):
                line = line.strip()
                if len(line) < 2: continue
                for regex in regexes:
                    if regex.search(line):
                        return line
            return None

        text_raw = extract_text_for_detection(page)
        matched_line = search_in_text(text_raw)
        if matched_line:
            return i, matched_line

        text_dedup = remove_duplicate_chars(text_raw)
        if text_dedup != text_raw:
             matched_line = search_in_text(text_dedup)
             if matched_line:
                 return i, matched_line

    raise SysException(ErrorCode.NO_ENDING)


def locate_mda_section(pdf: PDF, conf: JobConfig) -> MDARange:
    start_idx, chap_num, p_type, s_line = find_start(pdf, conf)
    end_idx, e_line = find_end_page(pdf, start_idx, chap_num, p_type, conf)
    return MDARange(start_idx, end_idx, chap_num, p_type, s_line, e_line)


# --- EXTRACTING LOGIC ---

def extract_text_for_content(page, aggressive_crop=True):
    width = page.width
    height = page.height

    if aggressive_crop:
        try:
            cropped = page.crop((0, height * 0.1, width, height * 0.9))
        except ValueError:
            cropped = page
    else:
        cropped = page

    # Table filtering
    tables = cropped.find_tables() or []
    
    def not_within_tables(obj):
        cx = (obj.get("x0",0) + obj.get("x1",0)) / 2
        cy = (obj.get("top",0) + obj.get("bottom",0)) / 2
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


def _trim_page_head(content: str, conf: JobConfig) -> str:
    lines = content.split("\n")
    match_index = -1
    regexes = [re.compile(p) for p in conf.start_patterns]
    for idx, line in enumerate(lines):
        for regex in regexes:
            if regex.search(line.strip()):
                match_index = idx
                break
        if match_index != -1: break
    
    if match_index != -1:
        return "\n".join(lines[match_index:])
    return content

def _trim_page_tail(content: str, chapter_num: int, p_type: str, conf: JobConfig) -> str:
    if not content: return ""
    lines = content.split("\n")
    match_index = -1
    
    next_num = chapter_num + 1
    next_cn = number_to_chinese(next_num)
    patterns = []
    if p_type == "CHAPTER":
        patterns.append(rf"^第(?:{next_num}|{next_cn})(?:章|节)")
    else:
        patterns.append(rf"^\s*(?:{next_num}|{next_cn})[、，：:.]")
    
    combined = patterns + conf.ending_patterns
    regexes = [re.compile(p) for p in combined]
    
    for idx, line in enumerate(lines):
        if len(line.strip()) < 2: continue
        for regex in regexes:
            if regex.search(line.strip()):
                match_index = idx
                break
        if match_index != -1: break
    
    if match_index != -1:
        return "\n".join(lines[:match_index])
    return content


def extract_content_by_range(pdf: PDF, rng: MDARange, conf: JobConfig) -> str:
    extracted_segments = []
    for i in range(rng.start_page_idx, rng.end_page_idx + 1):
        if i >= len(pdf.pages): break
        
        page = pdf.pages[i]
        content = extract_text_for_content(page, aggressive_crop=True)
        
        if i == rng.start_page_idx:
            content = _trim_page_head(content, conf)
        if i == rng.end_page_idx:
            content = _trim_page_tail(content, rng.chapter_num, rng.pattern_type, conf)
            
        if content:
            extracted_segments.append(content)
            
    return "\n".join(extracted_segments)


def process_pdf_file_standard(pdf_path: Path, conf: JobConfig) -> bool:
    """
    Standard extraction entry point.
    Returns: True if successful, False otherwise.
    """
    store_name = pdf_path.stem + ".txt"
    output_path = conf.output_dir / store_name
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            mda_range = locate_mda_section(pdf, conf)
            mda_text = extract_content_by_range(pdf, mda_range, conf)
            
            if not mda_text:
                raise SysException(ErrorCode.UNKNOWN_ERROR)
                
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(mda_text)
        return True

    except PdfminerException:
        raise SysException(ErrorCode.UNREADABLE)
    except SysException as e:
        logger.warning(f"{pdf_path.name},{e.code},{e.message}")
        raise e # Re-raise to let caller know specific error
    except Exception:
        raise SysException(ErrorCode.UNKNOWN_ERROR)
