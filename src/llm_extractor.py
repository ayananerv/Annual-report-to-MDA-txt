import os
import json
import time
import requests
import pdfplumber
from pathlib import Path
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from .config import JobConfig
from .utils import remove_duplicate_chars, clean_special_chars

# --- Helper Functions ---

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


def read_pdf_pages(pdf_path: Path, start_page: int = 1, end_page: int = 50, add_markers: bool = False):
    text_content = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            for i in range(start_page - 1, min(end_page, total_pages)):
                page = pdf.pages[i]
                current_page_num = i + 1
                page_text = extract_text_for_content(page)

                if add_markers:
                    marked_text = f"「{current_page_num}」\n{page_text}"
                    text_content.append(marked_text)
                else:
                    text_content.append(page_text)
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}")
        return None
    return "\n".join(text_content)


# --- API Interaction ---

class APIKeyManager:
    def __init__(self, conf: JobConfig):
        self.api_keys = conf.llm_keys
        self.requests_per_min = conf.llm_rpm
        self.request_timestamps = []
        self.lock = Lock()
        self.key_index = 0

    def get_key(self):
        with self.lock:
            if not self.api_keys: return "NO_KEY"
            key = self.api_keys[self.key_index]
            self.key_index = (self.key_index + 1) % len(self.api_keys)
            return key

    def wait_for_rate_limit(self):
        width = 60.0
        with self.lock:
            now = time.time()
            self.request_timestamps = [t for t in self.request_timestamps if now - t < width]
            if len(self.request_timestamps) >= self.requests_per_min:
                oldest = self.request_timestamps[0]
                sleep_time = width - (now - oldest) + 0.1
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.request_timestamps.append(time.time())


def call_llm_api(file_name: str, pdf_text: str, conf: JobConfig, api_manager: APIKeyManager):
    prompt = f"""
请阅读以下A股公司年报的前20页内容（包含目录）。
你的任务是：
1. 在目录中找到"管理层讨论与分析"（或类似标题，如"董事会报告"、"经营情况讨论与分析"）的章节，提取目录中显示的开始页码和结束页码。
2. 计算"目录页码"与"PDF真实页码"之间的差值 (page_offset)。
   计算原理：文本中每一页的开头都有标记「页码」（例如「5」表示这是PDF的第5页）。
   请找出一个在目录中也出现的章节（如"公司简介"或"管理层讨论与分析"本身），对比其"目录显示的页码"与"文本中标记的真实页码"。
   公式：page_offset = 真实页码 - 目录页码。

请严格按照以下JSON格式返回，不要包含其他解释：
{{
  "start_page": 目录显示的开始页码 (int),
  "end_page": 目录显示的结束页码 (int),
  "page_offset": 计算出的差值 (int),
  "start_keyword": "找到的目录标题文本",
  "reasoning": "计算差值的依据简述"
}}

文件名: {file_name}

前20页文本 (含真实页码标记):
{pdf_text}
"""
    api_manager.wait_for_rate_limit()
    key = api_manager.get_key()

    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": conf.llm_model,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that extracts structural information from financial reports."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
        "response_format": {"type": "json_object"},
    }

    endpoint = conf.llm_base_url
    if not endpoint.endswith("/chat/completions"):
        endpoint = endpoint.rstrip("/") + "/chat/completions"

    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        result = response.json()
        content = result["choices"][0]["message"]["content"]
        return json.loads(content)
    except Exception as e:
        print(f"API Call Failed for {file_name}: {e}")
        return None


def process_file_llm(file_path: Path, conf: JobConfig, api_manager: APIKeyManager):
    file_name = file_path.name
    print(f"[LLM] Processing: {file_name}")

    cleaned_text = read_pdf_pages(file_path, 1, 20, add_markers=True)
    if not cleaned_text:
        return {"file": file_name, "error": "Empty or unreadable PDF"}

    location_data = call_llm_api(file_name, cleaned_text, conf, api_manager)
    if not location_data:
        return {"file": file_name, "error": "LLM extraction failed"}

    start_page_doc = location_data.get("start_page")
    end_page_doc = location_data.get("end_page")
    offset = location_data.get("page_offset", 0)

    if not start_page_doc:
        return {"file": file_name, "error": "Could not find MDA in Directory"}

    if offset is None: offset = 0
    actual_start = int(start_page_doc + offset)
    actual_end = int(end_page_doc + offset) if end_page_doc else int(actual_start + 50)

    print(f"  -> [LLM] Found Range: {actual_start}-{actual_end} (Offset: {offset})")

    try:
        final_text = []
        with pdfplumber.open(file_path) as pdf:
            total_pages = len(pdf.pages)
            start_idx = max(0, actual_start - 1)
            end_idx = min(total_pages, actual_end)
            
            for i in range(start_idx, end_idx):
                page = pdf.pages[i]
                content = extract_text_for_content(page)
                final_text.append(content)

        final_content = "\n".join(final_text)
        
        output_path = conf.output_dir / file_path.with_suffix(".txt").name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(final_content)
            
        print(f"[LLM] Successfully saved {output_path.name}")
        return None

    except Exception as e:
        return {"file": file_name, "error": f"Extraction error: {e}"}


def extract_batch_using_llm(pdf_files: List[Path], conf: JobConfig) -> List[dict]:
    if not conf.llm_enable or not conf.llm_keys:
        print("LLM disabled or no keys provided.")
        return []

    print(f"Starting LLM Extraction for {len(pdf_files)} files...")
    api_manager = APIKeyManager(conf)
    errors = []
    
    import functools
    worker = functools.partial(process_file_llm, conf=conf, api_manager=api_manager)
    
    # Simple ThreadPool
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(worker, pdf_files)
        for res in results:
            if res:
                errors.append(res)
                
    return errors
