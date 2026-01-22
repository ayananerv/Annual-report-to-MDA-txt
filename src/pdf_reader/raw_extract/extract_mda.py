import pdfplumber
from pathlib import Path

import pdf_reader.config as cg
from pdf_reader.util.SysException import *
from .locate import locate_mda_section
from .save_file import save_text_to_file
from .extract import extract_content_by_range



def extract_mda_from_pdf2(pdf_path: Path) -> int:
    try:
        mda_text = ""
        store_name = pdf_path.stem + ".txt"
        with pdfplumber.open(pdf_path) as pdf:
            # 1. 定位 MD&A
            mda_range = locate_mda_section(pdf)
            # 2. 提取 MD&A
            mda_text = extract_content_by_range(pdf, mda_range)

        # 3. 文件IO，保存
        save_text_to_file(mda_text, cg.PATH["out"] / store_name)
        return 1
    except SysException as e:
        cg.sys_logger.warning(f"{pdf_path.stem}, {e.code}")
        return 0
    except UnknownException as e:
        cg.sys_logger.error(f"{pdf_path.stem}, {e.code}: {e.message}")
        return 0
    finally:
        pass

