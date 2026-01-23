import pdfplumber
from pathlib import Path
from pdfminer.psparser import PSEOF
import traceback

import pdf_reader.config as cg
from pdf_reader.util.SysException import *
from .locate import locate_mda_section
from .save_file import save_text_to_file
from .extract import extract_content_by_range



def extract_mda_from_pdf(pdf_path: Path) -> int:
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



import signal
from pdf_reader.util.SysException import SysException, ErrorCode
from pdfplumber.utils.exceptions import PdfminerException
from ..util.extract_util import get_file_size_mb

def extract_mda_from_pdf2(pdf_path: Path):
    mda_text = ""
    store_name = pdf_path.stem + ".txt"

    try:
        with pdfplumber.open(pdf_path) as pdf:
            file_size = get_file_size_mb(pdf_path)
            if (file_size > 10.0):
                raise SysException(ErrorCode.TIMEOUT)


            # 1. 定位 MD&A
            mda_range = locate_mda_section(pdf)
            # 2. 提取 MD&A
            mda_text = extract_content_by_range(pdf, mda_range)
            if not mda_text or mda_text == "":
                raise SysException(ErrorCode.UNKNOWN_ERROR)

        # 3. 文件IO，保存
        save_text_to_file(mda_text, cg.PATH["out"] / store_name)
        return 1
    except PdfminerException as e:
        sys_error = ErrorCode.UNREADABLE
        cg.sys_logger.warning(f"{pdf_path.stem}, {sys_error.code}")
        return 0
    except SysException as e:
        cg.sys_logger.warning(f"{pdf_path.stem}, {e.code}")
        return 0
    except Exception as e:
        sys_error = ErrorCode.UNKNOWN_ERROR
        cg.sys_logger.critical(f"{pdf_path.stem}, {sys_error.code}")
        traceback.print_exc()
        return 0
    finally:
        pass