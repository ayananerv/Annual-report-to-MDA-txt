from warnings import deprecated

from pathlib import Path
import pdfplumber

from .locate import locate_mda_section
from .extract import extract_content_by_range
from .save_file import save_text_to_file

from ..config import JobConfig


@deprecated("Use extract_mda_from_pdf2 instead")
def extract_mda_from_pdf(pdf_path: Path, conf: JobConfig) -> int:
    mda_text = ""
    store_name = pdf_path.stem + ".txt"
    with pdfplumber.open(pdf_path) as pdf:
        # 1. 定位 MD&A
        mda_range = locate_mda_section(pdf, conf)
        # 2. 提取 MD&A
        mda_text = extract_content_by_range(pdf, mda_range, conf)

    # 3. 文件IO，保存
    save_text_to_file(mda_text, conf.output_dir / store_name)
    return 1


from ..util.extract_util import get_file_size_mb
from ..util.SysException import *
from ..config import JobConfig, sys_logger

from pdfplumber.utils.exceptions import PdfminerException
import traceback
import pdfplumber


def extract_mda_from_pdf2(pdf_path: Path, conf: JobConfig) -> int:
    mda_text = ""
    store_name = pdf_path.stem + ".txt"

    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 1. 定位 MD&A
            mda_range = locate_mda_section(pdf, conf)
            # 2. 提取 MD&A
            mda_text = extract_content_by_range(pdf, mda_range, conf)
            if not mda_text or mda_text == "":
                raise SysException(ErrorCode.UNKNOWN_ERROR)

        # 3. 文件IO，保存
        save_text_to_file(mda_text, conf.output_dir / store_name)
        return 1
    except PdfminerException as e:
        raise SysException(ErrorCode.UNREADABLE)
    except SysException as e:
        sys_logger.warning(f"{pdf_path.resolve()},{e.code}")
        return 0
    except Exception as e:
        raise SysException(ErrorCode.UNKNOWN_ERROR)
        # traceback.print_exc()
