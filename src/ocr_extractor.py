import os
from pathlib import Path
from typing import List
import ocrmypdf

from .config import JobConfig

def process_pdfs_ocr(todo_list: List[Path], conf: JobConfig):
    output_dir = conf.ocr_output
    output_dir.mkdir(parents=True, exist_ok=True)

    if not todo_list:
        return

    print(f"[OCR] Found {len(todo_list)} files to process...")

    for pdf_file in todo_list:
        output_file = output_dir / pdf_file.name

        if output_file.exists():
            print(f"[OCR] Skipping existing: {output_file.name}")
            continue

        print(f"[OCR] Processing: {pdf_file.name} -> {output_file.name}")

        try:
            # Force OCR to fix garbled text
            ocrmypdf.ocr(
                input_file=pdf_file,
                output_file=output_file,
                language="chi_sim",
                force_ocr=True,
                progress_bar=True,
            )
            print(f"✅ [OCR] Success: {output_file.name}")
        except Exception as e:
            print(f"❌ [OCR] Failed {pdf_file.name}: {e}")
