import sys
import argparse
import multiprocessing as mp
import logging
import time
from pathlib import Path
from typing import List
from concurrent.futures import ProcessPoolExecutor, as_completed

from tqdm import tqdm

from .config import JobConfig, PROJECT_ROOT
from .utils import ErrorCode
from .extractor import process_pdf_file_standard
from .llm_extractor import extract_batch_using_llm
from .ocr_extractor import process_pdfs_ocr

# Setup Logging
logger = logging.getLogger("sys")
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def get_todo_list(input_dir: Path, output_dir: Path, use_increment: bool) -> List[Path]:
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    
    all_pdfs = list(input_dir.rglob("*.pdf"))
    valid_pdfs = [p for p in all_pdfs if not p.name.startswith("._") and p.is_file()]
    
    if not use_increment:
        return valid_pdfs
        
    done_stems = {f.stem for f in output_dir.rglob("*.txt")}
    todo = [p for p in valid_pdfs if p.stem not in done_stems]
    
    print(f"Total PDFs: {len(valid_pdfs)}, Done: {len(done_stems)}, Todo: {len(todo)}")
    return todo


def worker_wrapper(args):
    """Unpack args for multiprocessing"""
    file_path, conf = args
    try:
        process_pdf_file_standard(file_path, conf)
        return (file_path, True, ErrorCode.SUCCESS)
    except Exception as e:
        code = getattr(e, 'code', ErrorCode.UNKNOWN_ERROR.code)
        return (file_path, False, code)


def main():
    parser = argparse.ArgumentParser(description="Annual Report to MDA TXT Converter")
    parser.add_argument("--input", type=str, help="Input directory containing PDFs")
    parser.add_argument("--output", type=str, help="Output directory for TXTs")
    parser.add_argument("--cpu", type=int, default=mp.cpu_count(), help="Number of CPU workers")
    parser.add_argument("--llm", action="store_true", help="Enable LLM extraction fallback")
    args = parser.parse_args()

    # Load Config
    overrides = {}
    if args.input: overrides["input_dir"] = Path(args.input)
    if args.output: overrides["output_dir"] = Path(args.output)
    if args.cpu: overrides["cpu"] = args.cpu
    if args.llm: overrides["llm_enable"] = True
    
    conf = JobConfig.from_defaults(overrides)
    conf.display()
    conf.output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Get Tasks
    todo_list = get_todo_list(conf.input_dir, conf.output_dir, conf.use_increment)
    if not todo_list:
        print("No files to process.")
        return

    # 2. Standard Processing (Multiprocessing)
    failed_files = []
    ocr_candidates = []
    llm_candidates = []
    
    print(f"Starting Standard Extraction on {len(todo_list)} files with {conf.cpu} workers...")
    
    with ProcessPoolExecutor(max_workers=conf.cpu) as pool:
        # Create tasks
        future_to_file = {
            pool.submit(worker_wrapper, (p, conf)): p for p in todo_list
        }
        
        with tqdm(total=len(todo_list), unit="file") as pbar:
            for future in as_completed(future_to_file):
                file_path, success, code = future.result()
                pbar.update(1)
                
                if not success:
                    # Classify failure
                    if code == ErrorCode.NO_CHINESE.code:
                        ocr_candidates.append(file_path)
                    elif code == ErrorCode.NO_START.code:
                        llm_candidates.append(file_path) # Candidates for LLM
                    else:
                        failed_files.append((file_path, code))

    print(f"Standard Phase Complete.")
    print(f"Success: {len(todo_list) - len(failed_files) - len(ocr_candidates) - len(llm_candidates)}")
    print(f"Failed (Unknown/Other): {len(failed_files)}")
    print(f"Failed (No Scannable Text - Need OCR): {len(ocr_candidates)}")
    print(f"Failed (Structure Not Found - Need LLM): {len(llm_candidates)}")

    # 3. OCR Processing (Optional)
    if ocr_candidates and conf.ocr_enable:
        print("\n--- Starting OCR Phase ---")
        process_pdfs_ocr(ocr_candidates, conf)
        # Verify if OCR worked (re-add to todo list? or just leave for next run)
        # Ideally, we should re-run extraction on the OCR output.
        # For simplicity, we just output the OCR'ed PDF to a temp folder, 
        # user might need to point input there or we automate it.
        # Current logic just saves OCR pdfs. 
        print("OCR processed. Please check OCR output folder and re-run if needed.")

    # 4. LLM Processing (Optional)
    if llm_candidates and conf.llm_enable:
        print("\n--- Starting LLM Phase ---")
        errors = extract_batch_using_llm(llm_candidates, conf)
        print(f"LLM Phase Complete. {len(llm_candidates) - len(errors)} recovered.")


if __name__ == "__main__":
    mp.freeze_support()
    main()
