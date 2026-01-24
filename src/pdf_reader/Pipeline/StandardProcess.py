from .Pipeline import PipelineContext, PipelineStage
from ..config import setup_listener, read_log


class StandardProcess(PipelineStage):
    def process(self, stage_context: PipelineContext) -> PipelineContext:
        if stage_context.done:
            print("所有文件均已成功解析")
            return PipelineContext(None, True)

        print("开始标准解析过程")
        conf = self.get_stage_config()
        print("当前参数为: ")
        conf.display()

        log_path = conf.logs / conf.logfile
        log_queue = mp.Queue(-1)
        log_listener = setup_listener(log_path, log_queue)
        setup_root_logger(log_queue)

        todo_list = get_incremental_tasks(conf.input_dir, conf.output_dir)
        try:
            run_processing_module(todo_list, log_queue, conf)
        finally:
            from time import sleep

            sleep(1.0)
            log_listener.stop()

        done = self.check_done(log_path)
        if done:
            print("所有文件均成功解析")
        else:
            print(f"文件已写入{log_path}, 提交下一阶段")
        return PipelineContext(log_path, done)


# ==========================================
# 辅助模块: 任务生成 (含增量逻辑)
# ==========================================

from pathlib import Path
from typing import List, Tuple


def get_incremental_tasks(input_dir: Path, output_dir: Path) -> List[Path]:
    """
    执行增量更新逻辑：扫描输入目录，剔除输出目录中已存在的文件。
    """
    print("正在执行增量扫描...")

    # 1. 获取已完成列表 (Set 加速查找)
    # 假设输出文件后缀为 .txt，根据 stem (文件名不含后缀) 进行匹配
    done_stems = {f.stem for f in output_dir.rglob("*.txt") if f.is_file()}

    print(f" -> 历史已完成任务数: {len(done_stems)}")

    # 2. 扫描待处理列表
    todo_files = []
    skipped_count = 0

    for pdf_path in input_dir.rglob("*.pdf"):
        if pdf_path.stem.startswith("._"):
            continue

        if not pdf_path.is_file():
            continue

        if pdf_path.stem in done_stems:
            skipped_count += 1
        else:
            todo_files.append(pdf_path)

    print(f" -> 扫描结束. 跳过: {skipped_count}, 本次待处理: {len(todo_files)}")
    return todo_files


from ..raw_extract import extract_mda_from_pdf2

from ..config import JobConfig, sys_logger, setup_root_logger


# ==========================================
# 模块 2: 多进程处理核心
# ==========================================
def do_work(batch_files: List[Path], conf: JobConfig) -> Tuple[int, int]:
    """
    Worker 函数：被子进程调用
    """
    success_count = 0
    for file_path in batch_files:
        # 假设 extract_mda_from_pdf2 返回 1(成功) 或 0(失败)
        try:
            res = extract_mda_from_pdf2(file_path, conf)
        except SysException as e:
            sys_logger.warning(f"{file_path.resolve()},{e.code}")
            res = 0
        success_count += res
    return success_count, len(batch_files)


import functools

from tqdm import tqdm
from pebble import ProcessPool, ProcessExpired
from concurrent.futures import as_completed
import multiprocessing as mp
import logging.handlers

from ..util.SysException import *


def run_processing_module(
    todo_files: List[Path],
    log_queue: mp.Queue,
    conf: JobConfig,
) -> int:
    """
    核心调度器：创建进程池，分发任务，管理进度条。
    """
    total_tasks = len(todo_files)
    if total_tasks == 0:
        return 0

    worker = functools.partial(do_work, conf=conf)
    total_success = 0

    # 使用 Context Manager 管理进程池，确保异常时自动关闭
    with ProcessPool(
        max_workers=conf.cpu,
        initializer=setup_root_logger,
        initargs=[
            log_queue,
        ],
    ) as pool:

        futures = {}

        # 1. 任务分发 (Submit)
        for i in range(0, total_tasks, conf.batch_size):
            batch = todo_files[i : i + conf.batch_size]
            future = pool.schedule(
                worker,
                args=[
                    batch,
                ],
                timeout=conf.timeout,
            )
            futures[future] = batch

        # 2. 结果收集与监控 (Monitor)
        with tqdm(total=total_tasks, unit="file", ncols=100, mininterval=1.0) as pbar:
            from concurrent.futures import as_completed

            for future in as_completed(futures):
                batch_data: List[Path] = futures[future]
                try:
                    success, batch_len = future.result()
                    total_success += success

                except TimeoutError:
                    for d in batch_data:
                        sys_logger.warning(
                            f"{d.resolve()},{ErrorCode.BATCH_CORRUPT.code}"
                        )
                except ProcessExpired:
                    for d in batch_data:
                        sys_logger.warning(
                            f"{d.resolve()},{ErrorCode.BATCH_CORRUPT.code}"
                        )
                except Exception as e:
                    for d in batch_data:
                        sys_logger.critical(
                            f"{d.resolve()},{ErrorCode.BATCH_CORRUPT.code}"
                        )
                    tqdm.write(f"\nOpps! Subprocess corrupt!\nForce exit now!\n")
                finally:
                    pbar.update(batch_len)
                    pbar.set_postfix(ok=total_success, fail=pbar.n - total_success)

    return total_success
