import multiprocessing as mp
# from concurrent.futures import ProcessPoolExecutor, as_completed
from pebble import ProcessPool, ProcessExpired
from tqdm import tqdm
from pathlib import Path
from typing import List, Tuple
import logging
import logging.handlers

# 导入你的业务模块
import pdf_reader.config as cg
from .raw_extract import extract_mda_from_pdf2
from .util.SysException import *


# ==========================================
# 模块 1: 环境配置与初始化
# ==========================================
def init_environment() -> Tuple[mp.Queue, logging.handlers.QueueListener, Path, Path]:
    """
    初始化日志系统，检查文件路径权限。
    返回: (日志队列, 日志监听器, 输入路径, 输出路径)
    """
    # 1. 路径检查
    input_dir = cg.PATH["src"]
    output_dir = cg.PATH["out"]

    if not input_dir.exists():
        raise FileNotFoundError(f"输入目录不存在: {input_dir}")

    # 确保输出目录存在
    output_dir.mkdir(parents=True, exist_ok=True)

    # 2. 日志系统初始化
    # 使用 mp.Queue(-1) 避免队列满导致的死锁
    log_queue = mp.Queue(-1)

    # 启动监听器 (必须使用 log_config 中配置了 TqdmHandler 的版本)
    log_listener = cg.setup_listener(log_queue)

    # TEST 测试是否需要在主进程初始化根日志器
    cg.setup_root_logger(log_queue)

    # 打印初始化信息 (此时 log_listener 已启动，可以使用 logger)
    print(f"环境初始化完成. Input: {input_dir}, Output: {output_dir}")

    return log_queue, log_listener, input_dir, output_dir


# ==========================================
# 辅助模块: 任务生成 (含增量逻辑)
# ==========================================
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
        if not pdf_path.is_file():
            continue

        if pdf_path.stem in done_stems:
            skipped_count += 1
        else:
            todo_files.append(pdf_path)

    print(f" -> 扫描结束. 跳过: {skipped_count}, 本次待处理: {len(todo_files)}")
    return todo_files


def filter_timeout_tasks(batch: List[Path], output_dir: Path) -> List[Path]:
    # 1. 获取已完成列表 (Set 加速查找)
    # 假设输出文件后缀为 .txt，根据 stem (文件名不含后缀) 进行匹配
    done_stems = {f.stem for f in output_dir.rglob("*.txt") if f.is_file()}

    # 2. 扫描待处理列表
    todo_files = []
    skipped_count = 0

    for pdf_path in batch:
        if not pdf_path.is_file():
            continue

        if pdf_path.stem in done_stems:
            skipped_count += 1
        else:
            todo_files.append(pdf_path)

    return todo_files

# ==========================================
# 模块 2: 多进程处理核心
# ==========================================
def do_work(batch_files: List[Path]) -> Tuple[int, int]:
    """
    Worker 函数：被子进程调用
    """
    success_count = 0
    for file_path in batch_files:
        # 假设 extract_mda_from_pdf2 返回 1(成功) 或 0(失败)
        cg.sys_logger.debug(f"try to deal with {file_path.stem}")
        res = extract_mda_from_pdf2(file_path)
        success_count += res
    return success_count, len(batch_files)


def run_processing_module(todo_files: List[Path], log_queue: mp.Queue, timeout=None, batch_size=cg.IO["batch_size"], start_page=cg.IO["search_page_range"][0], end_page=cg.IO["search_page_range"][1]) -> Tuple[int, List[Path] | None]:
    """
    核心调度器：创建进程池，分发任务，管理进度条。
    """
    total_tasks = len(todo_files)
    if total_tasks == 0:
        print("没有需要处理的任务。")
        return 0, None

    total_success = 0
    retry_batch: List[Path] | None = None



    # 使用 Context Manager 管理进程池，确保异常时自动关闭
    with ProcessPool(
        max_workers=cg.ENV["cpu"],
        initializer=cg.setup_root_logger,
        initargs=[log_queue,],
    ) as pool:

        futures = {}

        # 1. 任务分发 (Submit)
        for i in range(0, total_tasks, batch_size):
            batch = todo_files[i : i + batch_size]
            future = pool.schedule(do_work, args=[batch,])
            futures[future] = batch

        print(f"任务已分发至 {cg.ENV['cpu']} 个核心，共 {len(futures)} 个 Batch...")

        # 2. 结果收集与监控 (Monitor)
        with tqdm(total=total_tasks, unit="file", ncols=100, mininterval=1.0) as pbar:
            from concurrent.futures import as_completed

            for future in as_completed(futures):
                batch_data: List[Path] = futures[future]
                try:
                    success, batch_len = future.result(timeout=timeout)
                    total_success += success

                    pbar.update(batch_len)
                    pbar.set_postfix(ok=total_success, fail=pbar.n - total_success)

                except TimeoutError:
                    if not retry_batch:
                        retry_batch = batch_data
                    else:
                        retry_batch.extend(batch_data)
                    for d in batch_data:
                        cg.sys_logger.warning(f"{d.stem}, {ErrorCode.BATCH_CORRUPT.code}")
                except ProcessExpired:
                    if not retry_batch:
                        retry_batch = batch_data
                    else:
                        retry_batch.extend(batch_data)
                    for d in batch_data:
                        cg.sys_logger.warning(f"{d.stem}, {ErrorCode.BATCH_CORRUPT.code}")
                except Exception as e:
                    if not retry_batch:
                        retry_batch = batch_data
                    else:
                        retry_batch.extend(batch_data)
                    for d in batch_data:
                        cg.sys_logger.critical(f"{d.stem}, {ErrorCode.BATCH_CORRUPT.code}")
                    tqdm.write(f"\nOpps! Subprocess corrupt!\nForce exit now!\n")

    return total_success, retry_batch


# ==========================================
# 模块 3: 失败重试 (预留接口)
# ==========================================
def run_retry_module(log_queue: mp.Queue):
    """
    [预留] 从失败日志中读取记录并重试。
    目前仅做占位，未来在这里实现读取 fail.log 的逻辑。
    """
    # 示例逻辑：
    # 1. scan_failed_logs()
    # 2. if failed_files: run_processing_module(failed_files, log_queue)
    print("模块 3 (失败重试) 暂未启用，跳过...")
    pass


# ==========================================
# 模块 4: 资源释放
# ==========================================
def cleanup_resources(log_listener: logging.handlers.QueueListener | None):
    """
    安全停止日志监听器，防止程序退出时报错。
    """
    if log_listener:
        print("正在清理资源...")
        log_listener.stop()
        # print("资源释放完成。")


# ==========================================
# 主编排函数 (Orchestrator)
# ==========================================
def main():
    log_listener = None
    try:
        # --- Phase 1: 初始化 ---
        log_queue, log_listener, input_dir, output_dir = init_environment()

        # --- Phase 2: 获取任务 ---
        todo_files = get_incremental_tasks(input_dir, output_dir)

        # --- Phase 3: 多进程处理 ---
        newly_success, timeout_batch = run_processing_module(todo_files, log_queue, timeout=500, batch_size=10, start_page=4, end_page=25)

        # --- Phase 3.2: 处理超时文件
        if timeout_batch:
            timeout_batch = filter_timeout_tasks(timeout_batch, output_dir)
            ts = run_processing_module(timeout_batch, log_queue, timeout=50, batch_size=1, start_page=4, end_page=25)
            ts = 0
            newly_success += ts

        # print(f"\n>>> 主处理阶段结束. 本次成功: {newly_success}/{len(todo_files)}")

        # --- Phase 4: 失败重试 (预留) ---
        run_retry_module(log_queue)

    except KeyboardInterrupt:
        print("\n[!] 用户强制中断 (Ctrl+C). 正在紧急停止所有进程...")
        # ProcessPoolExecutor 的 exit 会自动处理，这里主要是给用户反馈
    except Exception as e:
        # 捕获 Main 函数层级的配置错误或未知错误
        if log_listener:
            cg.sys_logger.critical(f"-, 系统发生系统级崩溃！", exc_info=True)
        else:
            print(f"FATAL ERROR (Logger未启动): {e}")
    finally:
        # --- Phase 5: 资源释放 (无论是成功还是报错都会执行) ---
        cleanup_resources(log_listener)

if __name__ == "__main__":
    mp.freeze_support() # Windows 下打包可能需要
    main()