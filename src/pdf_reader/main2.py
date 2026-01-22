import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path
from typing import List, Set, Tuple
import logging
import logging.handlers

# 导入你的业务模块
import pdf_reader.config as cg
from .raw_extract import extract_mda_from_pdf2


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

    # 打印初始化信息 (此时 log_listener 已启动，可以使用 logger)
    cg.sys_logger.info(f"环境初始化完成. Input: {input_dir}, Output: {output_dir}")

    return log_queue, log_listener, input_dir, output_dir


# ==========================================
# 辅助模块: 任务生成 (含增量逻辑)
# ==========================================
def get_incremental_tasks(input_dir: Path, output_dir: Path) -> List[Path]:
    """
    执行增量更新逻辑：扫描输入目录，剔除输出目录中已存在的文件。
    """
    cg.sys_logger.info("正在执行增量扫描...")

    # 1. 获取已完成列表 (Set 加速查找)
    # 假设输出文件后缀为 .txt，根据 stem (文件名不含后缀) 进行匹配
    done_stems = {f.stem for f in output_dir.rglob("*.txt") if f.is_file()}

    cg.sys_logger.info(f" -> 历史已完成任务数: {len(done_stems)}")

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

    cg.sys_logger.info(f" -> 扫描结束. 跳过: {skipped_count}, 本次待处理: {len(todo_files)}")
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
        res = extract_mda_from_pdf2(file_path)
        success_count += res
    return success_count, len(batch_files)


def run_processing_module(todo_files: List[Path], log_queue: mp.Queue) -> int:
    """
    核心调度器：创建进程池，分发任务，管理进度条。
    """
    batch_size = cg.IO["batch_size"]
    total_tasks = len(todo_files)
    if total_tasks == 0:
        print("没有需要处理的任务。")
        return 0

    total_success = 0

    # 使用 Context Manager 管理进程池，确保异常时自动关闭
    with ProcessPoolExecutor(
        max_workers=cg.ENV["cpu"],
        initializer=cg.setup_root_logger,
        initargs=(log_queue,),
    ) as executor:

        futures = {}

        # 1. 任务分发 (Submit)
        for i in range(0, total_tasks, batch_size):
            batch = todo_files[i : i + batch_size]
            future = executor.submit(do_work, batch)
            futures[future] = batch

        cg.sys_logger.info(f"任务已分发至 {cg.ENV['cpu']} 个核心，共 {len(futures)} 个 Batch...")

        # 2. 结果收集与监控 (Monitor)
        with tqdm(total=total_tasks, unit="file", ncols=100, mininterval=1.0) as pbar:
            for future in as_completed(futures):
                batch_data = futures[future]
                try:
                    success, batch_len = future.result()
                    total_success += success

                    pbar.update(batch_len)
                    pbar.set_postfix(ok=total_success, fail=pbar.n - total_success)

                except Exception as e:
                    # 处理进程级崩溃 (Unexpected EOF 等)
                    error_msg = str(e)
                    if "Unexpected EOF" in error_msg:
                        tqdm.write(f"CRITICAL: 子进程 OOM 或崩溃 (Batch: {len(batch_data)} files)")
                    else:
                        tqdm.write(f"Batch Error: {e}")

                    # 即使失败也要更新进度条，防止死锁卡住
                    pbar.update(len(batch_data))

    return total_success


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
    cg.sys_logger.info("模块 3 (失败重试) 暂未启用，跳过...")
    pass


# ==========================================
# 模块 4: 资源释放
# ==========================================
def cleanup_resources(log_listener: logging.handlers.QueueListener | None):
    """
    安全停止日志监听器，防止程序退出时报错。
    """
    if log_listener:
        cg.sys_logger.info("正在清理资源...")
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
        newly_success = run_processing_module(todo_files, log_queue)

        # print(f"\n>>> 主处理阶段结束. 本次成功: {newly_success}/{len(todo_files)}")

        # --- Phase 4: 失败重试 (预留) ---
        run_retry_module(log_queue)

    except KeyboardInterrupt:
        print("\n[!] 用户强制中断 (Ctrl+C). 正在紧急停止所有进程...")
        # ProcessPoolExecutor 的 exit 会自动处理，这里主要是给用户反馈
    except Exception as e:
        # 捕获 Main 函数层级的配置错误或未知错误
        if log_listener:
            cg.sys_logger.critical(f"程序发生严重错误: {e}", exc_info=True)
        else:
            print(f"FATAL ERROR (Logger未启动): {e}")
    finally:
        # --- Phase 5: 资源释放 (无论是成功还是报错都会执行) ---
        cleanup_resources(log_listener)

if __name__ == "__main__":
    mp.freeze_support() # Windows 下打包可能需要
    main()