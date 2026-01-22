import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List
import multiprocessing as mp
from typing import Tuple
from tqdm import tqdm

from .raw_extract import extract_mda_from_pdf2
import pdf_reader.config as cg
from .util.SysException import *


# 假设这是你的具体业务逻辑函数
def do_work(batch_files: List[Path]) -> Tuple[int, int]:
    """
    消费者：处理一个 batch 的文件
    """
    success_count = 0
    bs = len(batch_files)
    cg.sys_logger.info(f"开始处理 Batch，包含 {bs} 个文件")

    for file_path in batch_files:
        # 这里执行具体的 PDF 解析或其他 IO/CPU 密集型操作
        # print(f"Processing: {file_path.name}")
        result = extract_mda_from_pdf2(file_path)
        success_count += result

    # 根据需求，这里不需要返回值

    return success_count, bs


def main():
    """
    TODO 急需重构！！！！
    """
    # 0. 启动日志模块
    log_queue = mp.Queue(-1)
    log_listener = cg.setup_listener(log_queue)

    # 1. 指定读入目录 (Source)
    input_dir = cg.PATH["src"]

    # 2. 指定输出目录 (Destination) - 如果需要的话，可以在 do_work 中使用
    output_dir = cg.PATH["out"]

    # 确保目录存在
    if not input_dir.exists():
        cg.sys_logger.error(f"Error: 输入目录 {input_dir} 不存在")
        return
    output_dir.mkdir(parents=True, exist_ok=True)

    # 3. 从读入目录中递归地读取每一个文件的 Path() 对象，存储到列表中
    # rglob('*') 实现递归查找，is_file() 确保只添加文件而不是文件夹
    all_files = [p for p in input_dir.rglob("*.pdf") if p.is_file()]

    total_files = len(all_files)
    cg.sys_logger.info(f"扫描完成，共找到 {total_files} 个文件")

    # 配置 Batch Size
    BATCH_SIZE = cg.IO["batch_size"]  # 根据实际文件大小和内存情况调整

    total_success = 0


    try:
        with ProcessPoolExecutor(
            max_workers=cg.ENV["cpu"],  # CPU 核心数
            initializer=cg.setup_root_logger,
            initargs=(log_queue,),
        ) as executor:

            # --- 生产者逻辑 ---
            # 唯一的一个生产者（Main Thread）从列表中拿出一个 batch
            # 接着将这个 batch 送给 do_work() 函数去执行

            futures = []
            for i in range(0, total_files, BATCH_SIZE):
                # 切片操作，获取一个 batch
                batch = all_files[i : i + BATCH_SIZE]

                # 提交任务给进程池
                future = executor.submit(do_work, batch)
                futures.append(future)

            cg.sys_logger.info(f"已提交 {len(futures)} 个 Batch 任务到进程池...")

            # --- 修改 2: 使用 tqdm 和 as_completed 更新进度 ---
            # unit="file" 显示单位为文件
            # ncols=100 设置进度条宽度
            with tqdm(total=total_files, unit="file", ncols=100, mininterval=1.0) as pbar:

                # as_completed 会在任意一个子进程任务完成时立即 yield
                for future in as_completed(futures):
                    try:
                        # 获取子进程返回值
                        success, batch_len = future.result()

                        # 更新总成功数
                        total_success += success

                        # 更新进度条 (前进 batch_len 步)
                        # set_postfix 可以实时显示额外的统计信息（如当前成功率）
                        pbar.update(batch_len)
                        pbar.set_postfix(success=total_success, fail=pbar.n - total_success)

                    except Exception as e:
                        # 捕获 Unexpected EOF 或其他进程级崩溃
                        error_msg = str(e)
                        if "Unexpected EOF" in error_msg:
                            tqdm.write(f"CRITICAL: 子进程崩溃 (可能由 OOM 或 C底层错误引起)")
                        else:
                            tqdm.write(f"Batch Error: {e}")
                        cg.sys_logger.error(f"\nBatch 执行异常: {e}\n")
                        # 即使异常，也要更新进度条防止卡住，或者做其他处理
                        pbar.update(BATCH_SIZE)

        # with 语句结束后，会自动等待所有子进程执行完毕 (executor.shutdown(wait=True))
    except KeyboardInterrupt:
        executor.shutdown(wait=False, cancel_futures=True)
    except UnknownException as e:
        cg.sys_logger.error(f"{e.code}: {e.message}")
    finally:
        log_listener.stop()


if __name__ == "__main__":
    # 在 Windows 和 macOS (spawn 模式) 下，多进程代码必须放在 main 块保护下
    main()
