import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from typing import List
import logging
import multiprocessing as mp

from .extract_mda import PDF_DIR, OUTPUT_DIR, extract_mda_from_pdf
import pdf_reader.config as cg


# 假设这是你的具体业务逻辑函数
def do_work(batch_files: List[Path]):
    """
    消费者：处理一个 batch 的文件
    """
    cg.sys_logger.info(f"开始处理 Batch，包含 {len(batch_files)} 个文件")

    for file_path in batch_files:
        # 这里执行具体的 PDF 解析或其他 IO/CPU 密集型操作
        # print(f"Processing: {file_path.name}")
        extract_mda_from_pdf(file_path)

    # 根据需求，这里不需要返回值

    return


def main():
    # 0. 启动日志模块
    logging.info("加载日志模块...")
    m = mp.Manager()
    log_queue = m.Queue()
    log_listener = cg.setup_listener(log_queue)
    logging.info("日志模块加载完成")

    # 1. 指定读入目录 (Source)
    input_dir = PDF_DIR

    # 2. 指定输出目录 (Destination) - 如果需要的话，可以在 do_work 中使用
    output_dir = OUTPUT_DIR

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

    # 4. 创建一个进程池 ProcessPoolExecutor
    # max_workers 如果不填，默认是 CPU 核心数。对于 IO 密集型混合任务，可以适当调大。
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

        # with 语句结束后，会自动等待所有子进程执行完毕 (executor.shutdown(wait=True))
        cg.sys_logger.info("所有任务执行完毕")
    finally:
        log_listener.stop()
        m.shutdown()


if __name__ == "__main__":
    # 在 Windows 和 macOS (spawn 模式) 下，多进程代码必须放在 main 块保护下
    main()
