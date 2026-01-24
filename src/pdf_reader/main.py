from typing import List, Tuple
from pathlib import Path
import multiprocessing as mp
import logging
import logging.handlers
from warnings import deprecated

from .config import JobConfig, sys_logger
from .config import setup_listener, setup_root_logger


# ==========================================
# 模块 1: 环境配置与初始化
# ==========================================
def init_environment(conf: JobConfig) -> None:
    """
    初始化日志系统，检查文件路径权限。
    返回: (日志队列, 日志监听器, 输入路径, 输出路径)
    """
    # 1. 路径检查
    input_dir = conf.input_dir
    output_dir = conf.output_dir

    if not input_dir.exists():
        raise FileNotFoundError(f"输入目录不存在: {input_dir}")

    # 确保输出目录存在
    output_dir.mkdir(parents=True, exist_ok=True)


# ==========================================
# 模块 3: 失败重试 (预留接口)
# ==========================================
def run_retry_module():
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
@deprecated("Not use anymore")
def cleanup_resources(log_listener: logging.handlers.QueueListener):
    """
    安全停止日志监听器，防止程序退出时报错。
    """
    print("正在清理资源...")
    log_listener.stop()
    # print("资源释放完成。")


from .Pipeline import *


# ==========================================
# 主编排函数 (Orchestrator)
# ==========================================
def main():
    default_conf = JobConfig.from_defaults()
    ctx = PipelineContext(None, False)

    pipeline: List[PipelineStage] = [
        StandardProcess(),
        BatchCorruptRetry(
            {
                "batch_size": 1,
                "timeout": 20,
                "logfile": "batch_corrupt_retry.csv",
            }
        ),
        LargerRangeTrial(
            {
                "search_page_range": (24, 60),
                "logfile": "larger_range_trail.csv",
            }
        ),
    ]

    for p in pipeline:
        ctx = p.process(ctx)


if __name__ == "__main__":
    mp.freeze_support()  # Windows 下打包可能需要
    main()
