from pathlib import Path
import multiprocessing as mp
from typing import cast

from .Pipeline import PipelineContext, PipelineStage
from .StandardProcess import run_processing_module
from ..util.SysException import *
from ..config import setup_listener, read_log, setup_root_logger


class LargerRangeTrial(PipelineStage):
    def process(self, stage_context: PipelineContext) -> PipelineContext:
        if stage_context.done:
            return PipelineContext(None, True)

        input_file = cast(Path, stage_context.result)
        error_map = read_log(input_file)
        todo_list = error_map.get(ErrorCode.NO_START.code, [])
        if len(todo_list) == 0:
            print("所有文件均找到起始位置，跳过扩页查找阶段")
            return PipelineContext(input_file, False)

        print("开始扩页查找阶段")
        todo_list = [Path(p.strip()) for p in todo_list]
        conf = self.get_stage_config()
        print("当前参数为: ")
        conf.display()

        log_path = conf.logs / conf.logfile
        log_queue = mp.Queue(-1)
        log_listener = setup_listener(log_path, log_queue)
        setup_root_logger(log_queue)

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
            log_merge_batch_corrupt_larger_range(
                cast(Path, stage_context.result), log_path
            )
            print("提交下一阶段")
        return PipelineContext(log_path, done)


import pandas as pd
from pathlib import Path


def log_merge_batch_corrupt_larger_range(log_path1: Path, log_path2: Path):
    """
    按照指定逻辑合并两个日志文件：
    1. 读取两个日志文件。
    2. 提取 log1 中错误码为 303 的记录。
    3. 用 log2 中的记录更新上述 303 记录（若 log2 中存在），否则丢弃。
    4. 将 log1 中非 303 的记录与更新后的记录合并。
    """

    # --- 1. 读取数据 (a) & (b) ---
    # 定义读取辅助函数，保持与 read_log 相同的读取逻辑
    def _read_df(path: Path) -> pd.DataFrame:
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame()  # 返回空 DataFrame 以防止报错
        return pd.read_csv(path, header=None)

    data1 = _read_df(log_path1)
    data2 = _read_df(log_path2)

    # 边界情况处理：如果文件1为空，直接返回空（或者视需求返回data2，这里假设基准是data1）
    if data1.empty:
        return pd.DataFrame()

    # --- 2. 数据筛选与过滤 ---

    # (a) 从 data1 中筛选出错误码（索引6）为 404 的项 -> data3
    # 同时我们需要保留非 404 的部分用于最后合并，暂存为 data1_preserved
    is_303 = data1[6] == ErrorCode.NO_START.code
    data3 = data1[is_303].copy()
    data1_preserved = data1[~is_303].copy()

    # (b) 使用 data2 按照文件名（索引5）过滤 data3
    # 逻辑解析：
    # - "如果 data3 的某一项在 data2 中，则使用 data2 的对应项替换它"
    # - "如果不在 data2 中，则从 data3 中删除该项"
    #
    # 这在逻辑上等同于：取 data2 中那些 "文件名出现在 data3" 中的行。
    # 这样做既完成了"替换"（直接拿了 data2 的行），也完成了"删除"（没拿 data2 中不存在的）。

    if not data2.empty:
        # 获取 data3 中所有的文件名集合
        target_files = set(data3[5])

        # 筛选 data2：保留那些文件名在 data3 (303列表) 中的行 -> data4
        data4 = data2[data2[5].isin(target_files)].copy()
    else:
        # 如果 data2 为空，无法替换且需删除所有 data3 项，则 data4 为空
        data4 = pd.DataFrame(columns=data1.columns)

    # --- 3. 合并与返回 ---

    # (a) 将 data1 中除 data3 以外的部分 (data1_preserved) 与 data4 合并 -> data5
    data5 = pd.concat([data1_preserved, data4], ignore_index=True)

    # (b) 将结果保存到文件中
    data5.to_csv(log_path2, index=False, header=False)
