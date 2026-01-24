from .Pipeline import PipelineContext, PipelineStage
from .StandardProcess import run_processing_module
from ..util.SysException import *
from ..config import setup_listener, read_log, setup_root_logger

from pathlib import Path
import multiprocessing as mp
from typing import cast


class BatchCorruptRetry(PipelineStage):
    def process(self, stage_context: PipelineContext) -> PipelineContext:
        if stage_context.done:
            return PipelineContext(None, True)

        input_file = cast(Path, stage_context.result)
        error_map = read_log(input_file)
        todo_list = error_map.get(ErrorCode.BATCH_CORRUPT.code, [])
        if len(todo_list) == 0:
            print("所有文件均已按时解析，无需进行超时重试")
            return PipelineContext(input_file, False)

        print("开始超时重试阶段")
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
            print("提交下一阶段")
        return PipelineContext(log_path, done)
