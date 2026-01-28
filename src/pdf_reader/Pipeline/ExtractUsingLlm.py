from pathlib import Path
import multiprocessing as mp
from typing import cast

from .Pipeline import PipelineContext, PipelineStage
from ..util.SysException import *
from ..config import setup_listener, read_log, setup_root_logger
from ..llm import extract_using_llm


class ExtractUsingLlm(PipelineStage):
    def process(self, stage_context: PipelineContext) -> PipelineContext:
        if stage_context.done:
            return PipelineContext(None, True)

        input_file = cast(Path, stage_context.result)
        error_map = read_log(input_file)
        todo_list = error_map.get(ErrorCode.NO_START.code, [])
        if len(todo_list) == 0:
            print("所有文件均找到起始位置，跳过大模型解析阶段")
            return PipelineContext(input_file, False)

        print("开始大模型解析阶段")
        todo_list = [Path(p.strip()) for p in todo_list]
        conf = self.get_stage_config()
        print("当前参数为: ")
        conf.display()

        done = extract_using_llm(todo_list, conf)

        if done:
            print("所有文件均成功解析")
        else:
            print("进行下一阶段")
        return PipelineContext(input_file, False)
