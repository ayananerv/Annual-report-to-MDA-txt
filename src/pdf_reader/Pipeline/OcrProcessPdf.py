from pathlib import Path
import multiprocessing as mp
from typing import cast

from .Pipeline import PipelineContext, PipelineStage
from ..util.SysException import *
from ..config import setup_listener, read_log, setup_root_logger
from ..ocr import process_pdfs


class OcrProcessPdf(PipelineStage):
    def process(self, stage_context: PipelineContext) -> PipelineContext:
        if stage_context.done:
            return PipelineContext(None, True)

        input_file = cast(Path, stage_context.result)
        error_map = read_log(input_file)
        todo_list = error_map.get(ErrorCode.NO_CHINESE.code, [])
        if len(todo_list) == 0:
            print("所有文件均可识别，跳过OCR识别阶段")
            return PipelineContext(input_file, True)

        print("开始OCR识别阶段")
        todo_list = [Path(p.strip()) for p in todo_list]
        conf = self.get_stage_config()
        print("当前参数为: ")
        conf.display()

        process_pdfs(todo_list, conf)

        return PipelineContext(None, False)
