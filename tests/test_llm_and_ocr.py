from typing import List, Tuple, cast
from pathlib import Path
import multiprocessing as mp
import logging
import logging.handlers
from warnings import deprecated
import ocrmypdf


from pdf_reader.config import JobConfig, sys_logger
from pdf_reader.Pipeline import PipelineContext, PipelineStage
from pdf_reader.util.SysException import *
from pdf_reader.config import setup_listener, read_log, setup_root_logger


def process_pdfs(todo_list: List[Path], conf: JobConfig):
    # 定义输入和输出目录
    output_dir = Path("./output_ocr")

    # 如果输出文件夹不存在，则创建
    output_dir.mkdir(parents=True, exist_ok=True)

    # 获取所有 PDF 文件
    pdf_files = todo_list
    if not pdf_files:
        print(f"未找到 PDF 文件")
        return

    print(f"找到 {len(pdf_files)} 个 PDF 文件，准备处理...")

    for pdf_file in pdf_files:
        output_file = output_dir / pdf_file.name

        # 如果目标文件已存在，跳过（或者你可以选择覆盖）
        if output_file.exists():
            print(f"文件已跳过 (已存在): {output_file.name}")
            continue

        print(f"正在处理: {pdf_file.name} -> {output_file.name} ...")

        try:
            # 使用 ocrmypdf 进行 OCR 修复
            # force_ocr=True: 强制光栅化重做 OCR，解决乱码问题
            # language="chi_sim": 指定简体中文
            ocrmypdf.ocr(
                input_file=pdf_file,
                output_file=output_file,
                language="chi_sim",
                force_ocr=True,
                progress_bar=True,
            )
            print(f"✅ 处理成功: {output_file.name}")
        except Exception as e:
            print(f"❌ 处理失败 {pdf_file.name}: {e}")

            import traceback

            traceback.print_exc()


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


from pdf_reader.llm import *


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
        todo_list = todo_list[:4]
        conf = self.get_stage_config()
        print("当前参数为: ")
        conf.display()

        done = extract_using_llm(todo_list, conf)

        if done:
            print("所有文件均成功解析")
        else:
            print("进行下一阶段")
        return PipelineContext(input_file, done)


# ==========================================
# 主编排函数 (Orchestrator)
# ==========================================
def test_batch_corrupt_retry():
    default_conf = JobConfig.from_defaults()
    ctx = PipelineContext(
        Path(""),
        False,
    )

    pipeline: List[PipelineStage] = [
        OcrProcessPdf(),
    ]

    for p in pipeline:
        ctx = p.process(ctx)


if __name__ == "__main__":
    mp.freeze_support()  # Windows 下打包可能需要
    test_batch_corrupt_retry()
