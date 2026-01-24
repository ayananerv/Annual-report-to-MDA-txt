from dataclasses import dataclass
from typing import List, Any
from pathlib import Path


@dataclass
class PipelineContext:
    result: Path | None
    done: bool


from abc import ABC, abstractmethod

from ..config import JobConfig


class PipelineStage(ABC):
    def __init__(self, override_conf: dict | None = None):
        """
        Docstring for __init__

        :param self: Description
        :param override_conf: 自定义该阶段的配置项，否则采用默认
        :type override_conf: dict | None
        """
        self.config_overrides = override_conf or {}

    @abstractmethod
    def process(self, stage_context: PipelineContext) -> PipelineContext:
        pass

    def get_stage_config(self) -> JobConfig:
        return JobConfig.from_defaults(self.config_overrides)

    def check_done(self, log_path: Path):
        return not log_path.exists() or log_path.stat().st_size == 0
