from pathlib import Path
import os
import atexit
from typing import List

import pdf_reader.config.config as cg


class BufferedWriter:
    def __init__(self, filepath: Path, buffer_size: int = cg.IO["buffer_size"]):
        self.original_path = filepath
        self.buffer_size = buffer_size
        self.buffer: List[str] = []
        self.current_buffer_len = 0

        pid = os.getpid()
        # 实际写入路径变为: data.txt -> data.txt.12345
        # 这样每个进程只写自己的文件，操作系统不需要处理任何锁竞争
        self.real_path = self.original_path.with_name(
            f"{self.original_path.name}.{pid}"
        )

        # 预先打开文件句柄，避免每次 flush 重复 open/close 的系统调用开销
        # buffering=buffer_size: 让 Python 内置的 IO 也做一层缓冲
        filepath.parent.mkdir(parents=True, exist_ok=True)
        self._f = self.real_path.open("a", encoding="utf-8", buffering=self.buffer_size)

        # 注册退出时的清理函数，防止程序崩溃导致最后一段 buffer 没写入
        atexit.register(self.close)

    def write(self, data: str):
        assert not self._f.closed

        self.buffer.append(data)
        # 维护一个长度计数器，比 len(self.buffer) 稍微快一点点（极值优化）
        # 或者估算字符串字节数
        self.current_buffer_len += len(data)

        if self.current_buffer_len >= self.buffer_size:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return

        # 使用 join 一次性拼接，比循环 write 快得多
        chunk = "".join(self.buffer)
        self._f.write(chunk)

        # 清空缓冲区
        self.buffer.clear()
        self.current_buffer_len = 0

    def close(self):
        """显式关闭资源"""
        if not self._f.closed:
            self._flush()
            self._f.flush()
            self._f.close()
