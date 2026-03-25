from enum import Enum
from pathlib import Path


class ErrorCode(Enum):
    """NAME = (CODE, MESSAGE)"""

    SUCCESS = (0, "Success")
    UNREADABLE = (101, "Unreadable")
    NO_CHINESE = (202, "Not find valid character, need OCR")
    NO_START = (303, "Not find start index")
    NO_ENDING = (304, "Not find ending index")
    TIMEOUT = (404, "Extraction timeout")
    UNKNOWN_ERROR = (1000, "Unknown error occurred")

    @property
    def code(self):
        return self.value[0]

    @property
    def msg(self):
        return self.value[1]


class SysException(BaseException):
    def __init__(self, error_enum: ErrorCode):
        self.code = error_enum.code
        self.name = error_enum.name
        self.message = error_enum.msg
        super().__init__(self.message)


# --- TEXT UTILS ---

CN_NUM = {
    "零": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
}


def chinese_to_number(s):
    if not s:
        return 0
    if s.isdigit():
        return int(s)
    if s == "十":
        return 10
    if len(s) == 2 and s.startswith("十"):
        return 10 + CN_NUM.get(s[1], 0)
    if len(s) == 2 and s.endswith("十"):
        return CN_NUM.get(s[0], 1) * 10
    if len(s) == 3 and s[1] == "十":
        return CN_NUM.get(s[0], 0) * 10 + CN_NUM.get(s[2], 0)
    return CN_NUM.get(s, 0)


def number_to_chinese(n):
    if n <= 10:
        return list(CN_NUM.keys())[list(CN_NUM.values()).index(n)]
    if n < 20:
        return "十" + list(CN_NUM.keys())[list(CN_NUM.values()).index(n - 10)]
    if n < 100:
        tens = n // 10
        units = n % 10
        res = list(CN_NUM.keys())[list(CN_NUM.values()).index(tens)] + "十"
        if units > 0:
            res += list(CN_NUM.keys())[list(CN_NUM.values()).index(units)]
        return res
    return str(n)


def get_file_size_mb(file_path: Path):
    """
    Get file size in MB
    """
    return file_path.stat().st_size / (1024 * 1024)


def clean_special_chars(text):
    if not text:
        return ""
    return "".join(ch for ch in text if ch.isprintable() or ch in "\n\t")


def remove_duplicate_chars(text):
    if not text:
        return ""
    result = []
    if len(text) > 0:
        result.append(text[0])
    for i in range(1, len(text)):
        char = text[i]
        if char != text[i - 1]:
            result.append(char)
        elif ("a" <= char <= "z") or ("A" <= char <= "Z") or ("0" <= char <= "9"):
            result.append(char)
    return "".join(result)
