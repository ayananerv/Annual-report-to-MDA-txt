from enum import Enum, unique


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
