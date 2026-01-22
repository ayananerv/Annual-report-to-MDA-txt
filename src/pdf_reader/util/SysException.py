from enum import Enum, unique

class ErrorCode(Enum):
    """ NAME = (CODE, MESSAGE) """
    SUCCESS = (0, "Success")
    UNREADABLE = (202, "Unreadable, need OCR")
    NO_CHINESE = (303, "Cannot find valid charactor")
    NO_START = (440, "Cannot find start index")
    NO_ENDING = (505, "Cannot find ending index")
    UNKNOWN_ERROR = (1000, "Unknown error occured")


    @property
    def code(self):
        return self.value[0]

    @property
    def msg(self):
        return self.value[1]


class SysException(RuntimeError):
    def __init__(self, error_enum: ErrorCode):
        self.code = error_enum.code
        self.name = error_enum.name
        self.message = error_enum.msg
        super().__init__(self.message)


class UnknownException(RuntimeError):
    def __init__(self, error_enum: ErrorCode, e: Exception):
        self.code = error_enum.code
        self.name = error_enum.name
        self.message = str(e)
        super().__init__(e)