import pandas as pd
from pathlib import Path
from .. import config as cg


def clean_log(log_path: Path=Path(cg.PROJECT_ROOT / "logs" / "sys.csv")) -> dict:
    # 1. 读取日志文件
    # 假设列之间通过逗号分隔且没有标题行
    df = pd.read_csv('sys.csv', header=None)

    # 2. 清洗数据：以倒数第二列（索引 5）为准，保留最后一条记录
    # subset=5 表示文件名列，keep='last' 确保保留最新记录
    df_cleaned = df.drop_duplicates(subset=5, keep='last')

    # 3. 按照错误码（最后一列，索引 6）进行分类
    # 将结果转换为字典，Key 是错误码，Value 是文件名的列表
    error_map = df_cleaned.groupby(6)[5].apply(list).to_dict()

    # 现在 error_map 就是您需要的字典
    return error_map
