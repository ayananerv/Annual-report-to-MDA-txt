import pandas as pd
import numpy as np
from pathlib import Path
import sys

def analyze_pdf_sizes(dir_path: str, output_csv: str = "pdf_sizes.csv"):
    """
    递归统计文件夹内全部 PDF 文件大小，并利用统计学方法确定阈值。
    """
    base_path = Path(dir_path)
    if not base_path.exists():
        print(f"错误: 路径 '{dir_path}' 不存在。")
        return

    print(f"正在扫描目录: {base_path} ...")

    # 1. 递归获取所有 PDF 文件信息
    data = []
    # rglob("*.pdf") 会递归匹配所有子目录下的 pdf 文件
    pdf_files = list(base_path.rglob("*.pdf"))

    if not pdf_files:
        print("未在指定目录下找到任何 PDF 文件。")
        return

    for pdf_file in pdf_files:
        try:
            # 获取字节大小并转换为 MB
            size_mb = pdf_file.stat().st_size / (1024 * 1024)
            data.append({
                "filename": pdf_file.name,
                "path": str(pdf_file),
                "size_mb": size_mb
            })
        except Exception as e:
            print(f"无法读取文件 {pdf_file}: {e}")

    # 2. 转换为 DataFrame 并保存
    df = pd.DataFrame(data)
    df.to_csv(output_csv, index=False)
    print(f"统计数据已保存至: {output_csv}")

    # 3. 统计学分析
    sizes = df['size_mb']

    # 计算分位数
    q1 = sizes.quantile(0.25)
    q3 = sizes.quantile(0.75)
    median = sizes.median()
    mean = sizes.mean()
    iqr = q3 - q1

    # 计算统计学建议阈值
    # IQR 准则：Q3 + 1.5 * IQR (常用于识别离群大文件)
    iqr_threshold = q3 + 1.5 * iqr
    p95 = sizes.quantile(0.95)
    p99 = sizes.quantile(0.99)

    # 4. 打印报告
    print("\n" + "="*40)
    print("      PDF 文件大小统计报告")
    print("="*40)
    print(f"文件总数    : {len(sizes)}")
    print(f"最小大小    : {sizes.min():.4f} MB")
    print(f"最大大小    : {sizes.max():.4f} MB")
    print(f"平均大小    : {mean:.4f} MB")
    print(f"中位数      : {median:.4f} MB")
    print("-" * 40)
    print(f"25% 分位数 (Q1): {q1:.4f} MB")
    print(f"75% 分位数 (Q3): {q3:.4f} MB")
    print(f"四分位距 (IQR) : {iqr:.4f} MB")
    print("-" * 40)
    print(f"【建议阈值建议】")
    print(f"1. 离群值判定 (Q3 + 1.5*IQR): {iqr_threshold:.2f} MB")
    print(f"   (注：超过此值通常被认为是异常庞大的文件，可能包含大量图片或损坏)")
    print(f"2. P95 分位数 (覆盖95%文件): {p95:.2f} MB")
    print(f"3. P99 分位数 (覆盖99%文件): {p99:.2f} MB")
    print("="*40)

    # 给出最终工程建议
    recommendation = min(iqr_threshold, p99)
    print(f"\n工程建议：若需兼顾效率与覆盖率，建议将阈值设为 {recommendation:.1f} MB。")

if __name__ == "__main__":
    # 你可以在这里直接修改路径，或者通过命令行传入
    target_dir = "../assets/pdf" # 示例路径

    if len(sys.argv) > 1:
        target_dir = sys.argv[1]

    analyze_pdf_sizes(target_dir, "./pdf_size.csv")