from pathlib import Path


def save_text_to_file(text: str, result_txt_path: Path):
    if not text:
        return
    result_txt_path.parent.mkdir(parents=True, exist_ok=True)
    result_txt_path.write_text(text, encoding="utf-8")
