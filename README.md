## 初始化

```sh
uv sync
```

## 运行

```sh
# 一种方式
uv run src/pdf_reader/main.py
# 更推荐下面一种
uv run -m pdf_reader.main
```

## 测试

```sh
uv run pytest
```
