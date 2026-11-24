#!/usr/bin/env python3
"""执行 SQL 复习文档中的独立查询，并核对紧随其后的 JSON 预期结果。"""

import json
import re
import sqlite3
import sys
from pathlib import Path


DOCUMENT = Path(__file__).resolve().parent.parent / "08-interview-questions/10-sql-modeling.md"


def main():
    content = DOCUMENT.read_text(encoding="utf-8")
    questions = re.findall(r"^### 题目(\d+)[^\n]*\n(.*?)(?=^### 题目|^## |\Z)",
                           content, re.MULTILINE | re.DOTALL)
    if [number for number, _ in questions] != [str(number) for number in range(1, 7)]:
        raise ValueError("SQL 文档必须包含题目 1 至 6，且顺序完整")

    for number, body in questions:
        sql_blocks = re.findall(r"^```sql\n(.*?)^```", body, re.MULTILINE | re.DOTALL)
        expected_blocks = re.findall(r"^```json\n(.*?)^```", body, re.MULTILINE | re.DOTALL)
        if len(sql_blocks) != 1 or len(expected_blocks) != 1:
            raise ValueError(f"题目 {number} 必须有一段 SQL 和一段预期 JSON")
        expected = json.loads(expected_blocks[0])
        connection = sqlite3.connect(":memory:")
        try:
            actual = [list(row) for row in connection.execute(sql_blocks[0]).fetchall()]
        finally:
            connection.close()
        if actual != expected:
            raise ValueError(f"题目 {number} 结果不符，预期 {expected!r}，实际 {actual!r}")
        print(f"题目 {number} 通过，共 {len(actual)} 行")
    print(f"6 道 SQL 结果核验通过，SQLite {sqlite3.sqlite_version}")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, sqlite3.Error, OSError) as error:
        print(f"SQL 核验失败：{error}", file=sys.stderr)
        sys.exit(1)
