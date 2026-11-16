#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
知识库质量与完整性自动化检查脚本 (verify_knowledge_base.py)
用于自动扫描知识库中的 Markdown 文档：
1. 验证 YAML Frontmatter 元数据完整性
2. 验证技术文档模板结构（12项核心段落）
3. 检查内部 Markdown 相对链接有效性，防止死链
4. 统计学习进度状态分布（未学习 / 学习中 / 需要巩固 / 已掌握）
"""

import os
import re
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

# 技术文档必须包含的 12 个核心段落关键词
REQUIRED_TECH_SECTIONS = [
    "1. 知识概述",
    "2. 核心概念与关系",
    "3. 底层原理",
    "4. 核心实现与源码分析",
    "5. 工程实践与落地",
    "6. 常见生产问题与排查指南",
    "7. 技术选型对比与取舍",
    "8. 面试高频问题",
    "9. 标准参考答案",
    "10. 知识自检清单",
    "11. 知识脉络与上下游关联",
    "12. 权威参考资料与原始出处",
]

# 必须的 Frontmatter 键
REQUIRED_FRONTMATTER_KEYS = [
    "id",
    "title",
    "module",
    "status",
    "importance",
]

VALID_STATUSES = {"未学习", "学习中", "需要巩固", "已掌握"}

def parse_frontmatter(content: str):
    """提取 Markdown 文档的 YAML Frontmatter"""
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n", content, re.DOTALL)
    if not match:
        return None, content
    fm_text = match.group(1)
    body = content[match.end():]
    
    fm_dict = {}
    for line in fm_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            # 去除引号和注释
            v_clean = v.split("#")[0].strip().strip('"').strip("'")
            fm_dict[k.strip()] = v_clean
    return fm_dict, body

def check_relative_links(file_path: Path, content: str):
    """检查文档内的 Markdown 相对链接是否有效"""
    errors = []
    # 匹配 [text](relative_path) 格式，排除 http://, https://, mailto:, #
    links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', content)
    for text, target in links:
        target = target.strip()
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        # 去除锚点
        target_path_str = target.split("#")[0]
        if not target_path_str:
            continue
        
        # 解析相对路径
        target_path = (file_path.parent / target_path_str).resolve()
        if not target_path.exists():
            errors.append(f"无效相对链接: [{text}]({target}) -> 目标文件不存在")
    return errors

def verify_tech_doc(file_path: Path, content: str, fm: dict):
    """检查技术知识点文档的结构完整性"""
    warnings = []
    # 检查 Frontmatter 字段
    for key in REQUIRED_FRONTMATTER_KEYS:
        if key not in fm:
            warnings.append(f"缺少 Frontmatter 核心字段: '{key}'")
    
    if fm.get("status") not in VALID_STATUSES:
        warnings.append(f"非法状态值: '{fm.get('status')}' (允许: {VALID_STATUSES})")
    
    # 检查 12 个核心段落
    for sec in REQUIRED_TECH_SECTIONS:
        # 允许标题有些许前后空格或二级/三级标题形式
        sec_pattern = re.escape(sec.split(" ")[0]) + r"\s*" + re.escape(sec.split(" ", 1)[1])
        if not re.search(sec_pattern, content):
            warnings.append(f"缺少核心规范段落: '{sec}'")
            
    return warnings

def main():
    print("=" * 60)
    print(">>> 启动技术管理知识库质量与完整性自动化检查")
    print("=" * 60)

    # 扫描包含知识点的模块目录
    target_modules = [
        "01-bigdata",
        "02-data-platform",
        "03-java-engineering",
        "04-tech-management",
        "05-project-management",
        "06-ai-agent",
        "07-project-review",
    ]

    total_docs = 0
    status_counter = {"未学习": 0, "学习中": 0, "需要巩固": 0, "已掌握": 0, "未知": 0}
    all_issues = []

    for mod in target_modules:
        mod_dir = ROOT_DIR / mod
        if not mod_dir.exists():
            continue
        
        for md_file in mod_dir.rglob("*.md"):
            # 排除 README.md 或模板
            if md_file.name in ["README.md", "SUMMARY.md"]:
                continue
            
            total_docs += 1
            rel_path = md_file.relative_to(ROOT_DIR)
            content = md_file.read_text(encoding="utf-8")
            
            fm, body = parse_frontmatter(content)
            if not fm:
                all_issues.append((str(rel_path), ["未检测到 YAML Frontmatter 元数据"]))
                continue
            
            st = fm.get("status", "未知")
            status_counter[st if st in status_counter else "未知"] += 1
            
            file_issues = []
            
            # 若是 01-bigdata / 02-data-platform / 03-java-engineering 技术文档，校验12大段落
            if mod in ["01-bigdata", "02-data-platform", "03-java-engineering"]:
                tech_warns = verify_tech_doc(md_file, content, fm)
                file_issues.extend(tech_warns)
            
            # 链接检查
            link_errs = check_relative_links(md_file, content)
            file_issues.extend(link_errs)
            
            if file_issues:
                all_issues.append((str(rel_path), file_issues))

    print(f"\n[统计概览]")
    print(f"扫描知识文档总数: {total_docs}")
    for st, count in status_counter.items():
        print(f"  - {st}: {count}")

    if all_issues:
        print("\n" + "!" * 60)
        print(f"[质量告警] 共在 {len(all_issues)} 个文件中发现潜在规范问题:")
        for path, issues in all_issues:
            print(f"\n文件: {path}")
            for iss in issues:
                print(f"  ✖ {iss}")
        print("!" * 60)
        sys.exit(1)
    else:
        print("\n✔ [检查通过] 所有已创建知识文档均符合质量规范，元数据与链接完整无误！")
        sys.exit(0)

if __name__ == "__main__":
    main()
