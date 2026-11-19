#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
知识库质量、引用与事实核验自动化检查脚本 (verify_knowledge_base.py)
用于自动扫描知识库中的 Markdown 文档：
1. 结构质量检查 (Structural Quality):
   - 验证 YAML Frontmatter 元数据完整性（包含 doc_status 与 learning_status 双状态）
   - 验证技术文档 12 项核心段落规范
   - 验证大厂真题与参考答案 100% 对应及三层应答结构
2. 引用与路径检查 (Reference & Path Integrity):
   - 检查文档内 Markdown 相对链接有效性，防止死链
   - 严格拦截任何开发机本地绝对路径（如 file:///Users/... 或 /Users/...），保障跨平台与远程仓库可用性
3. 事实核验与隔离合规检查 (Fact Verification & Isolation Advisory):
   - 扫描个人履历关联内容（瑞纳智能、太原项目、思科、美的、Zoom）
   - 验证是否按规范挂载案例隔离声明或待核实标记（PROFILE_FACTS.md）
"""

import os
import re
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

# 技术文档必须包含的 12 个核心段落关键词
REQUIRED_TECH_SECTIONS = [
    "1. 知识概述与业务价值",
    "2. 核心概念与关系",
    "3. 底层工作机制与设计哲学",
    "4. 高频复杂业务场景实操与落地",
    "5. 生产异常故障深度排查与根因分析",
    "6. 技术选型对比",
    "7. 技术经理与架构师实战管控",
    "8. 互联网大厂高频面试真题库",
    "9. 结构化深度参考答案",
    "10. 知识自检与口述推演清单",
    "11. 知识脉络与技术图谱关联",
    "12. 权威参考与官方进阶资料",
]

# AI 数据工程模块 (10-ai-data-engineering/01~08) 必须包含的 12 个核心段落关键词
REQUIRED_AI_DATA_SECTIONS = [
    "1. 核心概念",
    "2. 解决的问题",
    "3. 底层原理",
    "4. 系统架构",
    "5. 关键算法或实现机制",
    "6. 技术选型与替代方案",
    "7. 工程实践",
    "8. 常见故障与解决思路",
    "9. 性能及成本分析",
    "10. 面试题",
    "11. 参考答案",
    "12. 官方资料与源码参考",
]

# 核心 Frontmatter 键
REQUIRED_FRONTMATTER_KEYS = [
    "id",
    "title",
    "module",
    "importance",
]

VALID_DOC_STATUSES = {"已完成", "编写中", "草稿"}
VALID_LEARNING_STATUSES = {"未学习", "学习中", "需要巩固", "已掌握"}

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
            v_clean = v.split("#")[0].strip().strip('"').strip("'")
            fm_dict[k.strip()] = v_clean
    return fm_dict, body

def check_path_and_links(file_path: Path, content: str):
    """检查文档内的绝对路径硬编码与 Markdown 相对链接有效性"""
    errors = []
    
    # 1. 严格检查本机绝对路径硬编码 (file:///Users/... 或 /Users/...)
    abs_matches = re.findall(r'(file:///Users/[^\s\)\"\'>]+|/Users/[a-zA-Z0-9_\-]+/[^\s\)\"\'>]+)', content)
    for m in abs_matches:
        errors.append(f"发现硬编码本机绝对路径: '{m}' (必须替换为标准相对路径或通用 URI)")
        
    # 2. 检查 Markdown 链接有效性 [text](target)
    links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', content)
    for text, target in links:
        target = target.strip()
        # 排除外部网络链接、邮件或锚点
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        # 去除链接内的锚点
        target_path_str = target.split("#")[0]
        if not target_path_str:
            continue
        
        # 解析相对路径
        target_path = (file_path.parent / target_path_str).resolve()
        if not target_path.exists():
            errors.append(f"无效相对链接: [{text}]({target}) -> 目标文件不存在")
            
    return errors

def verify_structural_quality(file_path: Path, content: str, fm: dict, is_tech_module: bool):
    """检查文档的结构质量、字段合规性与真题解答覆盖率"""
    errors = []
    
    # 检查 Frontmatter 必填字段
    for key in REQUIRED_FRONTMATTER_KEYS:
        if key not in fm:
            errors.append(f"缺少 Frontmatter 核心字段: '{key}'")
            
    # 检查双状态机制
    doc_st = fm.get("doc_status")
    if doc_st and doc_st not in VALID_DOC_STATUSES:
        errors.append(f"非法文档编写状态: '{doc_st}' (允许: {VALID_DOC_STATUSES})")
        
    learn_st = fm.get("learning_status", fm.get("status"))
    if learn_st and learn_st not in VALID_LEARNING_STATUSES:
        errors.append(f"非法学习进度状态: '{learn_st}' (允许: {VALID_LEARNING_STATUSES})")
        
    # 如果是技术知识点文档，检查 12 大段落规范
    if is_tech_module:
        for sec in REQUIRED_TECH_SECTIONS:
            sec_pattern = re.escape(sec.split(" ")[0]) + r"\s*" + re.escape(sec.split(" ", 1)[1])
            if not re.search(sec_pattern, content):
                errors.append(f"缺少核心规范段落: '{sec}'")
                
        # 深度检查真题与参考答案对应性
        sec8_match = re.search(r"## 8\.\s*互联网大厂高频面试真题库(.*?)(?=## 9\.|$)", content, re.DOTALL)
        sec9_match = re.search(r"## 9\.\s*结构化深度参考答案(.*?)(?=## 10\.|$)", content, re.DOTALL)
        
        if sec8_match and sec9_match:
            sec8_text = sec8_match.group(1)
            sec9_text = sec9_match.group(1)
            
            q_list = re.findall(r"\*\*Q(\d+)\*\*", sec8_text)
            ans_list = re.findall(r"###\s*题目(\d+)", sec9_text)
            
            if len(q_list) > 0 and len(ans_list) < len(q_list):
                errors.append(f"真题答案覆盖不全: 第8节列出 {len(q_list)} 道真题，但第9节仅解答了 {len(ans_list)} 道 (必须题题有解)")
                
            if "#### 简洁回答" not in sec9_text or "#### 深入回答" not in sec9_text:
                errors.append("第9节答案缺少标准三层回答结构 (必须包含简洁回答、深入回答与架构师视角)")
                
    # 如果是 AI 数据工程核心专题文档 (01~08)，校验用户指定的 12 大段落规范
    if "10-ai-data-engineering" in str(file_path) and re.match(r"^0[1-8]-", file_path.name):
        for sec in REQUIRED_AI_DATA_SECTIONS:
            sec_pattern = re.escape(sec.split(" ")[0]) + r"\s*" + re.escape(sec.split(" ", 1)[1])
            if not re.search(sec_pattern, content):
                errors.append(f"缺少 AI 数据工程规范段落: '{sec}'")

    return errors

def scan_fact_verification_advisory(file_path: Path, content: str):
    """扫描涉及个人经历的公司及未核实标记，输出人工事实审核提示"""
    advisories = []
    
    # 待核实标记
    unverified_tags = re.findall(r'>\s*\[!WARNING\]\s*待核实[:：](.*?)(?=\n|$)', content)
    for tag in unverified_tags:
        advisories.append(f"标记待核实项: {tag.strip()}")
        
    # 检查特定复盘、交付与教学假设文档中是否挂载 PROFILE_FACTS 隔离说明
    rel_str = str(file_path)
    if "07-project-review" in rel_str or "05-project-management" in rel_str or "10-ai-data-engineering" in rel_str:
        if "PROFILE_FACTS.md" not in content and "案例性质" not in content:
            advisories.append("项目复盘/教学案例文档未检测到案例隔离声明 (需引用 PROFILE_FACTS.md 或标注案例性质)")
            
    return advisories

def main():
    print("=" * 70)
    print(">>> 启动技术管理知识库三维质检系统")
    print("    [1. 结构规范检查 | 2. 引用与绝对路径检查 | 3. 事实核验与隔离审计]")
    print("=" * 70)

    target_modules = [
        "00-foundations",
        "01-bigdata",
        "02-data-platform",
        "03-java-engineering",
        "04-tech-management",
        "05-project-management",
        "06-ai-agent",
        "07-project-review",
        "09-review",
        "10-ai-data-engineering",
    ]

    total_docs = 0
    doc_status_counter = {"已完成": 0, "编写中": 0, "草稿": 0, "未标注": 0}
    learning_status_counter = {"未学习": 0, "学习中": 0, "需要巩固": 0, "已掌握": 0, "未知": 0}
    
    structural_errors = []
    path_link_errors = []
    fact_advisories = []

    # 扫描根目录关键 Markdown
    root_mds = [ROOT_DIR / "README.md", ROOT_DIR / "PLAN.md", ROOT_DIR / "PROGRESS.md", ROOT_DIR / "PROFILE_FACTS.md", ROOT_DIR / "AGENTS.md"]
    for r_md in root_mds:
        if r_md.exists():
            content = r_md.read_text(encoding="utf-8")
            p_errs = check_path_and_links(r_md, content)
            if p_errs:
                path_link_errors.append((str(r_md.name), p_errs))

    # 扫描模块 Markdown
    for mod in target_modules:
        mod_dir = ROOT_DIR / mod
        if not mod_dir.exists():
            continue
        
        for md_file in mod_dir.rglob("*.md"):
            if md_file.name in ["README.md", "SUMMARY.md"]:
                # 模块 README 也做链接与绝对路径校验
                rel_readme = str(md_file.relative_to(ROOT_DIR))
                p_errs = check_path_and_links(md_file, md_file.read_text(encoding="utf-8"))
                if p_errs:
                    path_link_errors.append((rel_readme, p_errs))
                continue
            
            total_docs += 1
            rel_path = str(md_file.relative_to(ROOT_DIR))
            content = md_file.read_text(encoding="utf-8")
            
            fm, body = parse_frontmatter(content)
            if not fm:
                structural_errors.append((rel_path, ["未检测到 YAML Frontmatter 元数据"]))
                continue
            
            # 统计双状态
            d_st = fm.get("doc_status", "未标注")
            doc_status_counter[d_st if d_st in doc_status_counter else "未标注"] += 1
            
            l_st = fm.get("learning_status", fm.get("status", "未知"))
            learning_status_counter[l_st if l_st in learning_status_counter else "未知"] += 1
            
            # 1. 结构质量
            is_tech = mod in ["01-bigdata", "02-data-platform", "03-java-engineering"]
            s_errs = verify_structural_quality(md_file, content, fm, is_tech)
            if s_errs:
                structural_errors.append((rel_path, s_errs))
                
            # 2. 路径与链接
            p_errs = check_path_and_links(md_file, content)
            if p_errs:
                path_link_errors.append((rel_path, p_errs))
                
            # 3. 事实核验与隔离审计
            advs = scan_fact_verification_advisory(md_file, content)
            if advs:
                fact_advisories.append((rel_path, advs))

    print(f"\n[知识资产与进度统计]")
    print(f"扫描知识文档总数: {total_docs}")
    print(f"  • 文档编写状态 (doc_status): " + ", ".join([f"{k}: {v}" for k, v in doc_status_counter.items()]))
    print(f"  • 用户实际学习状态 (learning_status): " + ", ".join([f"{k}: {v}" for k, v in learning_status_counter.items()]))

    has_fatal = False

    # 输出结构规范检查结果
    if structural_errors:
        has_fatal = True
        print("\n" + "✖" * 60)
        print(f"[结构质量告警] 共在 {len(structural_errors)} 个文件中发现结构缺陷:")
        for path, errs in structural_errors:
            print(f"\n文件: {path}")
            for e in errs:
                print(f"  ✖ {e}")
    else:
        print("\n✔ [结构质量检查通过] 所有知识文档元数据完整，核心段落与大厂真题解答 100% 覆盖。")

    # 输出路径与引用检查结果
    if path_link_errors:
        has_fatal = True
        print("\n" + "✖" * 60)
        print(f"[路径与引用告警] 共在 {len(path_link_errors)} 个文件中发现死链或绝对路径硬编码:")
        for path, errs in path_link_errors:
            print(f"\n文件: {path}")
            for e in errs:
                print(f"  ✖ {e}")
    else:
        print("✔ [路径与引用检查通过] 无失效相对链接，无本机绝对路径硬编码。")

    # 输出事实审计提示 (Advisory, 非阻断)
    if fact_advisories:
        print("\n" + "!" * 60)
        print(f"[事实核验与隔离提示] 共在 {len(fact_advisories)} 个文件中发现人工审核或隔离提示 (参考 PROFILE_FACTS.md):")
        for path, advs in fact_advisories:
            print(f"\n文件: {path}")
            for a in advs:
                print(f"  ℹ {a}")
    else:
        print("✔ [事实核验与隔离检查通过] 项目复盘与管理文档均已挂载案例性质隔离声明。")

    print("\n" + "=" * 70)
    if has_fatal:
        print(">>> 质检失败: 请修复上述结构缺陷或路径引用错误后重试。")
        sys.exit(1)
    else:
        print(">>> 质检完全通过: 知识库质量达到发布基线！")
        sys.exit(0)

if __name__ == "__main__":
    main()
