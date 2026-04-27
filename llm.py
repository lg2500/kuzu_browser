# -*- coding: utf-8 -*-
"""自然语言 → Cypher（OpenAI 兼容 API）。"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from config import CYPHER_LLM_SYSTEM_PROMPT, MOCK_LLM_SYSTEM_PROMPT
from db import fetch_rel_connection_endpoints


def format_schema_for_cypher_llm(
    conn: Any,
    node_tables: List[str],
    rel_tables: List[str],
    node_fields: Dict[str, List[str]],
    rel_fields: Dict[str, List[str]],
    node_pk: Dict[str, Set[str]],
    rel_pk: Dict[str, Set[str]],
) -> str:
    """
    将当前连接库中的节点/关系表及属性整理为 Markdown，供 LLM 生成 Cypher。
    含 SHOW_CONNECTION 解析出的关系端点类型，便于 MATCH 方向正确。
    """
    lines: List[str] = [
        "## 当前数据库 Schema（Kùzu）",
        "",
        "### 节点表（MATCH 中使用标签，如 MATCH (n:`表名`)）",
    ]
    for t in node_tables:
        fields = node_fields.get(t) or []
        pks = node_pk.get(t) or set()
        if fields:
            fld = ", ".join(f"{f} (PK)" if f in pks else f for f in fields)
        else:
            fld = "（无属性或未能读取）"
        lines.append(f"- **{t}**：{fld}")
    if not node_tables:
        lines.append("- （无节点表）")
    lines.extend(["", "### 关系表（模式中为 -[:`关系表名`]->）", ""])
    for r in rel_tables:
        src, dst = fetch_rel_connection_endpoints(conn, r)
        if src and dst:
            ep = f"源节点表 `{src}` → 目标节点表 `{dst}`"
        else:
            ep = "端点未能解析，请参考表名推断"
        fields = rel_fields.get(r) or []
        rpks = rel_pk.get(r) or set()
        if fields:
            fld = ", ".join(f"{f} (PK)" if f in rpks else f for f in fields)
        else:
            fld = "（无属性或未能读取）"
        lines.append(f"- **{r}**：{ep}；属性：{fld}")
    if not rel_tables:
        lines.append("- （无关系表）")
    return "\n".join(lines)


def extract_cypher_from_llm_text(raw: str) -> str:
    """从模型回复中提取 Cypher：优先 ```cypher 代码块，否则用去掉围栏后的全文。"""
    text = (raw or "").strip()
    if not text:
        return ""
    m = re.search(r"```(?:cypher|cql)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return text


def fix_anonymous_rel_when_return_uses_r(cypher: str) -> str:
    """
    纠错：MATCH 写成 -[:TYPE]-> 但 RETURN 含 r 时，Kùzu 报 Variable r is not in scope。
    将第一条匿名关系 -[: 改为 -[r:（若尚未出现 -[r:）。
    """
    cy = (cypher or "").strip()
    if not cy or re.search(r"-\[\s*r\s*:", cy, re.IGNORECASE):
        return cy
    ret_m = re.search(
        r"\bRETURN\b([\s\S]+?)(?=\bLIMIT\b|\s*;|\s*$)",
        cy,
        re.IGNORECASE,
    )
    if not ret_m:
        return cy
    ret_body = ret_m.group(1)
    if not re.search(r"(?<![a-zA-Z0-9_`])r(?![a-zA-Z0-9_`])", ret_body):
        return cy
    if "-[:" not in cy:
        return cy
    return re.sub(r"-\s*\[\s*:", "-[r:", cy, count=1)


def generate_cypher_via_llm(
    *,
    user_question: str,
    schema_markdown: str,
    api_base: str,
    api_key: str,
    model: str,
) -> Tuple[Optional[str], Optional[str]]:
    """调用 OpenAI 兼容 Chat Completions API，返回 (cypher, error)。"""
    try:
        from openai import OpenAI
    except ImportError:
        return None, "未安装 openai，请执行：pip install openai"

    key = (api_key or "").strip()
    if not key:
        return None, "未配置 LLM API Key。"

    base = (api_base or "").strip().rstrip("/")
    if not base:
        return None, "API Base URL 为空。"

    client_kwargs: Dict[str, Any] = {"api_key": key, "base_url": base}
    import httpx

    client_kwargs["http_client"] = httpx.Client(verify=False)
    client = OpenAI(**client_kwargs)
    user_block = f"""{schema_markdown}

---

## 用户需求（请生成满足需求的 Cypher）

{user_question.strip()}
"""
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": CYPHER_LLM_SYSTEM_PROMPT},
                {"role": "user", "content": user_block},
            ],
            temperature=0.2,
            max_tokens=2048,
        )
        choice = resp.choices[0] if resp.choices else None
        content = choice.message.content if choice and choice.message else None
        if not content:
            return None, "模型返回内容为空。"
        cy = extract_cypher_from_llm_text(content)
        if not cy:
            return None, "未能从模型输出中解析出 Cypher。"
        cy = fix_anonymous_rel_when_return_uses_r(cy)
        return cy, None
    except Exception as exc:  # noqa: BLE001
        return None, f"LLM 请求失败：{exc}"


def generate_mock_cypher_via_llm(
    *,
    user_question: str,
    schema_markdown: str,
    api_base: str,
    api_key: str,
    model: str,
    existing_data_sample: str = "",
    error_feedback: str = "",
) -> Tuple[Optional[str], Optional[str]]:
    """
    调用 LLM 生成 Mock 数据的 Kùzu Cypher 语句（DDL + DML）。
    返回 (多条语句文本, error)；多条语句以分号分隔。

    参数：
        existing_data_sample: 从数据库采样的现有节点/边字段值示例（Markdown 文本）
        error_feedback:       上一次执行失败的错误信息汇总，供 LLM 修正
    """
    try:
        from openai import OpenAI
    except ImportError:
        return None, "未安装 openai，请执行：pip install openai"

    key = (api_key or "").strip()
    if not key:
        return None, "未配置 LLM API Key。"

    base = (api_base or "").strip().rstrip("/")
    if not base:
        return None, "API Base URL 为空。"

    client_kwargs: Dict[str, Any] = {"api_key": key, "base_url": base}
    import httpx

    client_kwargs["http_client"] = httpx.Client(verify=False)
    client = OpenAI(**client_kwargs)

    schema_section = schema_markdown.strip() if schema_markdown.strip() else "（当前数据库为空，无任何表）"

    sample_section = ""
    if existing_data_sample.strip():
        sample_section = f"\n\n---\n\n{existing_data_sample.strip()}"

    error_section = ""
    if error_feedback.strip():
        error_section = (
            f"\n\n---\n\n## 上次生成的语句执行报错，请修正后重新生成\n\n"
            f"错误信息：\n{error_feedback.strip()}\n\n"
            "请根据以上错���重新生成完整的、可执行的 Cypher 语句，避免重复同样的问题。"
        )

    user_block = f"""## 当前数据库 Schema

{schema_section}{sample_section}

---

## 用户 Mock 数据需求（请生成建表与插入语句）

{user_question.strip()}{error_section}
"""
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": MOCK_LLM_SYSTEM_PROMPT},
                {"role": "user", "content": user_block},
            ],
            temperature=0.4,
            max_tokens=4096,
        )
        choice = resp.choices[0] if resp.choices else None
        content = choice.message.content if choice and choice.message else None
        if not content:
            return None, "模型返回内容为空。"
        # 提取代码块（如有），否则用全文
        raw = extract_cypher_from_llm_text(content)
        if not raw:
            return None, "未能从模型输出中解析出 Cypher 语句。"
        return raw, None
    except Exception as exc:  # noqa: BLE001
        return None, f"LLM 请求失败：{exc}"
