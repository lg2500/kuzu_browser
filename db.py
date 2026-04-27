# -*- coding: utf-8 -*-
"""Kuzu 连接、查询执行与 Schema 元数据读取（无 Streamlit 依赖）。"""

from __future__ import annotations

import ast
import json
import math
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd


def connect_kuzu(db_path: str, read_only: bool = False) -> Tuple[Any, Any, Optional[str]]:
    """
    打开 Kuzu 数据库并创建连接。
    返回 (database, connection, error_message)；成功时 error_message 为 None。
    """
    import kuzu

    path = (db_path or "").strip()
    if not path:
        return None, None, "数据库路径为空：请先在侧边栏上传 .kuzu 文件并连接。"

    try:
        database = kuzu.Database(path, read_only=read_only)
        connection = kuzu.Connection(database)
        return database, connection, None
    except Exception as exc:  # noqa: BLE001 — 统一展示给用户
        return None, None, f"连接失败：{exc}"


def ensure_limit_clause(cypher: str, max_rows: int) -> str:
    """
    若整段 Cypher 中未出现 LIMIT 子句（忽略大小写），则在末尾追加 LIMIT。
    说明：为简化实现，不做字符串字面量级别的词法分析；极少数边界情况可能误判。
    """
    text = cypher.strip()
    if not text:
        return text
    # 已有 LIMIT 则不修改
    if re.search(r"(?i)\blimit\b", text):
        return text
    out = text.rstrip()
    if out.endswith(";"):
        core, semi = out[:-1].rstrip(), ";"
    else:
        core, semi = out, ""
    return f"{core} LIMIT {max_rows}{semi}"


def execute_to_dataframe(conn: Any, cypher: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    执行查询并尽量转为 DataFrame；失败返回 (None, 错误信息)。
    兼容较新版本 kuzu.QueryResult.get_as_df() 与仅含 get_next 的旧接口。
    """
    try:
        result = conn.execute(cypher)
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)

    try:
        if hasattr(result, "get_as_df"):
            df = result.get_as_df()
            return df, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)

    try:
        cols = list(result.get_column_names()) if hasattr(result, "get_column_names") else []
        rows: List[Any] = []
        get_next = getattr(result, "get_next", None)
        has_next = getattr(result, "has_next", None)
        if callable(get_next):
            if callable(has_next):
                while has_next():
                    rows.append(get_next())
            else:
                while True:
                    try:
                        rows.append(get_next())
                    except Exception:  # noqa: BLE001
                        break
        if not cols and rows:
            # 退化：无列名时用占位
            cols = [f"col_{i}" for i in range(len(rows[0]))]
        return pd.DataFrame(rows, columns=cols), None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def read_node_rel_table_names(conn: Any) -> Tuple[List[str], List[str], Optional[str]]:
    """
    读取节点表名与关系表名。
    优先尝试规格中提到的 SHOW_NODE_TABLES / SHOW_REL_TABLES；
    失败则回退到 CALL SHOW_TABLES()，再失败则尝试连接对象的内部 API。
    """
    node_names: List[str] = []
    rel_names: List[str] = []
    err_accum: List[str] = []

    # 1) 公开的 CALL SHOW_TABLES（按 type 区分 NODE / REL）
    df, err = execute_to_dataframe(conn, "CALL SHOW_TABLES() RETURN *;")
    if df is not None and not df.empty:
        cols_lower = {c.lower(): c for c in df.columns}
        name_col = cols_lower.get("name") or cols_lower.get("table name")
        type_col = cols_lower.get("type")
        if name_col and type_col:
            for _, row in df.iterrows():
                t = str(row[type_col]).upper()
                nm = str(row[name_col])
                if "NODE" in t:
                    node_names.append(nm)
                elif "REL" in t:
                    rel_names.append(nm)
            return sorted(set(node_names)), sorted(set(rel_names)), None
        err_accum.append("SHOW_TABLES 返回列不符合预期，跳过。")
    elif err:
        err_accum.append(f"SHOW_TABLES: {err}")

    # 2) 规格中的 CALL（不同版本函数名可能略有差异，逐个尝试）
    for call_sql, kind in (
        ("CALL SHOW_NODE_TABLES() RETURN *;", "node"),
        ("CALL show_node_tables() RETURN *;", "node"),
        ("CALL SHOW_REL_TABLES() RETURN *;", "rel"),
        ("CALL show_rel_tables() RETURN *;", "rel"),
    ):
        sub_df, sub_err = execute_to_dataframe(conn, call_sql)
        if sub_df is not None and not sub_df.empty:
            first_col = sub_df.columns[0]
            vals = [str(x) for x in sub_df[first_col].tolist()]
            if kind == "node":
                node_names.extend(vals)
            else:
                rel_names.extend(vals)
        elif sub_err:
            err_accum.append(f"{call_sql} {sub_err}")

    if node_names or rel_names:
        return sorted(set(node_names)), sorted(set(rel_names)), None

    # 3) 连接对象上的非公开辅助方法（版本间可能存在）
    try:
        nfn = getattr(conn, "_get_node_table_names", None)
        rfn = getattr(conn, "_get_rel_table_names", None)
        if callable(nfn):
            node_names.extend([str(x) for x in nfn()])
        if callable(rfn):
            rel_names.extend([str(x) for x in rfn()])
        if node_names or rel_names:
            return sorted(set(node_names)), sorted(set(rel_names)), None
    except Exception as exc:  # noqa: BLE001
        err_accum.append(str(exc))

    return [], [], "；".join(err_accum) if err_accum else "无法读取 schema。"


def fetch_rel_connection_endpoints(conn: Any, rel_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    关系表对应的源 / 目的节点表名（CALL SHOW_CONNECTION）。
    用于 Schema 模式图画边。
    """
    ident = str(rel_name).replace("'", "''")
    df, _err = execute_to_dataframe(conn, f"CALL SHOW_CONNECTION('{ident}') RETURN *;")
    if df is None or df.empty:
        return None, None
    col_map = {str(c).lower().strip(): c for c in df.columns}
    src_c = col_map.get("source table name")
    dst_c = col_map.get("destination table name")
    if not src_c or not dst_c:
        return None, None
    row = df.iloc[0]
    return str(row[src_c]), str(row[dst_c])


def build_schema_definition_panel_payload(conn: Any, table_name: str, entity: str) -> Dict[str, Any]:
    """
    Kuzu Explorer Schema 右栏：Name | Type 两列，主键 PK 徽章。
    entity 为 \"node\"（节点表）或 \"rel\"（关系表）。
    """
    ident = str(table_name).replace("'", "''")
    df, _err = execute_to_dataframe(conn, f"CALL TABLE_INFO('{ident}') RETURN *;")
    rows: List[Dict[str, Any]] = []
    if df is not None and not df.empty:
        name_col, pk_col, type_col = _table_info_columns(df)
        if name_col:
            for _, row in df.iterrows():
                t = ""
                if type_col:
                    t = str(row[type_col])
                pk = False
                if pk_col:
                    v = row[pk_col]
                    pk = v is True or str(v).lower() in {"true", "1", "yes"}
                rows.append({"key": str(row[name_col]), "value": t, "pk": pk})
    ref = f"节点表 · {table_name}" if entity == "node" else f"关系表 · {table_name}"
    return {
        "entity": entity,
        "label_type": str(table_name),
        "internal_ref": ref,
        "properties_title": "字段定义",
        "rows": rows,
        "column_headers": ["Name", "Type"],
        "schema_mode": True,
    }


def build_schema_diagram_graph(
    conn: Any,
    node_tables: List[str],
    rel_tables: List[str],
) -> Tuple[Dict[str, Tuple[str, str, Dict[str, Any]]], List[Tuple[str, str, str, str, str, Dict[str, Any]]]]:
    """
    仅由元数据构成的「Schema」图：圆 = 节点表，有向边 = 关系表（含自环）。
    返回结构与 build_graph_from_dataframe 一致，供 make_pyvis_html 使用。
    """
    nodes: Dict[str, Tuple[str, str, Dict[str, Any]]] = {}
    for t in node_tables:
        panel = build_schema_definition_panel_payload(conn, t, "node")
        nodes[str(t)] = (str(t), "点击查看字段类型", panel)

    edges: List[Tuple[str, str, str, str, str, Dict[str, Any]]] = []
    for i, r in enumerate(rel_tables):
        src, dst = fetch_rel_connection_endpoints(conn, str(r))
        if not src or not dst:
            continue
        eid = f"schema_rel_{i}_{r}"
        panel = build_schema_definition_panel_payload(conn, str(r), "rel")
        edges.append((src, dst, eid, str(r), "点击查看关系属性字段", panel))

    return nodes, edges


def _table_info_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """解析 TABLE_INFO 返回列：属性名、是否主键。"""
    col_map = {str(c).lower().strip(): c for c in df.columns}
    name_col = col_map.get("name")
    pk_col = col_map.get("primary key")
    return name_col, pk_col, col_map.get("type")


def fetch_table_property_names(conn: Any, table_name: str) -> List[str]:
    """
    通过 CALL TABLE_INFO 读取某节点表或关系表在 Schema 中声明的属性名（顺序与定义一致）。
    """
    ident = str(table_name).replace("'", "''")
    df, _err = execute_to_dataframe(conn, f"CALL TABLE_INFO('{ident}') RETURN *;")
    if df is None or df.empty:
        return []
    name_col, _, _ = _table_info_columns(df)
    if not name_col:
        return []
    out: List[str] = []
    for _, row in df.iterrows():
        out.append(str(row[name_col]))
    return out


def fetch_table_pk_field_names(conn: Any, table_name: str) -> Set[str]:
    """Schema 中标记为主键的属性名集合（用于 Explorer 风格 PK 徽章）。"""
    ident = str(table_name).replace("'", "''")
    df, _err = execute_to_dataframe(conn, f"CALL TABLE_INFO('{ident}') RETURN *;")
    if df is None or df.empty:
        return set()
    name_col, pk_col, _ = _table_info_columns(df)
    if not name_col or not pk_col:
        return set()
    pks: Set[str] = set()
    for _, row in df.iterrows():
        v = row[pk_col]
        is_pk = v is True or str(v).lower() in {"true", "1", "yes"}
        if is_pk:
            pks.add(str(row[name_col]))
    return pks


def fetch_table_property_type_map(conn: Any, table_name: str) -> Dict[str, str]:
    """属性名 -> TABLE_INFO 中的类型字符串（如 STRING、STRING[]、INT64）。"""
    ident = str(table_name).replace("'", "''")
    df, _err = execute_to_dataframe(conn, f"CALL TABLE_INFO('{ident}') RETURN *;")
    if df is None or df.empty:
        return {}
    name_col, _, type_col = _table_info_columns(df)
    if not name_col:
        return {}
    out: Dict[str, str] = {}
    for _, row in df.iterrows():
        nm = str(row[name_col])
        typ = ""
        if type_col:
            v = row[type_col]
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                typ = str(v).strip()
        out[nm] = typ
    return out


def execute_write_statements(
    conn: Any, cypher_text: str
) -> List[Tuple[str, bool, Optional[str]]]:
    """
    将 cypher_text 按分号分割为多条 Cypher 语句并逐一执行（写操作）。
    返回 [(statement, success, error_msg), ...]。
    成功时 error_msg 为 None；失败时 success 为 False，error_msg 为错误文本。
    """
    # 简单按分号分割，过滤空语句；LLM 生成的 mock 数据字符串内含分号概率极低
    raw_stmts = [s.strip() for s in cypher_text.split(";")]
    statements = [s for s in raw_stmts if s]

    results: List[Tuple[str, bool, Optional[str]]] = []
    for stmt in statements:
        try:
            conn.execute(stmt)
            results.append((stmt, True, None))
        except Exception as exc:  # noqa: BLE001
            results.append((stmt, False, str(exc)))
    return results


def sample_existing_graph_data(
    conn: Any,
    node_tables: List[str],
    rel_tables: List[str],
    sample_rows: int = 3,
) -> str:
    """
    采样图中现有节点与关系的真实字段值，返回 Markdown 文本供 LLM 参考。
    每张节点/关系表最多取 sample_rows 条示例。
    """
    lines: List[str] = ["## 图中现有数据示例（供参考字段值格式与内容）", ""]

    # 节点示例
    for t in node_tables:
        ident = t.replace("'", "''")
        df, err = execute_to_dataframe(conn, f"MATCH (n:`{ident}`) RETURN n LIMIT {sample_rows};")
        if err or df is None or df.empty:
            continue
        lines.append(f"### 节点表 `{t}` 示例")
        # 展开第一列（可能是节点对象）
        first_col = df.columns[0]
        for _, row in df.iterrows():
            val = row[first_col]
            if hasattr(val, "__class__") and val.__class__.__name__ in ("KuzuNode", "dict"):
                try:
                    props = dict(val) if isinstance(val, dict) else val._properties  # type: ignore[attr-defined]
                    lines.append("- " + ", ".join(f"{k}: {repr(v)}" for k, v in props.items()))
                    continue
                except Exception:  # noqa: BLE001
                    pass
            lines.append(f"- {val}")
        lines.append("")

    # 关系示例
    for r in rel_tables:
        ident = r.replace("'", "''")
        df, err = execute_to_dataframe(
            conn, f"MATCH (a)-[e:`{ident}`]->(b) RETURN e LIMIT {sample_rows};"
        )
        if err or df is None or df.empty:
            continue
        lines.append(f"### 关系表 `{r}` 示例")
        first_col = df.columns[0]
        for _, row in df.iterrows():
            val = row[first_col]
            if hasattr(val, "__class__") and val.__class__.__name__ in ("KuzuRel", "dict"):
                try:
                    props = dict(val) if isinstance(val, dict) else val._properties  # type: ignore[attr-defined]
                    lines.append("- " + ", ".join(f"{k}: {repr(v)}" for k, v in props.items()))
                    continue
                except Exception:  # noqa: BLE001
                    pass
            lines.append(f"- {val}")
        lines.append("")

    if len(lines) <= 2:
        return ""
    return "\n".join(lines)


def rollback_created_tables(conn: Any, created_tables: List[Tuple[str, str]]) -> None:
    """
    回滚在一次 Mock 写入中新建的表（DROP TABLE）。
    created_tables 是 [(table_type, table_name), ...] 列表，
    table_type 为 'node' 或 'rel'，关系表需先 DROP。
    """
    # 先 DROP 关系表，再 DROP 节点表（避免外键依赖报错）
    for ttype, tname in reversed(created_tables):
        ident = tname.replace("'", "''")
        try:
            conn.execute(f"DROP TABLE `{ident}`;")
        except Exception:  # noqa: BLE001
            pass


def execute_write_statements_tracked(
    conn: Any, cypher_text: str
) -> Tuple[List[Tuple[str, bool, Optional[str]]], List[Tuple[str, str]]]:
    """
    执行多条写语句（DDL + DML），并追踪本次新建的表（用于失败时回滚）。
    返回:
        results: [(statement, success, error_msg), ...]
        created_tables: [(table_type, table_name), ...]  本次成功 CREATE TABLE 的列表
    """
    import re as _re

    raw_stmts = [s.strip() for s in cypher_text.split(";")]
    statements = [s for s in raw_stmts if s]

    results: List[Tuple[str, bool, Optional[str]]] = []
    created_tables: List[Tuple[str, str]] = []

    for stmt in statements:
        try:
            conn.execute(stmt)
            results.append((stmt, True, None))
            # 检测是否为 CREATE NODE/REL TABLE
            m_node = _re.match(r"(?i)CREATE\s+NODE\s+TABLE\s+`?(\w+)`?", stmt.strip())
            m_rel = _re.match(r"(?i)CREATE\s+REL\s+TABLE\s+`?(\w+)`?", stmt.strip())
            if m_node:
                created_tables.append(("node", m_node.group(1)))
            elif m_rel:
                created_tables.append(("rel", m_rel.group(1)))
        except Exception as exc:  # noqa: BLE001
            results.append((stmt, False, str(exc)))

    return results, created_tables


def build_schema_property_maps(
    conn: Any, node_tables: List[str], rel_tables: List[str]
) -> Tuple[
    Dict[str, List[str]],
    Dict[str, List[str]],
    Dict[str, Set[str]],
    Dict[str, Set[str]],
    Dict[str, Dict[str, str]],
    Dict[str, Dict[str, str]],
]:
    """
    table_name -> 属性名列表；
    table_name -> 主键属性名集合；
    table_name -> {属性名: 类型字符串}（来自 TABLE_INFO）。
    """
    node_map: Dict[str, List[str]] = {}
    rel_map: Dict[str, List[str]] = {}
    node_pk: Dict[str, Set[str]] = {}
    rel_pk: Dict[str, Set[str]] = {}
    node_types: Dict[str, Dict[str, str]] = {}
    rel_types: Dict[str, Dict[str, str]] = {}
    for t in node_tables:
        try:
            node_map[t] = fetch_table_property_names(conn, t)
            node_pk[t] = fetch_table_pk_field_names(conn, t)
            node_types[t] = fetch_table_property_type_map(conn, t)
        except Exception:  # noqa: BLE001
            node_map[t] = []
            node_pk[t] = set()
            node_types[t] = {}
    for t in rel_tables:
        try:
            rel_map[t] = fetch_table_property_names(conn, t)
            rel_pk[t] = fetch_table_pk_field_names(conn, t)
            rel_types[t] = fetch_table_property_type_map(conn, t)
        except Exception:  # noqa: BLE001
            rel_map[t] = []
            rel_pk[t] = set()
            rel_types[t] = {}
    return node_map, rel_map, node_pk, rel_pk, node_types, rel_types


def _kuzu_list_element_base_type(type_hint: Optional[str]) -> Optional[str]:
    """若为列表类型（STRING[]、LIST(STRING) 等）返回元素基础类型名，否则 None。"""
    if not type_hint:
        return None
    t = str(type_hint).strip().upper().replace(" ", "")
    if t.endswith("[]"):
        return (t[:-2] or None)
    m = re.match(r"^LIST\((\w+)\)$", t)
    if m:
        return m.group(1)
    return None


def _parse_user_string_list(s: str) -> List[str]:
    """把表单文本解析为字符串列表（JSON 数组、Python 列表字面量、逗号分隔、单值）。"""
    s = s.strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            val = json.loads(s)
            if isinstance(val, list):
                return [str(x) for x in val]
        except json.JSONDecodeError:
            pass
        try:
            val = ast.literal_eval(s)
            if isinstance(val, (list, tuple)):
                return [str(x) for x in val]
        except (ValueError, SyntaxError, TypeError):
            pass
    if "," in s:
        return [p.strip().strip("'\"") for p in s.split(",") if p.strip()]
    return [s]


def _cypher_string_list_literal(elems: List[str]) -> str:
    parts: List[str] = []
    for e in elems:
        esc = str(e).replace("\\", "\\\\").replace("'", "\\'")
        parts.append(f"'{esc}'")
    return "[" + ", ".join(parts) + "]"


def _parse_user_int_list(s: str) -> Optional[List[int]]:
    s = s.strip()
    if not s:
        return None
    if s.startswith("["):
        try:
            val = json.loads(s)
            if isinstance(val, list):
                return [int(x) for x in val]
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
        try:
            val = ast.literal_eval(s)
            if isinstance(val, (list, tuple)):
                return [int(x) for x in val]
        except (ValueError, SyntaxError, TypeError):
            return None
    if "," in s:
        out: List[int] = []
        for p in s.split(","):
            p = p.strip()
            if not p:
                continue
            out.append(int(p))
        return out
    return [int(s)]


def format_cypher_literal_from_text(
    raw: Optional[str], type_hint: Optional[str] = None
) -> Optional[str]:
    """
    将表单中的字符串转为 Cypher 属性值字面量；空白视为未填写（返回 None）。
    type_hint 来自 TABLE_INFO（如 STRING[]），用于生成列表等非标量字面量。
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None

    list_base = _kuzu_list_element_base_type(type_hint)
    if list_base:
        if list_base == "STRING":
            elems = _parse_user_string_list(s)
            if not elems:
                return None
            return _cypher_string_list_literal(elems)
        if list_base in ("INT64", "INT32", "INT16", "INT8", "UINT64", "UINT32", "UINT16", "UINT8"):
            try:
                ints = _parse_user_int_list(s)
            except ValueError:
                return None
            if not ints:
                return None
            return "[" + ", ".join(str(x) for x in ints) + "]"
        if list_base in ("DOUBLE", "FLOAT"):
            if s.startswith("["):
                try:
                    val = json.loads(s)
                    if isinstance(val, list):
                        nums = [float(x) for x in val]
                        return "[" + ", ".join(repr(x) for x in nums) + "]"
                except (json.JSONDecodeError, TypeError, ValueError):
                    return None
                return None
            try:
                f = float(s)
                if math.isfinite(f):
                    return "[" + repr(f) + "]"
            except ValueError:
                return None
            return None
        elems = _parse_user_string_list(s)
        if not elems:
            return None
        return _cypher_string_list_literal(elems)

    low = s.lower()
    if low in ("true", "false"):
        return low
    try:
        if "." in s:
            f = float(s)
            if math.isfinite(f):
                return repr(f)
        else:
            return str(int(s))
    except ValueError:
        pass
    esc = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{esc}'"


def _escape_cypher_ident(name: str) -> str:
    return str(name).replace("`", "``")


def _props_map_to_cypher_inner(
    props: Dict[str, Any], field_types: Optional[Dict[str, str]] = None
) -> str:
    """props 值来自表单（多为 str）；仅包含有值的属性。field_types: 属性名 -> TABLE_INFO 类型。"""
    field_types = field_types or {}
    parts: List[str] = []
    for k, v in props.items():
        hint = field_types.get(str(k))
        lit = format_cypher_literal_from_text(
            v if isinstance(v, str) else str(v), hint
        )
        if lit is None:
            continue
        ek = _escape_cypher_ident(str(k))
        parts.append(f"`{ek}`: {lit}")
    return ", ".join(parts)


def _validate_pks(
    props: Dict[str, Any],
    pk_names: Set[str],
    ctx: str,
    field_types: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    field_types = field_types or {}
    for pk in pk_names:
        raw = props.get(pk, "")
        if not isinstance(raw, str):
            raw = str(raw) if raw is not None else ""
        if format_cypher_literal_from_text(raw, field_types.get(pk)) is None:
            return f"{ctx}：主键 `{pk}` 不能为空。"
    return None


def build_mock_graph_write_cypher(
    draft_nodes: List[Dict[str, Any]],
    draft_edges: List[Dict[str, Any]],
    node_pk_map: Dict[str, Set[str]],
    rel_pk_map: Dict[str, Set[str]],
    rel_endpoints: Dict[str, Tuple[str, str]],
    node_field_types: Optional[Dict[str, Dict[str, str]]] = None,
    rel_field_types: Optional[Dict[str, Dict[str, str]]] = None,
) -> Tuple[str, Optional[str]]:
    """
    由 Mock 画布的草稿节点 / 边生成 Kùzu 写库 Cypher（多条语句以分号分隔）。
    draft_nodes: [{"id", "table", "props": {str: str}}]
    draft_edges: [{"id", "rel", "src", "dst", "props": {str: str}}]
    node_field_types / rel_field_types: 表名 -> {属性名: TABLE_INFO 类型}，用于 STRING[] 等。
    """
    if not draft_nodes and not draft_edges:
        return "", "画布是空的，请先添加节点。"

    node_field_types = node_field_types or {}
    rel_field_types = rel_field_types or {}

    by_id = {str(n["id"]): n for n in draft_nodes}
    stmts: List[str] = []

    for node in draft_nodes:
        t = str(node["table"])
        props = dict(node.get("props") or {})
        nft = node_field_types.get(t, {})
        err = _validate_pks(
            props, node_pk_map.get(t, set()), f"节点 `{t}`（{node['id']}）", nft
        )
        if err:
            return "", err
        inner = _props_map_to_cypher_inner(props, nft)
        if not inner:
            return "", f"节点 `{t}`（{node['id']}）没有可写入的非空属性。"
        lab = _escape_cypher_ident(t)
        pks = node_pk_map.get(t, set())
        if pks:
            pk_inner = _props_map_to_cypher_inner(
                {k: v for k, v in props.items() if k in pks}, nft
            )
            set_parts = []
            for k, v in props.items():
                if k not in pks:
                    v_lit = format_cypher_literal_from_text(v, nft.get(k))
                    if v_lit is not None:
                        set_parts.append(f"n.`{_escape_cypher_ident(k)}` = {v_lit}")
            if set_parts:
                stmts.append(f"MERGE (n:`{lab}` {{{pk_inner}}}) SET " + ", ".join(set_parts))
            else:
                stmts.append(f"MERGE (n:`{lab}` {{{pk_inner}}})")
        else:
            stmts.append(f"CREATE (n:`{lab}` {{{inner}}})")

    for edge in draft_edges:
        rel = str(edge["rel"])
        sid = str(edge["src"])
        did = str(edge["dst"])
        src_n = by_id.get(sid)
        dst_n = by_id.get(did)
        if not src_n or not dst_n:
            return "", f"星桥 `{edge.get('id')}` 的端点节点已丢失，请重新连接或删除该关系。"
        if rel not in rel_endpoints:
            return "", f"未知关系类型 `{rel}`。"
        exp_src, exp_dst = rel_endpoints[rel]
        if str(src_n["table"]) != exp_src or str(dst_n["table"]) != exp_dst:
            return (
                "",
                f"关系 `{rel}` 要求从 `{exp_src}` 指向 `{exp_dst}`，"
                f"当前为 `{src_n['table']}` → `{dst_n['table']}`。",
            )
        spe = dict(src_n.get("props") or {})
        dpe = dict(dst_n.get("props") or {})
        nft_s = node_field_types.get(exp_src, {})
        nft_d = node_field_types.get(exp_dst, {})
        err = _validate_pks(
            spe, node_pk_map.get(exp_src, set()), f"起点节点（{sid}）", nft_s
        )
        if err:
            return "", err
        err = _validate_pks(
            dpe, node_pk_map.get(exp_dst, set()), f"终点节点（{did}）", nft_d
        )
        if err:
            return "", err
        si = _props_map_to_cypher_inner(spe, nft_s)
        di = _props_map_to_cypher_inner(dpe, nft_d)
        rprops = dict(edge.get("props") or {})
        rft = rel_field_types.get(rel, {})
        err = _validate_pks(
            rprops, rel_pk_map.get(rel, set()), f"关系 `{rel}`（{edge.get('id')}）", rft
        )
        if err:
            return "", err
        ri = _props_map_to_cypher_inner(rprops, rft)
        rlab = _escape_cypher_ident(rel)
        sla = _escape_cypher_ident(exp_src)
        dla = _escape_cypher_ident(exp_dst)
        if ri:
            stmts.append(
                f"MATCH (a:`{sla}` {{{si}}}), (b:`{dla}` {{{di}}}) "
                f"CREATE (a)-[:`{rlab}` {{{ri}}}]->(b)"
            )
        else:
            stmts.append(
                f"MATCH (a:`{sla}` {{{si}}}), (b:`{dla}` {{{di}}}) CREATE (a)-[:`{rlab}`]->(b)"
            )

    return ";\n".join(stmts) + (";" if stmts else ""), None


def build_create_edge_only_cypher(
    src_node: Dict[str, Any],
    dst_node: Dict[str, Any],
    edge: Dict[str, Any],
    node_pk_map: Dict[str, Set[str]],
    rel_endpoints: Dict[str, Tuple[str, str]],
    node_field_types: Optional[Dict[str, Dict[str, str]]] = None,
    rel_field_types: Optional[Dict[str, Dict[str, str]]] = None,
) -> Tuple[str, Optional[str]]:
    """Only MATCH existing nodes and CREATE the edge — never MERGE/CREATE nodes."""
    node_field_types = node_field_types or {}
    rel_field_types = rel_field_types or {}

    rel = str(edge["rel"])
    if rel not in rel_endpoints:
        return "", f"未知关系类型 `{rel}`。"
    exp_src, exp_dst = rel_endpoints[rel]
    st_name = str(src_node["table"])
    dt_name = str(dst_node["table"])
    if st_name != exp_src or dt_name != exp_dst:
        return "", f"关系 `{rel}` 要求 `{exp_src}` → `{exp_dst}`，当前为 `{st_name}` → `{dt_name}`。"

    spe = dict(src_node.get("props") or {})
    dpe = dict(dst_node.get("props") or {})
    nft_s = node_field_types.get(exp_src, {})
    nft_d = node_field_types.get(exp_dst, {})

    src_pks = node_pk_map.get(exp_src, set())
    dst_pks = node_pk_map.get(exp_dst, set())
    si = _props_map_to_cypher_inner({k: v for k, v in spe.items() if k in src_pks}, nft_s) if src_pks else _props_map_to_cypher_inner(spe, nft_s)
    di = _props_map_to_cypher_inner({k: v for k, v in dpe.items() if k in dst_pks}, nft_d) if dst_pks else _props_map_to_cypher_inner(dpe, nft_d)
    if not si or not di:
        return "", "无法定位端点节点（主键为空）。"

    rprops = dict(edge.get("props") or {})
    rft = rel_field_types.get(rel, {})
    ri = _props_map_to_cypher_inner(rprops, rft)
    sla = _escape_cypher_ident(exp_src)
    dla = _escape_cypher_ident(exp_dst)
    rlab = _escape_cypher_ident(rel)
    if ri:
        stmt = f"MATCH (a:`{sla}` {{{si}}}), (b:`{dla}` {{{di}}}) CREATE (a)-[:`{rlab}` {{{ri}}}]->(b);"
    else:
        stmt = f"MATCH (a:`{sla}` {{{si}}}), (b:`{dla}` {{{di}}}) CREATE (a)-[:`{rlab}`]->(b);"
    return stmt, None


def build_mock_graph_delete_cypher(
    draft_nodes: List[Dict[str, Any]],
    draft_edges: List[Dict[str, Any]],
    node_pk_map: Dict[str, Set[str]],
    rel_pk_map: Dict[str, Set[str]],
    rel_endpoints: Dict[str, Tuple[str, str]],
    node_field_types: Optional[Dict[str, Dict[str, str]]] = None,
    rel_field_types: Optional[Dict[str, Dict[str, str]]] = None,
) -> Tuple[str, Optional[str]]:
    """生成用于删除指定的节点或关系的 Cypher 语句。"""
    node_field_types = node_field_types or {}
    rel_field_types = rel_field_types or {}
    stmts: List[str] = []

    for edge in draft_edges:
        rel = str(edge["rel"])
        sid = str(edge["src"])
        did = str(edge["dst"])
        if rel not in rel_endpoints:
            return "", f"未知关系类型 `{rel}`。"
        exp_src, exp_dst = rel_endpoints[rel]
        
        # 匹配端点
        nft_s = node_field_types.get(exp_src, {})
        nft_d = node_field_types.get(exp_dst, {})
        spe = dict(edge.get("_src_props") or {})
        dpe = dict(edge.get("_dst_props") or {})
        
        err = _validate_pks(spe, node_pk_map.get(exp_src, set()), f"起点节点", nft_s)
        if err: return "", err
        err = _validate_pks(dpe, node_pk_map.get(exp_dst, set()), f"终点节点", nft_d)
        if err: return "", err
        
        si = _props_map_to_cypher_inner({k:v for k,v in spe.items() if k in node_pk_map.get(exp_src, set())}, nft_s)
        di = _props_map_to_cypher_inner({k:v for k,v in dpe.items() if k in node_pk_map.get(exp_dst, set())}, nft_d)
        
        rprops = dict(edge.get("props") or {})
        rft = rel_field_types.get(rel, {})
        rpks = rel_pk_map.get(rel, set())
        
        err = _validate_pks(rprops, rpks, f"关系 `{rel}`", rft)
        if err: return "", err
        
        ri = _props_map_to_cypher_inner({k:v for k,v in rprops.items() if k in rpks}, rft)
        
        rlab = _escape_cypher_ident(rel)
        sla = _escape_cypher_ident(exp_src)
        dla = _escape_cypher_ident(exp_dst)
        
        if ri:
            stmts.append(f"MATCH (a:`{sla}` {{{si}}})-[r:`{rlab}` {{{ri}}}]->(b:`{dla}` {{{di}}}) DELETE r")
        else:
            stmts.append(f"MATCH (a:`{sla}` {{{si}}})-[r:`{rlab}`]->(b:`{dla}` {{{di}}}) DELETE r")

    for node in draft_nodes:
        t = str(node["table"])
        props = dict(node.get("props") or {})
        nft = node_field_types.get(t, {})
        pks = node_pk_map.get(t, set())
        
        err = _validate_pks(props, pks, f"节点 `{t}`（{node['id']}）", nft)
        if err: return "", err
        
        lab = _escape_cypher_ident(t)
        if pks:
            pk_inner = _props_map_to_cypher_inner({k: v for k, v in props.items() if k in pks}, nft)
            stmts.append(f"MATCH (n:`{lab}` {{{pk_inner}}}) DETACH DELETE n")

    return ";\n".join(stmts) + (";" if stmts else ""), None


