# -*- coding: utf-8 -*-
"""从查询结果 DataFrame 中提取节点与边。"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd


def _kuzu_internal_id_str(raw: Any) -> Optional[str]:
    """
    将 Kuzu 内部 ID 统一为稳定字符串。
    新版驱动在 pandas DataFrame 中常使用 dict：{'table': int, 'offset': int}。
    旧版或对象形态则可能是标量或可 str() 的值。
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        if "table" in raw and "offset" in raw:
            return f"{raw['table']}:{raw['offset']}"
        # 其它 dict（极少）退化为确定性字符串
        return str(raw)
    return str(raw)


def _kuzu_node_type() -> Any:
    """kuzu 包未必导出 Node（会通过 get_as_df 得到 dict），用 getattr 避免触发 __getattr__ 报错。"""
    import kuzu as kz

    return getattr(kz, "Node", None)


def _kuzu_relationship_type() -> Any:
    import kuzu as kz

    return getattr(kz, "Relationship", None)


def _is_missing_val(val: Any) -> bool:
    """空值判断，避免 pandas NA 误判。"""
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except Exception:  # noqa: BLE001
        pass
    return False


def _schema_field_order(
    table_name: str, field_map: Dict[str, List[str]], raw_keys: Set[str]
) -> List[str]:
    """
    详情面板中属性的展示顺序：以 Schema（TABLE_INFO）为准；
    若当前类型无缓存则退化为结果里出现的数据键排序（兜底）。
    """
    order = field_map.get(table_name)
    if order:
        return list(order)
    return sorted(raw_keys)


def _canvas_label_use_name(table_or_rel_name: str, raw: Dict[str, Any]) -> str:
    """画布上的短标签：优先使用属性 name，否则用类型名。"""
    v = raw.get("name")
    if not _is_missing_val(v) and str(v).strip():
        return str(v).strip()[:42]
    return str(table_or_rel_name)[:30]


def _build_explorer_panel_payload(
    entity: str,
    label_type: str,
    internal_ref: str,
    schema_order: List[str],
    full_raw: Dict[str, Any],
    pk_names: Set[str],
) -> Dict[str, Any]:
    """
    供嵌入 HTML 的 JSON 数据：右侧面板两列键值 + PK 标记（对齐 Kuzu Explorer 信息结构）。
    entity: \"node\" 或 \"rel\".
    """
    rows: List[Dict[str, Any]] = []
    for key in schema_order:
        raw_val = full_raw.get(key)
        empty = key not in full_raw or _is_missing_val(raw_val)
        rows.append(
            {
                "key": str(key),
                "value": "（空）" if empty else str(raw_val),
                "pk": str(key) in pk_names,
            }
        )
    return {
        "entity": entity,
        "label_type": str(label_type),
        "internal_ref": str(internal_ref),
        "properties_title": (
            "Node Properties" if entity == "node" else "Relationship properties"
        ),
        "rows": rows,
    }


def _unpack_kuzu_node(
    val: Any,
    node_field_map: Dict[str, List[str]],
    node_pk_map: Dict[str, Set[str]],
) -> Optional[Tuple[str, str, str, str, Dict[str, Any]]]:
    """
    从 kuzu.Node（若驱动仍导出）或 dict 形态解析：
    (internal_id_str, 类型名, 画布短标签, 悬停 title, 右侧面板 JSON 数据)。
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None

    NodeCls = _kuzu_node_type()
    if NodeCls is not None and isinstance(val, NodeCls):
        internal_id = None
        label = "Node"
        # 不同版本可能提供 _id / get_id、_label / get_label
        if hasattr(val, "_id"):
            internal_id = _kuzu_internal_id_str(val._id)
        if internal_id is None and callable(getattr(val, "get_id", None)):
            internal_id = _kuzu_internal_id_str(val.get_id())
        if hasattr(val, "_label"):
            label = str(val._label)
        elif callable(getattr(val, "get_label", None)):
            label = str(val.get_label())

        props: Dict[str, Any] = {}
        # 常见：支持按属性名索引 / get_property_names
        try:
            names = val.get_property_names()  # type: ignore[attr-defined]
            for name in names:
                props[str(name)] = val[name]  # type: ignore[index]
        except Exception:  # noqa: BLE001
            try:
                iter_items = val.items()  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                try:
                    props = dict(val) if hasattr(val, "keys") else {}
                except Exception:  # noqa: BLE001
                    props = {}
            else:
                for k, v in iter_items:
                    if str(k).startswith("_") or str(k).upper() in {"_ID", "_LABEL"}:
                        continue
                    props[str(k)] = v

        if internal_id is None:
            return None
        schema_order = _schema_field_order(label, node_field_map, set(props.keys()))
        canvas = _canvas_label_use_name(label, props)
        pks = node_pk_map.get(label, set())
        panel = _build_explorer_panel_payload("node", label, internal_id, schema_order, props, pks)
        tip = "点击查看右侧属性"
        return internal_id, label, canvas, tip, panel

    if isinstance(val, dict):
        # 关系行也会带 _label，必须用 _src/_dst 与节点区分（先留给 _unpack_kuzu_rel）
        if any(k in val for k in ("_SRC", "_src")) and any(k in val for k in ("_DST", "_dst")):
            return None
        nid_raw = val.get("_ID") or val.get("_id")
        nid_str = _kuzu_internal_id_str(nid_raw)
        lab = val.get("_LABEL") or val.get("_label") or "Node"
        if nid_str is None:
            return None
        props = {
            k: v
            for k, v in val.items()
            if str(k) not in {"_ID", "_id", "_LABEL", "_label"}
        }
        schema_order = _schema_field_order(str(lab), node_field_map, set(props.keys()))
        canvas = _canvas_label_use_name(str(lab), props)
        pks = node_pk_map.get(str(lab), set())
        panel = _build_explorer_panel_payload("node", str(lab), nid_str, schema_order, props, pks)
        tip = "点击查看右侧属性"
        return nid_str, str(lab), canvas, tip, panel

    return None


def _unpack_kuzu_rel(
    val: Any,
    rel_field_map: Dict[str, List[str]],
    rel_pk_map: Dict[str, Set[str]],
) -> Optional[Tuple[str, str, str, str, str, Dict[str, Any]]]:
    """
    从 kuzu.Relationship 或 dict 解析：
    (src_id, dst_id, 关系类型名, 画布短标签, 悬停 title, 右侧面板 JSON)。
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None

    RelCls = _kuzu_relationship_type()
    if RelCls is not None and isinstance(val, RelCls):
        src_id = None
        dst_id = None
        rlab = "REL"
        # 常见字段名：_src / _dst 或 _src_id / _dst_id
        for s_attr, d_attr in (
            ("_src", "_dst"),
            ("_src_id", "_dst_id"),
            ("src_id", "dst_id"),
        ):
            if hasattr(val, s_attr) and hasattr(val, d_attr):
                src_id = _kuzu_internal_id_str(getattr(val, s_attr))
                dst_id = _kuzu_internal_id_str(getattr(val, d_attr))
                break
        if hasattr(val, "_label"):
            rlab = str(val._label)
        elif callable(getattr(val, "get_label", None)):
            rlab = str(val.get_label())

        props: Dict[str, Any] = {}
        try:
            names = val.get_property_names()  # type: ignore[attr-defined]
            for name in names:
                props[str(name)] = val[name]  # type: ignore[index]
        except Exception:  # noqa: BLE001
            pass

        if src_id is None or dst_id is None:
            return None
        schema_order = _schema_field_order(rlab, rel_field_map, set(props.keys()))
        canvas = _canvas_label_use_name(rlab, props)
        ref = f"{src_id} → {dst_id}"
        pks = rel_pk_map.get(rlab, set())
        panel = _build_explorer_panel_payload("rel", rlab, ref, schema_order, props, pks)
        tip = "点击查看右侧属性"
        return src_id, dst_id, rlab, canvas, tip, panel

    if isinstance(val, dict):
        src = val.get("_SRC") or val.get("_src") or val.get("SRC")
        dst = val.get("_DST") or val.get("_dst") or val.get("DST")
        rlab = val.get("_LABEL") or val.get("_label") or "REL"
        src_str = _kuzu_internal_id_str(src)
        dst_str = _kuzu_internal_id_str(dst)
        if src_str is None or dst_str is None:
            return None
        props = {
            k: v
            for k, v in val.items()
            if str(k).upper()
            not in {
                "_SRC",
                "_DST",
                "_LABEL",
                "_SRC_ID",
                "_DST_ID",
                "_src",
                "_dst",
                "_label",
            }
        }
        rlab_s = str(rlab)
        schema_order = _schema_field_order(rlab_s, rel_field_map, set(props.keys()))
        canvas = _canvas_label_use_name(rlab_s, props)
        ref = f"{src_str} → {dst_str}"
        pks = rel_pk_map.get(rlab_s, set())
        panel = _build_explorer_panel_payload("rel", rlab_s, ref, schema_order, props, pks)
        tip = "点击查看右侧属性"
        return src_str, dst_str, rlab_s, canvas, tip, panel

    return None


def build_graph_from_dataframe(
    df: pd.DataFrame,
    node_field_map: Dict[str, List[str]],
    rel_field_map: Dict[str, List[str]],
    node_pk_map: Dict[str, Set[str]],
    rel_pk_map: Dict[str, Set[str]],
) -> Tuple[
    Dict[str, Tuple[str, str, Dict[str, Any]]],
    List[Tuple[str, str, str, str, str, Dict[str, Any]]],
]:
    """
    扫描 DataFrame 每个单元格，收集节点字典与边列表。
    返回：
        nodes: internal_id -> (画布短标签, 悬停 title, 面板数据)
        edges: (src_id, dst_id, edge_id, 边画布标签, 悬停 title, 面板数据)
    """
    NodeCls = _kuzu_node_type()
    RelCls = _kuzu_relationship_type()

    nodes: Dict[str, Tuple[str, str, Dict[str, Any]]] = {}
    edges: List[Tuple[str, str, str, str, str, Dict[str, Any]]] = []
    edge_seen: Set[Tuple[str, str, str]] = set()
    edge_idx = 0

    def add_node(raw: Any) -> None:
        packed = _unpack_kuzu_node(raw, node_field_map, node_pk_map)
        if not packed:
            return
        nid, _tab, canvas, tip, panel = packed
        prev = nodes.get(nid)
        if prev is None or len(json.dumps(panel, ensure_ascii=False)) > len(
            json.dumps(prev[2], ensure_ascii=False)
        ):
            nodes[nid] = (canvas, tip, panel)

    def add_rel(raw: Any) -> None:
        nonlocal edge_idx
        packed = _unpack_kuzu_rel(raw, rel_field_map, rel_pk_map)
        if not packed:
            return
        s, d, rt, ecanvas, etip, panel = packed
        key = (s, d, rt)
        if key not in edge_seen:
            edge_seen.add(key)
            eid = f"kz_e_{edge_idx}_{s}_{d}_{rt}"
            edge_idx += 1
            edges.append((s, d, eid, ecanvas, etip, panel))

    for _, row in df.iterrows():
        for cell in row.tolist():
            if NodeCls is not None and isinstance(cell, NodeCls):
                add_node(cell)
            elif RelCls is not None and isinstance(cell, RelCls):
                add_rel(cell)
            elif isinstance(cell, dict):
                # 新版 get_as_df：整格即为 dict；关系含 _src/_dst，节点含 _id
                if any(k in cell for k in ("_SRC", "_src")):
                    add_rel(cell)
                elif any(k in cell for k in ("_ID", "_id")):
                    add_node(cell)
            # RecursiveRel：尽量从中取出嵌套 Node / Rel（若存在 iter 接口）
            elif type(cell).__name__ == "RecursiveRel":
                try:
                    for item in cell:  # type: ignore[var-annotated]
                        if NodeCls is not None and isinstance(item, NodeCls):
                            add_node(item)
                        elif RelCls is not None and isinstance(item, RelCls):
                            add_rel(item)
                        elif isinstance(item, dict):
                            if any(k in item for k in ("_SRC", "_src")):
                                add_rel(item)
                            elif any(k in item for k in ("_ID", "_id")):
                                add_node(item)
                except Exception:  # noqa: BLE001
                    pass

    return nodes, edges


def draft_canvas_to_pyvis(
    draft_nodes: List[Dict[str, Any]],
    draft_edges: List[Dict[str, Any]],
    node_field_map: Dict[str, List[str]],
    rel_field_map: Dict[str, List[str]],
    node_pk_map: Dict[str, Set[str]],
    rel_pk_map: Dict[str, Set[str]],
) -> Tuple[
    Dict[str, Tuple[str, str, Dict[str, Any]]],
    List[Tuple[str, str, str, str, str, Dict[str, Any]]],
]:
    """
    将 Mock 画布的草稿转为 Pyvis 所需的 nodes / edges 结构（含 Explorer 侧栏 JSON）。
    draft_nodes: [{"id", "table", "props"}]；draft_edges: [{"id", "rel", "src", "dst", "props"}]
    """
    nodes: Dict[str, Tuple[str, str, Dict[str, Any]]] = {}
    edges: List[Tuple[str, str, str, str, str, Dict[str, Any]]] = []

    for dn in draft_nodes:
        nid = str(dn["id"])
        lab = str(dn["table"])
        props = dict(dn.get("props") or {})
        schema_order = _schema_field_order(lab, node_field_map, set(props.keys()))
        canvas = _canvas_label_use_name(lab, props)
        tip = f"✨ 节点 · {lab}"
        pks = node_pk_map.get(lab, set())
        panel = _build_explorer_panel_payload(
            "node",
            lab,
            f"id：{nid}",
            schema_order,
            props,
            pks,
        )
        nodes[nid] = (canvas, tip, panel)

    for de in draft_edges:
        eid = str(de["id"])
        src = str(de["src"])
        dst = str(de["dst"])
        rlab = str(de["rel"])
        props = dict(de.get("props") or {})
        schema_order = _schema_field_order(rlab, rel_field_map, set(props.keys()))
        canvas = _canvas_label_use_name(rlab, props)
        tip = f"🌉 关系 · {rlab} · {src} → {dst}"
        pks = rel_pk_map.get(rlab, set())
        panel = _build_explorer_panel_payload(
            "rel",
            rlab,
            f"id：{eid}，端点 {src} → {dst}",
            schema_order,
            props,
            pks,
        )
        edges.append((src, dst, eid, canvas, tip, panel))

    return nodes, edges
