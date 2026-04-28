# -*- coding: utf-8 -*-
"""Streamlit 子视图：查询结果与 Schema 画布。"""

from __future__ import annotations

import datetime
import os
import random
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from config import MAX_QUERY_LIMIT
from db import (
    build_mock_graph_write_cypher,
    build_mock_graph_delete_cypher,
    build_create_edge_only_cypher,
    build_update_edge_cypher,
    build_schema_diagram_graph,
    ensure_limit_clause,
    execute_to_dataframe,
    fetch_rel_connection_endpoints,
    execute_write_statements_tracked,
)
from graph import build_graph_from_dataframe, draft_canvas_to_pyvis
from session import get_schema_property_maps_cached
from visualization import make_pyvis_html


_DF_HTML_DISPLAY_CAP = 500


def _render_results_dataframe_themed(df: pd.DataFrame) -> None:
    """深色主题结果表（避免默认 st.dataframe 白底画布与整页风格冲突）。"""
    n = len(df)
    if n > _DF_HTML_DISPLAY_CAP:
        st.caption(
            f"以下表格展示前 **{_DF_HTML_DISPLAY_CAP}** 行（共 **{n}** 行）。"
            "行数来自当前 LIMIT / 自动上限。"
        )
        df_show = df.head(_DF_HTML_DISPLAY_CAP)
    else:
        df_show = df
    html_table = df_show.to_html(
        classes="kz-df",
        border=0,
        escape=True,
        index=True,
        justify="left",
    )
    st.markdown(
        f'<div class="kz-df-outer"><div class="kz-df-scroll">{html_table}</div></div>',
        unsafe_allow_html=True,
    )


def _section_bar() -> None:
    st.markdown(
        '<div style="height:2px;border-radius:999px;background:linear-gradient(90deg,transparent,'
        "#5a5890,#e8b4b8,#5a5890,transparent);"
        'margin:1rem 0 0.75rem;"></div>',
        unsafe_allow_html=True,
    )


def _sync_mock_props_widgets_to_drafts(
    nodes_list: List[Dict[str, Any]],
    edges_list: List[Dict[str, Any]],
    nfields: Dict[str, List[str]],
    rfields: Dict[str, List[str]],
) -> None:
    """将沙盘属性表单的 widget 值写回 mock_draft_*字典（在 form提交或入库前调用）。"""
    for node in nodes_list:
        nid = node["id"]
        tbl = str(node["table"])
        for fn in nfields.get(tbl, []):
            k = f"mock_np_{nid}_{fn}"
            if k in st.session_state:
                node["props"][fn] = st.session_state[k]
    for edge in edges_list:
        eid = edge["id"]
        rt = str(edge["rel"])
        for fn in rfields.get(rt, []):
            k = f"mock_ep_{eid}_{fn}"
            if k in st.session_state:
                edge["props"][fn] = st.session_state[k]


def render_query_results(
    conn: Any,
    raw_cypher: str,
    node_tables: List[str],
    rel_tables: List[str],
    *,
    precomputed_df: Optional[pd.DataFrame] = None,
) -> None:
    """
    执行 Cypher、套用 LIMIT 保护、渲染 Pyvis 图与结果表。
    所有数据库访问已在 execute_to_dataframe 内 try/except；此处处理 UI 反馈。
    precomputed_df：若传入则跳过执行（供 Streamlit 重跑脚本时复用上次结果，避免每次交互都查库）。
    """
    if precomputed_df is not None:
        df = precomputed_df
        q_err = None
    else:
        limited = ensure_limit_clause(raw_cypher, MAX_QUERY_LIMIT)
        if limited.strip() != raw_cypher.strip():
            st.caption(f"已自动追加 LIMIT {MAX_QUERY_LIMIT}")

        df, q_err = execute_to_dataframe(conn, limited)
        if q_err:
            st.error(f"查询执行失败：{q_err}")
            return
        if df is None:
            st.error("查询无结果或无法转换为表格。")
            return
        st.session_state["_query_result_cache_query"] = (raw_cypher or "").strip()
        st.session_state["_query_result_cache_df"] = df

    if q_err:
        st.error(f"查询执行失败：{q_err}")
        return
    if df is None:
        st.error("查询无结果或无法转换为表格。")
        return

    if "_query_graph_fullscreen" not in st.session_state:
        st.session_state._query_graph_fullscreen = False
    if "_query_graph_bridge_last" not in st.session_state:
        st.session_state._query_graph_bridge_last = ""

    query_graph_event = ""
    if _kuzu_bridge:
        query_graph_event = _kuzu_bridge(key="query_graph_bridge_comp") or ""
    if query_graph_event and query_graph_event != st.session_state._query_graph_bridge_last:
        st.session_state._query_graph_bridge_last = query_graph_event
        if query_graph_event == "fullscreen|enter" and not st.session_state._query_graph_fullscreen:
            st.session_state._query_graph_fullscreen = True
            st.rerun()
        if query_graph_event == "fullscreen|exit" and st.session_state._query_graph_fullscreen:
            st.session_state._query_graph_fullscreen = False
            st.rerun()

    if st.session_state._query_graph_fullscreen:
        st.markdown(
            """
            <style>
                header[data-testid="stHeader"],
                section[data-testid="stSidebar"] {
                    display: none !important;
                }
                div[data-testid="stToolbar"] {
                    display: none !important;
                }
                .block-container {
                    padding-top: 0.8rem !important;
                    padding-left: 0.8rem !important;
                    padding-right: 0.8rem !important;
                    max-width: 100% !important;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.caption(f"查询成功 · {len(df)} 行")

    nfields, rfields, npk, rpk, _, _ = get_schema_property_maps_cached(conn, node_tables, rel_tables)
    nodes, edges = build_graph_from_dataframe(df, nfields, rfields, npk, rpk)
    if not nodes and not edges:
        st.warning("结果中未解析到 Node / Relationship 对象，仅展示下方表格。")
    else:
        st.markdown('<div id="kz-graph-preview" style="margin-top:-0.5rem;"></div>', unsafe_allow_html=True)
        st.markdown(
            '<p class="kz-card-title" style="margin-top:0;">图预览 · Graph</p>',
            unsafe_allow_html=True,
        )
        if not st.session_state._query_graph_fullscreen:
            st.caption("可拖动节点、滚轮缩放；双击图进入全屏，按 Esc 退出。")
        try:
            html_doc = make_pyvis_html(
                nodes,
                edges,
                allow_fullscreen_shortcuts=True,
            )
            graph_height = 1500 if st.session_state._query_graph_fullscreen else 600
            components.html(html_doc, height=graph_height, scrolling=False)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Pyvis 渲染失败：{exc}")

        if st.session_state._query_graph_fullscreen:
            return
_kuzu_bridge = None
try:
    bridge_path = os.path.join(os.path.dirname(__file__), "vis_bridge")
    _kuzu_bridge = components.declare_component("kuzu_bridge", path=bridge_path)
except Exception:
    pass

def render_mock_view(
    conn: Any,
    node_tables: List[str],
    rel_tables: List[str],
    read_only: bool = False,
) -> None:
    """
    沙盒视图：加载已有数据，通过画布交互式修改或拖拉连线，并即时写入数据库。
    """
    st.markdown(
        """
        <style>
            .sb-prop-header {
                padding: 12px 16px;
                border-radius: 12px;
                background: linear-gradient(90deg, rgba(232, 180, 184, 0.12), transparent);
                border-left: 4px solid var(--kz-accent);
                margin-bottom: 16px;
            }
            .sb-prop-title {
                font-family: var(--kz-font);
                font-size: 1.15rem;
                font-weight: 700;
                color: #efd9ce;
                margin: 0;
            }
            .sb-prop-sub {
                font-size: 0.85rem;
                color: var(--kz-muted);
                margin-top: 4px;
            }
            /* Style form containers to be more subtle */
            [data-testid="stForm"] {
                border: 1px solid #3d3b6e !important;
                background: rgba(26, 26, 46, 0.6) !important;
                border-radius: 14px !important;
                padding: 1.25rem !important;
            }
            /* Advanced HUD Input Styling */
            [data-testid="stForm"] .stTextInput input {
                background: #1a1a2e !important;
                border: 1px solid #3d3b6e !important;
                color: #d4d0f0 !important;
                font-family: var(--kz-mono) !important;
                font-size: 0.9rem !important;
                box-shadow: inset 0 0 10px rgba(0,0,0,0.2) !important;
            }
            [data-testid="stForm"] .stTextInput input:focus {
                border-color: #e8b4b8 !important;
                box-shadow: 0 0 15px rgba(232, 180, 184, 0.2) !important;
                background: rgba(26, 26, 46, 0.8) !important;
            }
            
            /* High-Tech Glow Buttons */
            .stButton > button[data-testid="baseButton-primary"] {
                background: linear-gradient(135deg, #e8b4b8 0%, #efd9ce 100%) !important;
                border: none !important;
                box-shadow: 0 0 20px rgba(232, 180, 184, 0.2) !important;
                text-transform: uppercase !important;
                letter-spacing: 0.1em !important;
                font-weight: 700 !important;
            }
            .stButton > button[data-testid="baseButton-secondary"] {
                background: #3d3b6e !important;
                border: 1px solid #5a5890 !important;
                color: #d4d0f0 !important;
            }
            .stButton > button[data-testid="baseButton-secondary"]:hover {
                border-color: #7a76a8 !important;
                box-shadow: 0 0 15px rgba(0, 0, 0, 0.15) !important;
                color: #fff !important;
            }

            /* Streamlit Expander Re-styling (Glassmorphic HUD) */
            [data-testid="stExpander"] {
                background: rgba(26, 26, 46, 0.6) !important;
                border: 1px solid #3d3b6e !important;
                border-radius: 12px !important;
                overflow: hidden !important;
                margin-bottom: 1rem !important;
            }
            [data-testid="stExpander"] summary {
                background: linear-gradient(90deg, rgba(232, 180, 184, 0.1), transparent) !important;
                color: #e8b4b8 !important;
                font-family: var(--kz-font) !important;
                font-weight: 600 !important;
                padding: 10px 14px !important;
                transition: background 0.2s ease !important;
            }
            [data-testid="stExpander"] summary:hover {
                background: rgba(232, 180, 184, 0.15) !important;
            }
            [data-testid="stExpander"] [data-testid="stVerticalBlock"] {
                padding: 1rem !important;
                gap: 0.75rem !important;
            }
            [data-testid="stExpander"] p, [data-testid="stExpander"] span {
                color: #d4d0f0 !important;
            }
            
            /* Section Separation */
            .kz-card {
                display: none !important;
            }
            div[data-testid="column"]:has(.sb-panel-anchor) > div {
                padding: 24px 24px 20px !important;
            }
            .sb-graph-anchor,
            .sb-panel-anchor {
                width: 100%;
                height: 0;
            }
            div[data-testid="column"]:has(.sb-graph-anchor) {
                position: sticky;
                top: 0.9rem;
                align-self: start;
            }
            div[data-testid="column"]:has(.sb-graph-anchor) [data-testid="stIFrame"] {
                position: sticky;
                top: 2.9rem;
            }
            div[data-testid="column"]:has(.sb-panel-anchor) {
                position: sticky;
                top: 0.9rem;
                align-self: start;
            }
            div[data-testid="column"]:has(.sb-panel-anchor) div[data-testid="stVerticalBlock"] {
                position: sticky;
                top: 0.9rem;
                max-height: calc(100vh - 5.2rem);
                overflow-y: auto;
                padding-right: 6px;
                scrollbar-width: thin;
                scrollbar-color: rgba(232, 180, 184, 0.45) rgba(26, 26, 46, 0.4);
            }
            div[data-testid="column"]:has(.sb-panel-anchor) div[data-testid="stVerticalBlock"]::-webkit-scrollbar {
                width: 8px;
            }
            div[data-testid="column"]:has(.sb-panel-anchor) div[data-testid="stVerticalBlock"]::-webkit-scrollbar-track {
                background: rgba(26, 26, 46, 0.4);
                border-radius: 999px;
            }
            div[data-testid="column"]:has(.sb-panel-anchor) div[data-testid="stVerticalBlock"]::-webkit-scrollbar-thumb {
                background: linear-gradient(180deg, rgba(232, 180, 184, 0.6), rgba(192, 132, 252, 0.5));
                border-radius: 999px;
            }
            .sb-toolbar {
                margin-top: 1.5rem !important;
                border-top: 1px solid #3d3b6e !important;
                padding-top: 1rem !important;
            }
            .sb-pk-badge {
                display: inline-flex;
                align-items: center;
                gap: 4px;
                background: rgba(232, 180, 184, 0.1);
                color: #e8b4b8;
                font-size: 0.65rem;
                font-weight: 700;
                padding: 1px 6px;
                border-radius: 4px;
                border: 1px solid rgba(232, 180, 184, 0.4);
                text-transform: uppercase;
                letter-spacing: 0.05em;
                box-shadow: 0 0 8px rgba(232, 180, 184, 0.15);
            }
        </style>
        <div class="kz-hero-band">
          <div class="kz-title">可视化编辑 · Visual Editor</div>
          <div class="kz-sub">点选图中实体即可 <b>在位编辑</b>，或拖拽连线 <b>即时建图</b>。所见即所得，改动直达数据库。</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    whims = [
        "点击左侧图中的节点或关系，右侧即刻切换到编辑面板。",
        "按住节点拖出的连线，会在右侧呼出新关系创建表单。",
        "任何改动保存后都会立刻写入 Kùzu DB 并刷新星图。",
    ]
    st.caption(random.choice(whims))

    st.markdown(
        """
        <div style="background: rgba(232, 180, 184, 0.05); border: 1px dashed rgba(232, 180, 184, 0.2); border-radius: 12px; padding: 14px 18px; margin-bottom: 16px;">
            <p style="color: #e8b4b8; font-weight: 700; margin-top: 0; margin-bottom: 6px; font-size: 0.9rem;">操作指南</p>
            <div style="font-size: 0.82rem; color: #9b97c4; line-height: 1.6;">
                点击图中节点或关系 → 右侧编辑属性　｜　顶部工具栏点「🔌 牵拉连线」→ 从节点拖拽到目标节点 → 创建关系　｜　右下角可新建独立节点
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if read_only:
        st.warning(
            "⚠️ 当前为**只读模式**，无法编辑。"
            "请在左侧栏取消勾选「Read Only」后重新点击「连接」。"
        )
        return

    if not node_tables:
        st.error("当前数据库没有节点表，无法制图。请换一个有 Schema 的库。")
        return

    for k in ("mock_last_result", "_mock_auto_run", "_mock_cypher_box_pending"):
        st.session_state.pop(k, None)

    sel_val = ""
    if _kuzu_bridge:
        sel_val = _kuzu_bridge(key="mock_bridge_comp") or ""

    nfields, rfields, npk, rpk, ntypes, rtypes = get_schema_property_maps_cached(
        conn, node_tables, rel_tables
    )
    rel_endpoints: Dict[str, Tuple[str, str]] = {}
    for r in rel_tables:
        s, d = fetch_rel_connection_endpoints(conn, r)
        if s and d:
            rel_endpoints[str(r)] = (str(s), str(d))

    if "db_graph_nodes" not in st.session_state:
        st.session_state.db_graph_nodes = []
        st.session_state.db_graph_edges = []
        try:
            df_all, _ = execute_to_dataframe(conn, "MATCH (a)-[r]->(b) RETURN a,r,b LIMIT 50;")
            df_n, _ = execute_to_dataframe(conn, "MATCH (n) RETURN n LIMIT 50;")
            g_nodes, g_edges = ({}, [])
            if df_all is not None and not df_all.empty:
                g_nodes, g_edges = build_graph_from_dataframe(df_all, nfields, rfields, npk, rpk)
            if df_n is not None and not df_n.empty:
                g_n, _ = build_graph_from_dataframe(df_n, nfields, rfields, npk, rpk)
                g_nodes.update(g_n)
            
            for nid, (canvas, tip, panel) in g_nodes.items():
                props = {r["key"]: r["value"] for r in panel.get("rows", []) if r["value"] != "（空）"}
                st.session_state.db_graph_nodes.append({"id": nid, "table": panel["label_type"], "props": props})
            
            for s, d, eid, ecanvas, etip, panel in g_edges:
                props = {r["key"]: r["value"] for r in panel.get("rows", []) if r["value"] != "（空）"}
                st.session_state.db_graph_edges.append({"id": eid, "rel": panel["label_type"], "src": s, "dst": d, "props": props})
        except Exception as e:
            st.error(f"加载现有数据失败: {e}")

    nodes_list: List[Dict[str, Any]] = st.session_state.db_graph_nodes
    edges_list: List[Dict[str, Any]] = st.session_state.db_graph_edges
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    left, right = st.columns([1.6, 1.0], gap="large")

    _sel_kind, _sel_id, _sel_dst = "", "", ""
    if sel_val and "|" in sel_val:
        parts = sel_val.split("|")
        _sel_kind = parts[0]
        if len(parts) > 1: _sel_id = parts[1]
        if len(parts) > 2: _sel_dst = parts[2]

    def _run_and_refresh(cy: str):
        """执行写入并重载整个图（仅用于删除等需要全量刷新的场景）。"""
        with st.spinner("正在写入 Kùzu …"):
            exec_results, _ct = execute_write_statements_tracked(conn, cy)
        bad = [(s, e) for s, ok, e in exec_results if not ok]
        if bad:
            for s, msg in bad:
                short = s[:200] + ("…" if len(s) > 200 else "")
                st.error(f"{msg}\n\n```\n{short}\n```")
        else:
            st.success("操作成功！数据已更新。")
            try:
                conn.execute("CHECKPOINT;")
            except Exception:
                pass
            st.session_state.pop("db_graph_nodes", None)
            st.session_state.pop("db_graph_edges", None)
            st.session_state.pop("_edit_graph_cache_key", None)
            st.session_state.pop("_edit_graph_cache_html", None)
            st.rerun()

    def _write_and_patch(cy: str, params: Optional[Dict[str, Any]] = None):
        """执行写入，成功返回 True。"""
        exec_results, _ct = execute_write_statements_tracked(conn, cy, params)
        bad = [(s, e) for s, ok, e in exec_results if not ok]
        if bad:
            for s, msg in bad:
                st.error(f"{msg}\n\n```\n{s[:200]}\n```")
            return False
        try:
            conn.execute("CHECKPOINT")
        except Exception:
            pass
        return True

    def _send_graph_cmd(cmd_js: str):
        """Send a postMessage command to the vis.js iframe for incremental update."""
        import time as _t
        st.components.v1.html(
            f'<script>'
            f'var frames = window.parent.document.querySelectorAll("iframe");'
            f'for(var i=0;i<frames.length;i++){{'
            f'  try{{ frames[i].contentWindow.postMessage({cmd_js},"*"); }}catch(e){{}}'
            f'}}'
            f'</script><!-- {_t.time()} -->',
            height=0,
        )

    with right:
        st.markdown('<div class="sb-panel-anchor"></div>', unsafe_allow_html=True)
        st.markdown('<div class="kz-card">', unsafe_allow_html=True)
        st.markdown('<p class="kz-card-title" style="margin-top:0;">⚡ 属性 HUD 与系统操作</p>', unsafe_allow_html=True)
        
        if not _sel_kind:
            st.info("请点击左侧图中的节点或关系进行编辑")
            with st.container():
                with st.expander("✨ 新建独立实体节点", expanded=True):
                    new_tbl = st.selectbox("选择节点类型", node_tables, key="mock_pick_node_table")
                    fields = list(nfields.get(new_tbl, []))
                    
                    with st.form("new_node_form"):
                        st.markdown(
                            f"""
                            <div class="sb-prop-header" style="border-left-color: var(--kz-accent-soft);">
                                <div class="sb-prop-title">新建 {new_tbl}</div>
                                <div class="sb-prop-sub">请填写该节点的初始属性</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                        new_props = {}
                        pks_t = npk.get(new_tbl, set())
                        for fn in fields:
                            pk_badge = '<span class="sb-pk-badge">🔑 主键</span>' if fn in pks_t else ""
                            ph = "主键必填" if fn in pks_t else "可留空"
                            val0 = now_str if fn in ("created_at", "updated_at", "create_at", "update_at") else ""
                            st.markdown(f'<div style="margin-bottom: 3px; margin-top: 10px; font-size:0.85rem; color:#d4d0f0; font-weight:600;">{fn} {pk_badge}</div>', unsafe_allow_html=True)
                            new_props[fn] = st.text_input(f"Label for {fn}", value=val0, key=f"nn_{fn}", placeholder=ph, label_visibility="collapsed")
                        
                        if st.form_submit_button("🌟 保存入库", type="primary"):
                            cy, params, err = build_mock_graph_write_cypher([{"id": "new", "table": new_tbl, "props": new_props}], [], npk, rpk, rel_endpoints, ntypes, rtypes)
                            if err:
                                st.error(err)
                            elif cy and _write_and_patch(cy, params):
                                import uuid as _uuid
                                nid = f"new_{_uuid.uuid4().hex[:8]}"
                                st.session_state.db_graph_nodes.append({"id": nid, "table": new_tbl, "props": new_props})
                                pk_fields = npk.get(new_tbl, set())
                                label = next((new_props[f] for f in pk_fields if new_props.get(f)), new_tbl)
                                import json as _json
                                node_js = _json.dumps({"id": nid, "label": label, "color": "#e8b4b8", "size": 30})
                                _send_graph_cmd('{kuzu_cmd:"add_node",node:' + node_js + '}')
                                st.success("节点已创建！")

        elif _sel_kind == "node":
            node = next((n for n in nodes_list if str(n["id"]) == str(_sel_id)), None)
            if node is None:
                st.warning("所选节点未在当前数据集中找到（可能是因为随机加载限制）。")
            else:
                tbl = node["table"]
                props = node["props"]
                st.markdown(
                    f"""
                    <div class="sb-prop-header">
                        <div class="sb-prop-title">编辑节点: {tbl}</div>
                        <div class="sb-prop-sub">内部 ID: {_sel_id}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                
                with st.form(f"edit_node_form_{_sel_id}"):
                    edit_props = {}
                    pks_t = npk.get(tbl, set())
                    for fn in nfields.get(tbl, []):
                        pk_badge = '<span class="sb-pk-badge">🔑 🔑主键</span>' if fn in pks_t else ""
                        st.markdown(f'<div style="margin-bottom: 3px; margin-top: 12px; font-size:0.85rem; color:#d4d0f0; font-weight:600;">{fn} {pk_badge}</div>', unsafe_allow_html=True)
                        edit_props[fn] = st.text_input(f"Label for {fn}", value=props.get(fn, ""), key=f"en_{_sel_id}_{fn}", label_visibility="collapsed")
                    
                    if st.form_submit_button("🚀 更新", type="primary"):
                        cy, params, err = build_mock_graph_write_cypher([{"id": _sel_id, "table": tbl, "props": edit_props}], [], npk, rpk, rel_endpoints, ntypes, rtypes)
                        if err:
                            st.error(err)
                        elif cy and _write_and_patch(cy, params):
                            node["props"] = edit_props
                            name_val = edit_props.get("name", "").strip()
                            label = name_val[:42] if name_val else tbl
                            _send_graph_cmd('{kuzu_cmd:"update_node",node_id:"' + str(_sel_id) + '",label:"' + label.replace('"', '\\"') + '"}')
                            st.success("节点已更新！")

                st.markdown('<div style="margin-top: 12px;">', unsafe_allow_html=True)
                if st.button("🗑️ 抛弃该节点", type="secondary", use_container_width=True):
                    cy, params, err = build_mock_graph_delete_cypher([{"id": _sel_id, "table": tbl, "props": props}], [], npk, rpk, rel_endpoints, ntypes, rtypes)
                    if err:
                        st.error(err)
                    elif cy and _write_and_patch(cy, params):
                        st.session_state.db_graph_nodes = [n for n in st.session_state.db_graph_nodes if str(n["id"]) != str(_sel_id)]
                        st.session_state.db_graph_edges = [e for e in st.session_state.db_graph_edges if str(e["src"]) != str(_sel_id) and str(e["dst"]) != str(_sel_id)]
                        _send_graph_cmd('{kuzu_cmd:"remove_node",node_id:"' + str(_sel_id) + '"}')
                        st.success("节点已删除！")
                st.markdown("</div>", unsafe_allow_html=True)

        elif _sel_kind == "edge":
            edge = next((e for e in edges_list if str(e["id"]) == str(_sel_id)), None)
            if edge is None:
                st.warning("所选关系未在当前数据集中找到。")
            else:
                rt = edge["rel"]
                props = edge["props"]
                st.markdown(
                    f"""
                    <div class="sb-prop-header">
                        <div class="sb-prop-title">编辑关系: {rt}</div>
                        <div class="sb-prop-sub">{edge['src']} → {edge['dst']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                
                with st.form(f"edit_edge_form_{_sel_id}"):
                    edit_props = {}
                    pks_r = rpk.get(rt, set())
                    for fn in rfields.get(rt, []):
                        pk_badge = '<span class="sb-pk-badge">🔑 主键</span>' if fn in pks_r else ""
                        st.markdown(f'<div style="margin-bottom: 3px; margin-top: 12px; font-size:0.85rem; color:#d4d0f0; font-weight:600;">{fn} {pk_badge}</div>', unsafe_allow_html=True)
                        edit_props[fn] = st.text_input(f"Label for {fn}", value=props.get(fn, ""), key=f"ee_{_sel_id}_{fn}", label_visibility="collapsed")
                    
                    if st.form_submit_button("🚀 更新", type="primary"):
                        edge_data = {"id": _sel_id, "rel": rt, "src": edge["src"], "dst": edge["dst"], "props": edit_props}
                        src_node = next((n for n in nodes_list if str(n["id"]) == str(edge["src"])), None)
                        dst_node = next((n for n in nodes_list if str(n["id"]) == str(edge["dst"])), None)
                        if src_node and dst_node:
                            cy, params, err = build_update_edge_cypher(src_node, dst_node, edge_data, npk, rel_endpoints, ntypes, rtypes)
                            if err:
                                st.error(err)
                            elif cy and _write_and_patch(cy, params):
                                edge["props"] = edit_props
                                new_label = edit_props.get("name", "").strip()[:42] if edit_props.get("name", "").strip() else rt
                                tip = "\\n".join(f"{k}: {v}" for k, v in edit_props.items())
                                _send_graph_cmd('{kuzu_cmd:"update_edge",edge_id:"' + str(_sel_id) + '",label:"' + new_label.replace('"', '\\"') + '",title:"' + tip.replace('"', '\\"') + '"}')
                                st.success("关系已更新！")
                        else:
                            st.error("无法构建完整写入：找不到关系两端的完整节点上下文信息。")

                st.markdown('<div style="margin-top: 12px;">', unsafe_allow_html=True)
                if st.button("✂️ 剪断该连线", type="secondary", use_container_width=True):
                    src_node = next((n for n in nodes_list if str(n["id"]) == str(edge["src"])), None)
                    dst_node = next((n for n in nodes_list if str(n["id"]) == str(edge["dst"])), None)
                    if src_node and dst_node:
                        edge_data = {"id": _sel_id, "rel": rt, "src": edge["src"], "dst": edge["dst"], "props": props, "_src_props": src_node["props"], "_dst_props": dst_node["props"]}
                        cy, params, err = build_mock_graph_delete_cypher([], [edge_data], npk, rpk, rel_endpoints, ntypes, rtypes)
                        if err:
                            st.error(err)
                        elif cy and _write_and_patch(cy, params):
                            st.session_state.db_graph_edges = [e for e in st.session_state.db_graph_edges if str(e["id"]) != str(_sel_id)]
                            _send_graph_cmd('{kuzu_cmd:"remove_edge",edge_id:"' + str(_sel_id) + '"}')
                            st.success("连线已删除！")
                    else:
                        st.error("找不到关系两端的节点，无法执行删除。")
                st.markdown("</div>", unsafe_allow_html=True)

        elif _sel_kind == "add_edge":
            src_node = next((n for n in nodes_list if str(n["id"]) == str(_sel_id)), None)
            dst_node = next((n for n in nodes_list if str(n["id"]) == str(_sel_dst)), None)
            if src_node is None or dst_node is None:
                st.warning("连线无效：找不到起始或目标节点。")
            else:
                valid_rels = [r for r, ep in rel_endpoints.items() if ep[0] == src_node["table"] and ep[1] == dst_node["table"]]
                if not valid_rels:
                    st.error("此起止节点组合没有支持的关系表（Schema 不匹配）。")
                else:
                    st.markdown(
                        f'<div class="sb-prop-header" style="border-left-color: var(--kz-accent-hot);">'
                        f'<div class="sb-prop-title">新建关系</div>'
                        f'<div class="sb-prop-sub">{src_node["table"]} → {dst_node["table"]}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                    with st.form("add_edge_form"):
                        new_rt = st.selectbox("选择关系类型", valid_rels, key="ae_rt")
                        new_props = {}
                        pks_r = rpk.get(new_rt, set())
                        for fn in rfields.get(new_rt, []):
                            pk_badge = '<span class="sb-pk-badge">🔑 主键</span>' if fn in pks_r else ""
                            val0 = now_str if fn in ("created_at", "updated_at", "create_at", "update_at") else ""
                            st.markdown(f'<div style="margin-bottom:3px;margin-top:10px;font-size:0.85rem;color:#d4d0f0;font-weight:600;">{fn} {pk_badge}</div>', unsafe_allow_html=True)
                            new_props[fn] = st.text_input(f"Label for {fn}", value=val0, key=f"ae_{fn}", label_visibility="collapsed")
                        if st.form_submit_button("🚀 创建关系并入库", type="primary"):
                            missing_pks = [f for f in pks_r if not (new_props.get(f) or "").strip()]
                            if missing_pks:
                                st.error(f"必填主键字段不能为空：{', '.join(missing_pks)}")
                            else:
                                edge_data = {"id": "new", "rel": new_rt, "src": _sel_id, "dst": _sel_dst, "props": new_props}
                                cy, params, err = build_create_edge_only_cypher(src_node, dst_node, edge_data, npk, rel_endpoints, ntypes, rtypes)
                                if err:
                                    st.error(err)
                                elif cy and _write_and_patch(cy, params):
                                    new_edge = {"id": f"new_{_sel_id}_{_sel_dst}", "rel": new_rt, "src": _sel_id, "dst": _sel_dst, "props": new_props}
                                    st.session_state.db_graph_edges.append(new_edge)
                                    st.session_state.pop("_edit_graph_cache_html", None)
                                    st.success("关系已创建！")
                                    st.rerun()

        _section_bar()
        st.markdown('<div style="margin-top: 10px;">', unsafe_allow_html=True)
        if st.button("🔄 取消选择 / 刷新数据", use_container_width=True):
            st.session_state.pop("db_graph_nodes", None)
            st.session_state.pop("db_graph_edges", None)
            st.session_state.pop("_edit_graph_cache_html", None)
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True) # Close kz-card

    with left:
        st.markdown('<div class="sb-graph-anchor"></div>', unsafe_allow_html=True)
        n_n, n_e = len(nodes_list), len(edges_list)
        st.markdown(
            f'<p style="margin:0 0 10px;font-size:0.95rem;color:#9b97c4;">'
            f"已有数据抽样：🌟 节点 <b>{n_n}</b>　🌉 星桥 <b>{n_e}</b></p>",
            unsafe_allow_html=True,
        )
        py_nodes, py_edges = draft_canvas_to_pyvis(nodes_list, edges_list, nfields, rfields, npk, rpk)
        if "_edit_graph_cache_html" not in st.session_state:
            try:
                html_doc = make_pyvis_html(
                    py_nodes,
                    py_edges,
                    panel_hint="点击节点或关系唤出编辑面板 · 顶部工具栏可拖拉连线",
                    disable_internal_panel=True,
                    enable_manipulation=True,
                )
                st.session_state["_edit_graph_cache_html"] = html_doc
            except Exception as exc:
                st.error(f"图渲染失败：{exc}")
                html_doc = None
        else:
            html_doc = st.session_state.get("_edit_graph_cache_html")
        if html_doc:
            st.components.v1.html(html_doc, height=820, scrolling=False)

def render_schema_view(
    conn: Any,
    node_tables: List[str],
    rel_tables: List[str],
    schema_err: Optional[str],
) -> None:
    """Schema 模式画布（无多余说明文案）。"""
    st.markdown(
        """
        <div class="kz-hero-band">
          <div class="kz-title">Schema 拓扑</div>
          <div class="kz-sub">节点表与关系类型的有向结构；点击表或边查看字段定义。</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if schema_err and not node_tables and not rel_tables:
        st.warning(schema_err)
    if not node_tables and not rel_tables:
        st.error("未能读取到任何节点表或关系表。")
        return
    try:
        nodes, edges = build_schema_diagram_graph(conn, node_tables, rel_tables)
        if not nodes:
            st.warning("未生成 Schema 图节点，请检查 SHOW_TABLES / TABLE_INFO。")
            return
        st.markdown(
            '<p class="kz-card-title">结构图 · Diagram</p>',
            unsafe_allow_html=True,
        )
        html_doc = make_pyvis_html(nodes, edges, visual_theme="schema")
        components.html(html_doc, height=600, scrolling=False)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Schema 图渲染失败：{exc}")
