# -*- coding: utf-8 -*-
"""Streamlit session_state 初始化与 Schema 字段缓存。"""

from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

import streamlit as st

from config import DEFAULT_LLM_BASE_URL, DEFAULT_LLM_MODEL
from db import build_schema_property_maps


def init_session() -> None:
    """初始化会话中与连接相关的状态键。"""
    if "kuzu_db" not in st.session_state:
        st.session_state.kuzu_db = None
    if "kuzu_conn" not in st.session_state:
        st.session_state.kuzu_conn = None
    if "connected" not in st.session_state:
        st.session_state.connected = False
    if "kuzu_llm_base_url" not in st.session_state:
        st.session_state.kuzu_llm_base_url = DEFAULT_LLM_BASE_URL
    if "kuzu_llm_model" not in st.session_state:
        st.session_state.kuzu_llm_model = DEFAULT_LLM_MODEL
    if "uploaded_kuzu_path" not in st.session_state:
        st.session_state.uploaded_kuzu_path = None
    if "kuzu_upload_cleanup_dir" not in st.session_state:
        st.session_state.kuzu_upload_cleanup_dir = None
    if "_kuzu_upload_sig" not in st.session_state:
        st.session_state._kuzu_upload_sig = None


def get_schema_property_maps_cached(
    conn: Any, node_tables: List[str], rel_tables: List[str]
) -> Tuple[
    Dict[str, List[str]],
    Dict[str, List[str]],
    Dict[str, Set[str]],
    Dict[str, Set[str]],
    Dict[str, Dict[str, str]],
    Dict[str, Dict[str, str]],
]:
    """在 Streamlit 会话中缓存 Schema 字段、主键与属性类型（TABLE_INFO）。"""
    key = (tuple(node_tables), tuple(rel_tables))
    cache_key_ok = st.session_state.get("_schema_fields_cache_key") == key
    has_types = "_schema_node_types" in st.session_state and "_schema_rel_types" in st.session_state
    if not cache_key_ok or not has_types:
        st.session_state._schema_fields_cache_key = key
        nmap, rmap, npk, rpk, ntyp, rtyp = build_schema_property_maps(
            conn, list(node_tables), list(rel_tables)
        )
        st.session_state._schema_node_fields = nmap
        st.session_state._schema_rel_fields = rmap
        st.session_state._schema_node_pk = npk
        st.session_state._schema_rel_pk = rpk
        st.session_state._schema_node_types = ntyp
        st.session_state._schema_rel_types = rtyp
    return (
        st.session_state._schema_node_fields,
        st.session_state._schema_rel_fields,
        st.session_state._schema_node_pk,
        st.session_state._schema_rel_pk,
        st.session_state._schema_node_types,
        st.session_state._schema_rel_types,
    )


def clear_schema_property_cache() -> None:
    """连接状态变化时清空，确保重新拉取 TABLE_INFO。"""
    for k in (
        "_schema_fields_cache_key",
        "_schema_node_fields",
        "_schema_rel_fields",
        "_schema_node_pk",
        "_schema_rel_pk",
        "_schema_node_types",
        "_schema_rel_types",
        "_query_result_cache_query",
        "_query_result_cache_df",
        "_kuzu_dl_cache",
        "_mock_rel_cache_key",
        "_mock_rel_cache_val",
    ):
        st.session_state.pop(k, None)
