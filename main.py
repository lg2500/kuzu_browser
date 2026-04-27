# -*- coding: utf-8 -*-
"""
Kuzu 图数据库轻量可视化浏览器（Streamlit）

依赖（见 requirements.txt）。官方源超时可用镜像，例如：
    pip install -r requirements.txt --default-timeout=600 --retries=15 \\
      -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

运行（在项目根目录）：
    python -m streamlit run main.py

连接成功后，在侧边栏「数据库连接」下方切换 Query / Schema。
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional, Tuple

import streamlit as st

from config import (
    DEFAULT_CYPHER,
    DEFAULT_LLM_API_KEY,
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_MODEL,
)
from db import connect_kuzu, read_node_rel_table_names
from llm import format_schema_for_cypher_llm, generate_cypher_via_llm
from session import (
    clear_schema_property_cache,
    get_schema_property_maps_cached,
    init_session,
)
from theme import inject_theme
from uploads import remove_tree_quiet, save_kuzu_upload_to_temp
from views import render_query_results, render_schema_view, render_mock_view


def _prepare_kuzu_download_cached(
    db_path: str,
    conn: object | None,
    read_only: bool,
) -> Tuple[Optional[bytes], Optional[str]]:
    """按路径+mtime 缓存下载字节，避免每次重跑都读整库 + CHECKPOINT（显著减轻卡顿）。"""
    from uploads import prepare_kuzu_download

    p = Path(db_path)
    try:
        if p.exists():
            sig = (str(p.resolve()), p.stat().st_mtime_ns, bool(read_only))
            hit = st.session_state.get("_kuzu_dl_cache")
            if isinstance(hit, dict) and hit.get("sig") == sig and hit.get("data") is not None:
                return hit["data"], None
    except OSError:
        pass

    data, err = prepare_kuzu_download(db_path, conn=conn, read_only=read_only)
    if not err and data:
        try:
            if p.exists():
                sig = (str(p.resolve()), p.stat().st_mtime_ns, bool(read_only))
                st.session_state["_kuzu_dl_cache"] = {"sig": sig, "data": data}
        except OSError:
            pass
    return data, err


def main() -> None:
    st.set_page_config(
        page_title="Kùzu Graph Explorer",
        page_icon="🕸️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_theme()
    init_session()

    current_page = "Query"
    read_only = False

    # -- Sidebar extra styles (radio nav, expander) --
    _sidebar_extra_css = (
        '<style>'
        'section[data-testid="stSidebar"] .stRadio > div { '
        '  display:flex; flex-direction:column; gap:4px; }'
        'section[data-testid="stSidebar"] .stRadio > div > label { '
        '  padding:6px 10px !important; border-radius:8px; '
        '  font-size:0.88rem !important; color:#9b97c4 !important; font-weight:500; '
        '  cursor:pointer; transition:background 0.12s ease; }'
        'section[data-testid="stSidebar"] .stRadio > div > label:hover { '
        '  background:rgba(232,180,184,0.06); }'
        'section[data-testid="stSidebar"] .stRadio > div > label[data-checked="true"],'
        'section[data-testid="stSidebar"] .stRadio > div > label:has(input:checked) { '
        '  background:rgba(232,180,184,0.12); color:#efd9ce !important; font-weight:600; }'
        'section[data-testid="stSidebar"] [data-testid="stExpander"] { '
        '  background:rgba(26,26,46,0.4) !important; border:1px solid #3d3b6e !important; '
        '  border-radius:10px !important; overflow:hidden !important; margin-top:4px !important; }'
        'section[data-testid="stSidebar"] [data-testid="stExpander"] summary { '
        '  background:transparent !important; color:#9b97c4 !important; '
        '  font-size:0.82rem !important; font-weight:500 !important; padding:8px 12px !important; }'
        'section[data-testid="stSidebar"] [data-testid="stExpander"] summary:hover { '
        '  color:#e8b4b8 !important; }'
        'section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stVerticalBlock"] { '
        '  padding:0 12px 10px !important; gap:0.6rem !important; }'
        '</style>'
    )

    with st.sidebar:
        st.markdown(_sidebar_extra_css, unsafe_allow_html=True)

        # ── Logo ──
        st.markdown(
            '<div style="display:flex;align-items:center;gap:10px;margin-bottom:20px;padding-bottom:14px;'
            'border-bottom:1px solid #3d3b6e;">'
            '<div style="width:32px;height:32px;border-radius:10px;'
            'background:linear-gradient(135deg,#e8b4b8,#efd9ce);'
            'display:flex;align-items:center;justify-content:center;font-size:15px;">🕸️</div>'
            '<div>'
            '<div style="font-size:1.1rem;font-weight:700;color:#efd9ce;letter-spacing:-0.02em;line-height:1.2;">'
            'Kùzu Explorer</div>'
            '<div style="font-size:0.65rem;color:#5a5890;letter-spacing:0.06em;">Graph Database Browser</div>'
            '</div></div>',
            unsafe_allow_html=True,
        )

        # ── 连接状态指示 ──
        _is_connected = st.session_state.connected and st.session_state.kuzu_conn is not None
        _status_dot = "#67e8f9" if _is_connected else "#5a5890"
        _status_text = "已连接" if _is_connected else "未连接"
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:8px;'
            f'background:rgba(26,26,46,0.5);border:1px solid #3d3b6e;border-radius:8px;'
            f'padding:8px 12px;margin-bottom:14px;">'
            f'<div style="width:8px;height:8px;border-radius:50%;background:{_status_dot};'
            f'box-shadow:0 0 6px {_status_dot};flex-shrink:0;"></div>'
            f'<span style="font-size:0.78rem;color:#9b97c4;">{_status_text}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # ── 数据库区域 ──
        st.markdown(
            '<p style="font-size:0.65rem;font-weight:600;letter-spacing:0.12em;'
            'text-transform:uppercase;color:#5a5890;margin:0 0 8px;">数据库</p>',
            unsafe_allow_html=True,
        )

        if _is_connected and st.session_state.get("uploaded_kuzu_path"):
            from pathlib import Path as _Path
            _file_name = _Path(st.session_state.uploaded_kuzu_path).name
            _dl_bytes, _dl_err = _prepare_kuzu_download_cached(
                st.session_state.uploaded_kuzu_path,
                conn=st.session_state.kuzu_conn,
                read_only=False,
            )
            st.markdown(
                f'<div style="background:rgba(26,26,46,0.5);border:1px solid #3d3b6e;border-radius:10px;'
                f'padding:10px 12px;margin-bottom:6px;">'
                f'<div style="display:flex;align-items:center;gap:8px;">'
                f'<span style="font-size:14px;">📁</span>'
                f'<span style="font-size:0.82rem;color:#d4d0f0;flex:1;overflow:hidden;'
                f'text-overflow:ellipsis;white-space:nowrap;">{_file_name}</span>'
                f'</div></div>',
                unsafe_allow_html=True,
            )
            _c1, _c2 = st.columns(2)
            with _c1:
                if not _dl_err and _dl_bytes:
                    st.download_button(
                        label="⬇ 下载",
                        data=_dl_bytes,
                        file_name=_file_name,
                        mime="application/octet-stream",
                        key="download_kuzu_file",
                    )
            with _c2:
                if st.button("✕ 断开", key="disconnect_db"):
                    remove_tree_quiet(st.session_state.get("kuzu_upload_cleanup_dir"))
                    st.session_state.uploaded_kuzu_path = None
                    st.session_state.kuzu_upload_cleanup_dir = None
                    st.session_state._kuzu_upload_sig = None
                    st.session_state.kuzu_db = None
                    st.session_state.kuzu_conn = None
                    st.session_state.connected = False
                    clear_schema_property_cache()
                    st.rerun()
            st.markdown(
                '<style>'
                'section[data-testid="stSidebar"] .stDownloadButton,'
                'section[data-testid="stSidebar"] .stDownloadButton > div {'
                '  margin-top:0!important;padding-top:0!important;'
                '}'
                'section[data-testid="stSidebar"] .stDownloadButton button,'
                'section[data-testid="stSidebar"] .stButton > button:not([data-testid="baseButton-primary"]) {'
                '  background:transparent!important;'
                '  background-color:transparent!important;'
                '  border:none!important;'
                '  box-shadow:none!important;'
                '  color:#e8b4b8!important;'
                '  font-size:0.73rem!important;'
                '  font-weight:500!important;'
                '  padding:2px 4px!important;'
                '  min-height:0!important;'
                '  text-decoration:underline!important;'
                '  text-underline-offset:3px!important;'
                '}'
                'section[data-testid="stSidebar"] .stDownloadButton button:hover,'
                'section[data-testid="stSidebar"] .stButton > button:not([data-testid="baseButton-primary"]):hover {'
                '  color:#efd9ce!important;'
                '  background:transparent!important;'
                '  background-color:transparent!important;'
                '  transform:none!important;'
                '  filter:none!important;'
                '}'
                '</style>',
                unsafe_allow_html=True,
            )
        else:
            # 未连接：显示上传框 + 连接按钮
            uploaded = st.file_uploader(
                "上传 .kuzu 文件",
                type=["kuzu"],
                help="Kuzu 0.11+ 单文件库",
                accept_multiple_files=False,
            )

            if uploaded is not None:
                sig = hashlib.sha256(uploaded.getvalue()).hexdigest() + (uploaded.name or "")
                if st.session_state.get("_kuzu_upload_sig") != sig:
                    remove_tree_quiet(st.session_state.get("kuzu_upload_cleanup_dir"))
                    path_u, cleanup_u, err_u = save_kuzu_upload_to_temp(
                        uploaded.getvalue(), uploaded.name or "upload.kuzu"
                    )
                    if err_u:
                        st.session_state.uploaded_kuzu_path = None
                        st.session_state.kuzu_upload_cleanup_dir = None
                        st.session_state._kuzu_upload_sig = None
                        st.error(err_u)
                    else:
                        st.session_state.uploaded_kuzu_path = path_u
                        st.session_state.kuzu_upload_cleanup_dir = cleanup_u
                        st.session_state._kuzu_upload_sig = sig
            elif st.session_state.get("_kuzu_upload_sig"):
                remove_tree_quiet(st.session_state.get("kuzu_upload_cleanup_dir"))
                st.session_state.uploaded_kuzu_path = None
                st.session_state.kuzu_upload_cleanup_dir = None
                st.session_state._kuzu_upload_sig = None
                st.session_state.kuzu_db = None
                st.session_state.kuzu_conn = None
                st.session_state.connected = False
                clear_schema_property_cache()
                st.rerun()

            if st.button("连接", type="primary", use_container_width=True):
                path_to_use = (st.session_state.get("uploaded_kuzu_path") or "").strip()
                if not path_to_use:
                    st.error("请先上传 .kuzu 文件，再点击连接。")
                    st.session_state.kuzu_db = None
                    st.session_state.kuzu_conn = None
                    st.session_state.connected = False
                    clear_schema_property_cache()
                else:
                    db, conn, err = connect_kuzu(path_to_use, read_only=read_only)
                    if err:
                        st.error(err)
                        st.session_state.kuzu_db = None
                        st.session_state.kuzu_conn = None
                        st.session_state.connected = False
                        clear_schema_property_cache()
                    else:
                        st.session_state.kuzu_db = db
                        st.session_state.kuzu_conn = conn
                        st.session_state.connected = True
                        clear_schema_property_cache()
                        st.rerun()

        if not _is_connected:
            pass  # 状态已在上方指示灯显示
        else:
            # ── 分割线 ──
            st.markdown(
                '<div style="height:1px;background:linear-gradient(90deg,transparent,#3d3b6e,transparent);'
                'margin:14px 0;"></div>',
                unsafe_allow_html=True,
            )

            # ── 导航 ──
            st.markdown(
                '<p style="font-size:0.65rem;font-weight:600;letter-spacing:0.12em;'
                'text-transform:uppercase;color:#5a5890;margin:0 0 6px;">导航</p>',
                unsafe_allow_html=True,
            )
            current_page = st.radio(
                "page_nav",
                ["Query", "Schema", "Edit"],
                label_visibility="collapsed",
                key="kuzu_page_nav",
            )

            # ── 分割线 ──
            st.markdown(
                '<div style="height:1px;background:linear-gradient(90deg,transparent,#3d3b6e,transparent);'
                'margin:14px 0;"></div>',
                unsafe_allow_html=True,
            )

            # ── LLM 配置（折叠） ──
            st.markdown(
                '<p style="font-size:0.65rem;font-weight:600;letter-spacing:0.12em;'
                'text-transform:uppercase;color:#5a5890;margin:0 0 4px;">LLM</p>',
                unsafe_allow_html=True,
            )
            with st.expander("模型配置", expanded=False):
                st.caption("已接入默认大模型；仅需切换时修改。")
                st.text_input(
                    "API Base URL",
                    key="kuzu_llm_base_url",
                    help="OpenAI 兼容地址，默认已填；一般保持即可。",
                )
                st.text_input(
                    "模型名称",
                    key="kuzu_llm_model",
                    help="默认 SenseAuto-Chat。",
                )
                st.text_input(
                    "API Key",
                    type="password",
                    key="kuzu_llm_api_key",
                    help="留空则使用内置默认 Key。",
                )

    if not st.session_state.connected or st.session_state.kuzu_conn is None:
        # ---- Landing page ----
        st.markdown(
            '<style>\n'
            '.kz-lp-wrap { max-width: 980px; margin: 0 auto; padding: 12px 16px 48px; }\n'
            '</style>\n'
            '\n'
            '<div class="kz-lp-wrap">\n'
            '  <div style="display:flex;align-items:center;gap:28px;padding:48px 12px 24px;">\n'
            '    <div style="flex:1;">\n'
            '      <h1 style="font-size:clamp(2rem,5vw,2.85rem);font-weight:700;letter-spacing:-0.03em;margin:0 0 12px;color:#efd9ce;line-height:1.15;">探索你的<br/>图数据世界</h1>\n'
            '      <p style="font-size:1.05rem;color:#9b97c4;max-width:400px;margin:0 0 18px;line-height:1.65;">上传 .kuzu 即可连接，用自然语言与 Cypher 探索节点与关系。</p>\n'
            '      <div id="kz-start-btn" style="display:inline-block;background:linear-gradient(135deg,#e8b4b8,#efd9ce);border-radius:10px;padding:10px 24px;font-size:0.95rem;font-weight:600;color:#1a1a2e;cursor:pointer;">开始探索 →</div>\n'
            '    </div>\n'
            '    <div style="width:180px;height:180px;position:relative;flex-shrink:0;">\n'
            '      <div style="position:absolute;top:15px;left:75px;width:40px;height:40px;border-radius:50%;background:#e8b4b8;box-shadow:0 0 20px rgba(232,180,184,0.4);"></div>\n'
            '      <div style="position:absolute;top:75px;left:15px;width:34px;height:34px;border-radius:50%;background:#67e8f9;box-shadow:0 0 20px rgba(103,232,249,0.3);"></div>\n'
            '      <div style="position:absolute;top:115px;left:95px;width:32px;height:32px;border-radius:50%;background:#efd9ce;box-shadow:0 0 20px rgba(239,217,206,0.3);"></div>\n'
            '      <div style="position:absolute;top:45px;right:15px;width:30px;height:30px;border-radius:50%;background:#c084fc;box-shadow:0 0 20px rgba(192,132,252,0.3);"></div>\n'
            '      <svg style="position:absolute;top:0;left:0;width:100%;height:100%;opacity:0.4;" viewBox="0 0 180 180">\n'
            '        <line x1="95" y1="35" x2="32" y2="92" stroke="#5a5890" stroke-width="2"/>\n'
            '        <line x1="95" y1="35" x2="111" y2="131" stroke="#5a5890" stroke-width="2"/>\n'
            '        <line x1="32" y1="92" x2="111" y2="131" stroke="#5a5890" stroke-width="2"/>\n'
            '        <line x1="95" y1="35" x2="150" y2="60" stroke="#5a5890" stroke-width="2"/>\n'
            '        <line x1="150" y1="60" x2="111" y2="131" stroke="#5a5890" stroke-width="2"/>\n'
            '      </svg>\n'
            '    </div>\n'
            '  </div>\n'
            '\n'
            '  <div style="display:flex;gap:10px;align-items:center;padding:14px 12px;border-top:1px solid #3d3b6e;margin-bottom:18px;">\n'
            '    <div style="display:flex;align-items:center;gap:6px;">\n'
            '      <div style="width:22px;height:22px;border-radius:6px;background:linear-gradient(135deg,#e8b4b8,#efd9ce);display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#1a1a2e;">1</div>\n'
            '      <span style="font-size:0.85rem;color:#d4d0f0;">上传</span>\n'
            '    </div>\n'
            '    <div style="flex:1;height:1px;background:#3d3b6e;"></div>\n'
            '    <div style="display:flex;align-items:center;gap:6px;">\n'
            '      <div style="width:22px;height:22px;border-radius:6px;background:linear-gradient(135deg,#e8b4b8,#efd9ce);display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#1a1a2e;">2</div>\n'
            '      <span style="font-size:0.85rem;color:#d4d0f0;">连接</span>\n'
            '    </div>\n'
            '    <div style="flex:1;height:1px;background:#3d3b6e;"></div>\n'
            '    <div style="display:flex;align-items:center;gap:6px;">\n'
            '      <div style="width:22px;height:22px;border-radius:6px;background:linear-gradient(135deg,#e8b4b8,#efd9ce);display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#1a1a2e;">3</div>\n'
            '      <span style="font-size:0.85rem;color:#d4d0f0;">探索</span>\n'
            '    </div>\n'
            '  </div>\n'
            '\n'
            '  <div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap;padding:0 12px;">\n'
            '    <div style="display:flex;align-items:center;gap:6px;background:rgba(61,59,110,0.2);border:1px solid rgba(61,59,110,0.4);border-radius:20px;padding:6px 14px;">\n'
            '      <span style="font-size:14px;">✨</span>\n'
            '      <span style="font-size:0.85rem;color:#9b97c4;font-weight:500;">NL → Cypher</span>\n'
            '    </div>\n'
            '    <div style="display:flex;align-items:center;gap:6px;background:rgba(61,59,110,0.2);border:1px solid rgba(61,59,110,0.4);border-radius:20px;padding:6px 14px;">\n'
            '      <span style="font-size:14px;">🕸️</span>\n'
            '      <span style="font-size:0.85rem;color:#9b97c4;font-weight:500;">力导向图</span>\n'
            '    </div>\n'
            '    <div style="display:flex;align-items:center;gap:6px;background:rgba(61,59,110,0.2);border:1px solid rgba(61,59,110,0.4);border-radius:20px;padding:6px 14px;">\n'
            '      <span style="font-size:14px;">📋</span>\n'
            '      <span style="font-size:0.85rem;color:#9b97c4;font-weight:500;">属性面板</span>\n'
            '    </div>\n'
            '    <div style="display:flex;align-items:center;gap:6px;background:rgba(61,59,110,0.2);border:1px solid rgba(61,59,110,0.4);border-radius:20px;padding:6px 14px;">\n'
            '      <span style="font-size:14px;">🗺️</span>\n'
            '      <span style="font-size:0.85rem;color:#9b97c4;font-weight:500;">Schema 视图</span>\n'
            '    </div>\n'
            '  </div>\n'
            '</div>',
            unsafe_allow_html=True,
        )
        import streamlit.components.v1 as components
        components.html(
            '<script>'
            'var doc = window.parent.document;'
            'var btn = doc.getElementById("kz-start-btn");'
            'if (btn && !btn._kzBound) {'
            '  btn._kzBound = true;'
            '  btn.style.cursor = "pointer";'
            '  btn.addEventListener("click", function() {'
            '    var b = doc.querySelector('
            '      \'section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button\''
            '    );'
            '    if (b) b.click();'
            '  });'
            '}'
            '</script>',
            height=0,
        )
        st.stop()

    conn = st.session_state.kuzu_conn
    node_tables, rel_tables, schema_err = read_node_rel_table_names(conn)

    if current_page == "Schema":
        render_schema_view(conn, node_tables, rel_tables, schema_err)
        st.stop()

    if current_page == "Edit":
        render_mock_view(conn, node_tables, rel_tables, read_only=read_only)
        st.stop()

    # ---------- Query ----------
    get_schema_property_maps_cached(conn, node_tables, rel_tables)

    if "_cypher_text" not in st.session_state:
        st.session_state["_cypher_text"] = DEFAULT_CYPHER

    st.markdown(
        """
        <div class="kz-hero-band">
          <div class="kz-title">查询工作台</div>
          <div class="kz-sub">自然语言可生成 Cypher；也可直接编辑语句，一键执行并出图。</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col_nl, col_cy = st.columns(2, gap="medium")

    with col_nl:
        st.markdown('<p class="kz-card-title">自然语言</p>', unsafe_allow_html=True)
        st.caption("输入需求，点击生成 Cypher 并自动执行。")
        nl_question = st.text_area(
            "自然语言需求",
            height=140,
            key="nl_cypher_question",
            label_visibility="collapsed",
            placeholder="例如：查询张三相关的所有节点与关系；或统计各节点表数量。",
        )
        gen_and_run = st.button("生成 Cypher 并执行", type="secondary")

    with col_cy:
        st.markdown('<p class="kz-card-title">Cypher 编辑器</p>', unsafe_allow_html=True)
        st.caption("可查看或编辑语句；修改后点「执行查询」。")
        # sync session state for cypher text
        if "_cypher_box_pending" in st.session_state:
            st.session_state["_cypher_text"] = st.session_state.pop("_cypher_box_pending")
        st.session_state["cypher_box"] = st.session_state["_cypher_text"]
        cypher_text = st.text_area(
            "Cypher 语句",
            height=140,
            key="cypher_box",
            label_visibility="collapsed",
        )
        st.session_state["_cypher_text"] = cypher_text
        run_clicked = st.button("执行查询", type="primary")

    if "last_query" not in st.session_state:
        st.session_state.last_query = None

    # 本轮 NL 生成成功后在本页底部统一执行查询（须先渲染 Cypher 输入框，否则大图/表格会顶掉控件）
    _nl_query_to_run: str | None = None

    if gen_and_run:
        nl_q = (nl_question or "").strip()
        if not nl_q:
            st.warning("请先输入自然语言描述。")
        else:
            api_base = str(st.session_state.get("kuzu_llm_base_url") or DEFAULT_LLM_BASE_URL).strip()
            model = str(st.session_state.get("kuzu_llm_model") or DEFAULT_LLM_MODEL).strip()
            api_key_sidebar = str(st.session_state.get("kuzu_llm_api_key") or "").strip()
            api_key_eff = api_key_sidebar or DEFAULT_LLM_API_KEY.strip()
            nfields, rfields, npk, rpk, _, _ = get_schema_property_maps_cached(
                conn, node_tables, rel_tables
            )
            schema_md = format_schema_for_cypher_llm(
                conn, node_tables, rel_tables, nfields, rfields, npk, rpk
            )
            if schema_err and not node_tables and not rel_tables:
                st.warning(f"Schema 读取异常（{schema_err}），仍将把已知片段发给模型。")
            with st.spinner("正在生成 Cypher（大模型处理中，请稍候）…"):
                cy, llm_err = generate_cypher_via_llm(
                    user_question=nl_q,
                    schema_markdown=schema_md,
                    api_base=api_base,
                    api_key=api_key_eff,
                    model=model or DEFAULT_LLM_MODEL,
                )
            if llm_err:
                st.error(llm_err)
            else:
                q = (cy or "").strip()
                st.session_state["_cypher_box_pending"] = cy
                st.session_state.last_query = q
                _nl_query_to_run = q

    _did_query = False
    _scroll_to_graph = st.session_state.pop("_scroll_to_graph", False)
    if run_clicked:
        query_to_run = (cypher_text or "").strip()
        if not query_to_run:
            st.warning("查询为空。")
        else:
            st.session_state.last_query = query_to_run
            with st.spinner("正在执行查询并渲染结果…"):
                render_query_results(conn, query_to_run, node_tables, rel_tables)
            _did_query = True
    elif _nl_query_to_run is not None:
        with st.spinner("正在执行查询并渲染结果…"):
            render_query_results(conn, _nl_query_to_run, node_tables, rel_tables)
        st.session_state["_scroll_to_graph"] = True
        st.rerun()
    elif st.session_state.last_query:
        lq = (st.session_state.last_query or "").strip()
        cached_q = (st.session_state.get("_query_result_cache_query") or "").strip()
        cached_df = st.session_state.get("_query_result_cache_df")
        if lq and cached_q == lq and cached_df is not None:
            render_query_results(
                conn,
                st.session_state.last_query,
                node_tables,
                rel_tables,
                precomputed_df=cached_df,
            )
        else:
            render_query_results(conn, st.session_state.last_query, node_tables, rel_tables)

    if _did_query or _scroll_to_graph:
        import streamlit.components.v1 as _comp
        import time as _time
        _comp.html(
            '<script>'
            'var el = window.parent.document.getElementById("kz-graph-preview");'
            'if (el) el.scrollIntoView({behavior:"smooth",block:"start"});'
            '</script>'
            f'<!-- {_time.time()} -->',
            height=0,
        )


if __name__ == "__main__":
    main()
