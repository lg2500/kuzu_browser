# -*- coding: utf-8 -*-
"""Pyvis 图渲染与 Explorer 风格侧栏 HTML 注入。"""

from __future__ import annotations

import ast
import base64
import colorsys
import hashlib
import html as html_module
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from pyvis.network import Network


_NODE_TYPE_COLOR_MAP: Dict[str, Dict[str, Any]] = {
    "person": {
        "background": "#e8b4b8",
        "border": "#f0c8cb",
        "highlight": {"background": "#f0c8cb", "border": "#d4a0a5"},
        "shadow": "rgba(232, 180, 184, 0.42)",
    },
    "attribute": {
        "background": "#2da8a8",
        "border": "#5ec4c4",
        "highlight": {"background": "#45b8b8", "border": "#6dcfcf"},
        "shadow": "rgba(45, 168, 168, 0.4)",
    },
    "attributes": {
        "background": "#2da8a8",
        "border": "#5ec4c4",
        "highlight": {"background": "#45b8b8", "border": "#6dcfcf"},
        "shadow": "rgba(45, 168, 168, 0.4)",
    },
    "preference": {
        "background": "#c084fc",
        "border": "#d8b4fe",
        "highlight": {"background": "#d8b4fe", "border": "#b08ad8"},
        "shadow": "rgba(192, 132, 252, 0.42)",
    },
    "preferences": {
        "background": "#c084fc",
        "border": "#d8b4fe",
        "highlight": {"background": "#d8b4fe", "border": "#b08ad8"},
        "shadow": "rgba(192, 132, 252, 0.42)",
    },
    "preferrence": {
        "background": "#c084fc",
        "border": "#d8b4fe",
        "highlight": {"background": "#d8b4fe", "border": "#b08ad8"},
        "shadow": "rgba(192, 132, 252, 0.42)",
    },
    "preferrences": {
        "background": "#c084fc",
        "border": "#d8b4fe",
        "highlight": {"background": "#d8b4fe", "border": "#b08ad8"},
        "shadow": "rgba(192, 132, 252, 0.42)",
    },
}


def _node_visual_type(panel: Dict[str, Any]) -> str:
    """优先使用节点 attributes.type；缺失时再回退。"""
    rows = panel.get("rows") or []

    def _extract_from_attributes(raw: Any) -> Optional[str]:
        candidate = raw
        if isinstance(candidate, str):
            txt = candidate.strip()
            if not txt or txt == "（空）":
                return None
            for parser in (json.loads, ast.literal_eval):
                try:
                    candidate = parser(txt)
                    break
                except Exception:  # noqa: BLE001
                    candidate = txt
            if isinstance(candidate, str):
                return None
        if isinstance(candidate, dict):
            nested_type = candidate.get("type")
            if nested_type is not None:
                nested = str(nested_type).strip().lower()
                if nested:
                    return nested
        return None

    for row in rows:
        key = str(row.get("key") or "").strip().lower()
        if key == "attributes":
            nested = _extract_from_attributes(row.get("value"))
            if nested:
                return nested
    for row in rows:
        if str(row.get("key") or "").strip().lower() == "type":
            val = str(row.get("value") or "").strip()
            if val and val != "（空）":
                return val.lower()
    return str(panel.get("label_type") or "node").strip().lower()


def _rgb_to_hex(r: float, g: float, b: float) -> str:
    return "#{:02x}{:02x}{:02x}".format(
        max(0, min(255, int(round(r * 255)))),
        max(0, min(255, int(round(g * 255)))),
        max(0, min(255, int(round(b * 255)))),
    )


def _schema_color_map(schema_name: str) -> Dict[str, Any]:
    key = str(schema_name or "node").strip().lower() or "node"
    digest = hashlib.md5(key.encode("utf-8")).digest()
    hue = digest[0] / 255.0
    saturation = 0.68 + ((digest[1] / 255.0) * 0.12)

    bg_r, bg_g, bg_b = colorsys.hls_to_rgb(hue, 0.47, saturation)
    border_r, border_g, border_b = colorsys.hls_to_rgb(hue, 0.76, min(1.0, saturation * 0.9))
    hi_r, hi_g, hi_b = colorsys.hls_to_rgb(hue, 0.58, min(1.0, saturation * 0.95))

    hi_border_r, hi_border_g, hi_border_b = colorsys.hls_to_rgb(hue, 0.65, min(1.0, saturation * 0.85))

    return {
        "background": _rgb_to_hex(bg_r, bg_g, bg_b),
        "border": _rgb_to_hex(border_r, border_g, border_b),
        "highlight": {
            "background": _rgb_to_hex(hi_r, hi_g, hi_b),
            "border": _rgb_to_hex(hi_border_r, hi_border_g, hi_border_b),
        },
        "shadow": f"rgba({int(round(bg_r * 255))}, {int(round(bg_g * 255))}, {int(round(bg_b * 255))}, 0.42)",
    }


def _hex_luminance(hex_color: str) -> float:
    """Calculate relative luminance of a hex color (0=dark, 1=bright)."""
    h = hex_color.lstrip("#")
    if len(h) != 6:
        return 0.5
    r, g, b = int(h[0:2], 16) / 255.0, int(h[2:4], 16) / 255.0, int(h[4:6], 16) / 255.0
    return 0.299 * r + 0.587 * g + 0.114 * b


def _node_color_config(panel: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    schema_label = str(panel.get("label_type") or "node").strip().lower()
    visual_type = _node_visual_type(panel)
    mapped = _NODE_TYPE_COLOR_MAP.get(schema_label) or _NODE_TYPE_COLOR_MAP.get(visual_type)
    if mapped is None:
        mapped = _schema_color_map(schema_label)
    return (
        {
            "background": mapped["background"],
            "border": mapped["border"],
            "highlight": mapped["highlight"],
        },
        {"enabled": True, "color": mapped["shadow"], "size": 15, "x": 0, "y": 0},
    )


def _panel_b64_payload(node_payloads: Dict[str, Any], edge_payloads: Dict[str, Any]) -> str:
    """嵌入页面：Base64(JSON)，避免 </script> 与引号截断。"""
    raw = json.dumps(
        {"nodes": node_payloads, "edges": edge_payloads},
        ensure_ascii=False,
    ).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def _inject_explorer_panel_and_click_handler(
    html_doc: str,
    panel_b64: str,
    panel_hint: str = "点击节点或边查看属性",
    *,
    physics_stays_on: bool = False,
    disable_internal_panel: bool = False,
    allow_fullscreen_shortcuts: bool = False,
) -> str:
    """
    在 Pyvis 生成的页面上叠加 Kuzu Explorer 风格右侧面板，并用 vis 的 click 事件填充。
    """
    extra_css = """
      /* 铺满嵌入页：彻底消除物理白边与 Bootstrap 留白 */
      html, body {
        margin: 0 !important;
        padding: 0 !important;
        background: transparent !important;
        background-color: transparent !important;
        min-height: 100vh !important;
        height: 100% !important;
        overflow: hidden !important;
      }
      body > center {
        margin: 0 !important;
        padding: 0 !important;
        width: 100% !important;
        background: transparent !important;
      }
      /* 强制穿透覆盖可能的容器背景及边框 */
      .card, .card-header, #header, .container-fluid, .row, [class*="col-"], .card-body, .vis-network {
        background: transparent !important;
        background-color: transparent !important;
        border: 0 none transparent !important;
        box-shadow: none !important;
        outline: none !important;
        margin: 0 !important;
        padding: 0 !important;
        max-width: none !important;
      }
      .kuzu-outer {
        position: relative !important;
        width: 100% !important;
        height: 100vh !important;
        background: #1a1a2e !important;
        display: flex !important;
        flex-direction: column !important;
        overflow: hidden !important;
        /* Removed border to eliminate double-edge artifacts */
      }
      #mynetwork {
        flex: 1 !important;
        width: 100% !important;
        background: #1a1a2e !important;
        border: 0 none transparent !important;
        outline: none !important;
        margin: 0 !important;
        padding-top: 10px !important;
      }

      /* === Vis.js Manipulation UI — hide default, use custom button === */
      .vis-manipulation, .vis-edit-mode { display: none !important; }
      #kz-add-edge-btn {
        position: absolute;
        top: 12px;
        left: 12px;
        z-index: 1000;
        background: rgba(45, 43, 85, 0.95);
        border: 1px solid #5a5890;
        border-radius: 20px;
        padding: 6px 18px;
        color: #efd9ce;
        font-weight: 600;
        font-size: 13px;
        cursor: pointer;
        box-shadow: 0 4px 16px rgba(0,0,0,0.4);
        transition: all 0.2s ease;
        user-select: none;
      }
      #kz-add-edge-btn:hover {
        background: rgba(232, 180, 184, 0.2);
        border-color: #e8b4b8;
      }
      #kz-add-edge-btn.active {
        background: #e8b4b8;
        color: #1a1a2e;
        border-color: #e8b4b8;
      }

      /* Right detail panel styling */
      #kuzu-detail-hint {
        position: absolute;
        bottom: 24px;
        left: 20px;
        z-index: 99;
        background: rgba(45, 43, 85, 0.9);
        border: 1px solid #5a5890;
        border-radius: 12px;
        padding: 10px 18px;
        color: #e8b4b8;
        font-family: inherit;
        font-size: 14px;
        font-weight: 600;
        box-shadow: 0 4px 16px rgba(0,0,0,0.3);
        pointer-events: none;
      }
      #kuzu-detail-panel {
        position: absolute;
        top: 60px;
        right: 12px;
        width: 310px;
        bottom: 12px;
        z-index: 100;
        background: rgba(45, 43, 85, 0.95);
        border: 1px solid #5a5890;
        border-radius: 14px;
        box-shadow: -4px 0 16px rgba(0,0,0,0.3);
        overflow: auto;
        font-family: inherit;
        color: #d4d0f0;
        padding: 16px;
        display: none;
      }

      #kuzu-detail-panel.visible { display: block; }
      #kuzu-detail-hint {
        font-size: 13px; font-weight: 500; color: #e8b4b8;
        background: rgba(45,43,85,0.8);
        padding: 8px 14px; border-radius: 8px; pointer-events: none;
        border: 1px solid #5a5890;
        box-shadow: 0 4px 16px rgba(0,0,0,0.3);
      }
      .kuzu-badge {
        display: inline-block;
        background: #e8b4b8;
        color: #1a1a2e;
        font-size: 11px;
        font-weight: 600;
        padding: 3px 10px;
        border-radius: 999px;
        letter-spacing: 0.02em;
      }
      .kuzu-kind { font-size: 13px; color: #d4d0f0; margin: 8px 0 12px; }
      .kuzu-ref-block {
        font-size: 11px;
        color: #9b97c4;
        border-bottom: 1px solid #3d3b6e;
        padding-bottom: 10px;
        margin-bottom: 12px;
      }
      .kuzu-ref-val { color: #efd9ce; word-break: break-all; margin-top: 4px; font-size: 12px; }
      .kuzu-sec-title { font-size: 12px; font-weight: 600; color: #efd9ce; margin: 14px 0 10px; }
      .kuzu-kv {
        display: grid;
        grid-template-columns: minmax(72px, 32%) 1fr;
        gap: 6px 12px;
        font-size: 12px;
        padding: 8px 0;
        border-top: 1px solid #3d3b6e;
        align-items: start;
      }
      .kuzu-k { color: #9b97c4; text-align: left; }
      .kuzu-v { color: #efd9ce; text-align: right; white-space: pre-wrap; word-break: break-word; }
      .kuzu-pk {
        display: inline-block;
        margin-left: 6px;
        font-size: 9px;
        font-weight: 700;
        padding: 1px 5px;
        border-radius: 3px;
        background: #c084fc;
        color: #fff;
        vertical-align: middle;
      }
      .kuzu-kv-head {
        font-weight: 600;
        border-top: none !important;
        padding-top: 0 !important;
        margin-bottom: 2px;
      }
      .kuzu-kv-head .kuzu-k, .kuzu-kv-head .kuzu-v {
        color: #d4d0f0;
        font-size: 11px;
        text-transform: none;
      }
    """

    click_js = f"""
      (function() {{
        /* Base64(JSON 的 UTF-8 字节)。仅用 atob + JSON.parse 会把多字节汉字弄乱码，必须先按 UTF-8 解码。 */
        function utf8JsonFromB64(b64) {{
          var bin = atob(b64);
          if (typeof TextDecoder !== "undefined") {{
            var u8 = new Uint8Array(bin.length);
            for (var i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i) & 0xff;
            return new TextDecoder("utf-8").decode(u8);
          }}
          return decodeURIComponent(escape(bin));
        }}
        function decodePanel() {{
          try {{
            return JSON.parse(utf8JsonFromB64("{panel_b64}"));
          }} catch (e) {{ return {{ nodes: {{}}, edges: {{}} }}; }}
        }}
        var KUZU_PANEL = decodePanel();
        function esc(s) {{
          if (s === null || s === undefined) return "";
          return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
        }}
        function renderPanel(payload) {{
          var el = document.getElementById("kuzu-detail-panel");
          if (!payload || !el) return;
          var h = "";
          h += '<div><span class="kuzu-badge">' + esc(payload.label_type) + '</span></div>';
          var kindLine = payload.schema_mode
            ? (payload.entity === "node" ? "节点类型（Node table）" : "关系类型（Rel table）")
            : (payload.entity === "node" ? "Node" : "Relationship");
          h += '<div class="kuzu-kind">' + kindLine + '</div>';
          h += '<div class="kuzu-ref-block"><div>说明</div><div class="kuzu-ref-val">' + esc(payload.internal_ref) + '</div></div>';
          h += '<div class="kuzu-sec-title">' + esc(payload.properties_title) + '</div>';
          var rows = payload.rows || [];
          if (payload.schema_mode && payload.column_headers && payload.column_headers.length >= 2) {{
            h += '<div class="kuzu-kv kuzu-kv-head">';
            h += '<div class="kuzu-k">' + esc(payload.column_headers[0]) + '</div>';
            h += '<div class="kuzu-v">' + esc(payload.column_headers[1]) + '</div>';
            h += '</div>';
          }}
          for (var i = 0; i < rows.length; i++) {{
            var r = rows[i];
            h += '<div class="kuzu-kv">';
            h += '<div class="kuzu-k">' + esc(r.key) + (r.pk ? '<span class="kuzu-pk">PK</span>' : '') + '</div>';
            h += '<div class="kuzu-v">' + esc(r.value) + '</div>';
            h += '</div>';
          }}
          el.innerHTML = h;
          el.classList.add("visible");
        }}
        function emitBridgeEvent(payload) {{
          try {{
            for (var i = 0; i < window.parent.frames.length; i++) {{
              window.parent.frames[i].postMessage({{kuzu_click: payload}}, "*");
            }}
          }} catch(e) {{}}
        }}
        var _kzHooked = false;
        function hookNetworkClick() {{
          if (_kzHooked || typeof network === "undefined" || !network) {{
            if (!_kzHooked) setTimeout(hookNetworkClick, 80);
            return;
          }}
          _kzHooked = true;
          var edgeDragState = {{
            active: false,
            edgeId: "",
            fromId: "",
            toId: "",
            startCanvas: null,
            startSmooth: null,
          }};
          var suppressClickUntil = 0;
          var manualEdgeRoute = {{}};
          var rerouteTimer = null;
          var rerouting = false;

          function clamp(v, lo, hi) {{
            return Math.max(lo, Math.min(hi, v));
          }}

          function hashText(s) {{
            var txt = String(s || "");
            var h = 0;
            for (var i = 0; i < txt.length; i++) {{
              h = ((h << 5) - h + txt.charCodeAt(i)) | 0;
            }}
            return Math.abs(h);
          }}

          function parseSmooth(s) {{
            if (!s || typeof s !== "object") return {{ enabled: false, type: "curvedCW", roundness: 0.25 }};
            var t = String(s.type || "curvedCW");
            if (t !== "curvedCW" && t !== "curvedCCW") t = "curvedCW";
            var r = Number(s.roundness || 0.25);
            return {{ enabled: !!s.enabled, type: t, roundness: clamp(r, 0.02, 0.95) }};
          }}

          function canonicalEdgeId(rawId) {{
            var s = String(rawId || "");
            var p = s.indexOf("__seg__");
            return p >= 0 ? s.slice(0, p) : s;
          }}

          function segmentIds(eid) {{
            return {{
              a: eid + "__seg__a",
              b: eid + "__seg__b"
            }};
          }}

          function edgeDataset() {{
            return (network.body && network.body.data && network.body.data.edges) ? network.body.data.edges : null;
          }}

          function nodeDataset() {{
            return (network.body && network.body.data && network.body.data.nodes) ? network.body.data.nodes : null;
          }}

          function defaultRouteSign(seed) {{
            return (hashText(seed) % 2 === 0) ? 1 : -1;
          }}

          function scheduleEdgeReroute(delayMs) {{
            return;
          }}

          function rerouteEdges() {{
            return;
          }}

          function beginEdgeDrag(ev) {{
            return;
          }}

          function moveEdgeDrag(ev) {{
            return;
          }}

          function endEdgeDrag() {{
            return;
          }}

          if (network.canvas && network.canvas.frame && network.canvas.frame.canvas) {{
            var cv = network.canvas.frame.canvas;
            cv.tabIndex = 0;
            cv.style.outline = "none";
            cv.addEventListener("pointerdown", function() {{
              try {{ cv.focus(); }} catch(e) {{}}
            }});
            cv.addEventListener("pointerdown", beginEdgeDrag);
            cv.addEventListener("pointermove", moveEdgeDrag);
            cv.addEventListener("pointerup", endEdgeDrag);
            cv.addEventListener("pointerleave", endEdgeDrag);
            cv.addEventListener("pointercancel", endEdgeDrag);
            if ({str(allow_fullscreen_shortcuts).lower()}) {{
              cv.addEventListener("dblclick", function() {{
                emitBridgeEvent("fullscreen|enter");
              }});
            }}
          }}

          if ({str(allow_fullscreen_shortcuts).lower()}) {{
            function handleEscape(ev) {{
              if (!ev || ev.key !== "Escape") return;
              emitBridgeEvent("fullscreen|exit");
            }}
            window.addEventListener("keydown", handleEscape);
            document.addEventListener("keydown", handleEscape);
            network.on("doubleClick", function () {{
              emitBridgeEvent("fullscreen|enter");
            }});
          }}

          /* physics_stays_on：布局结束后仍保留力导，拖动节点时边像弹簧，邻居会跟着动 */
          if (!{str(physics_stays_on).lower()}) {{
            network.on("stabilizationIterationsDone", function () {{
              try {{ network.setOptions({{ physics: false }}); }} catch (e) {{}}
            }});
          }}
          network.on("click", function (params) {{
            if (Date.now() < suppressClickUntil) {{
              return;
            }}
            var panel = document.getElementById("kuzu-detail-panel");
            var hint = document.getElementById("kuzu-detail-hint");
            if (hint) hint.style.display = "none";
            if (params.nodes && params.nodes.length > 0) {{
              var id = params.nodes[0];
              if (!{str(disable_internal_panel).lower()}) {{
                 var payload = KUZU_PANEL.nodes[id];
                 if (payload) renderPanel(payload);
              }}
              emitBridgeEvent("node|" + String(id));
              return;
            }}
            if (params.edges && params.edges.length > 0) {{
              var eid = canonicalEdgeId(params.edges[0]);
              var payload = KUZU_PANEL.edges[eid];
              if (payload) {{
                if (!{str(disable_internal_panel).lower()}) renderPanel(payload);
                emitBridgeEvent("edge|" + String(eid));
              }} else {{
                var edata = network.body.data.edges.get(params.edges[0]);
                if (edata && edata.from && edata.to) {{
                  emitBridgeEvent("add_edge|" + edata.from + "|" + edata.to);
                }}
              }}
              return;
            }}
            if (panel) {{
              panel.classList.remove("visible");
              panel.innerHTML = "";
            }}
            emitBridgeEvent("");
          }});
        }}
        if (document.readyState === "loading") {{
          document.addEventListener("DOMContentLoaded", hookNetworkClick);
        }} else {{ hookNetworkClick(); }}

        // Incremental graph update listener
        window.addEventListener("message", function(ev) {{
          if (!ev.data || !ev.data.kuzu_cmd) return;
          var cmd = ev.data.kuzu_cmd;
          if (typeof network === "undefined" || !network) return;
          var nds = network.body.data.nodes;
          var eds = network.body.data.edges;
          if (cmd === "remove_edge" && ev.data.edge_id) {{
            try {{ eds.remove(ev.data.edge_id); }} catch(e) {{}}
          }}
          if (cmd === "remove_node" && ev.data.node_id) {{
            try {{
              var connected = network.getConnectedEdges(ev.data.node_id);
              if (connected) eds.remove(connected);
              nds.remove(ev.data.node_id);
            }} catch(e) {{}}
          }}
          if (cmd === "add_node" && ev.data.node) {{
            try {{ nds.add(ev.data.node); }} catch(e) {{}}
          }}
          if (cmd === "add_edge" && ev.data.edge) {{
            try {{ eds.add(ev.data.edge); }} catch(e) {{}}
          }}
          if (cmd === "update_node" && ev.data.node_id && ev.data.label) {{
            try {{ nds.update({{id: ev.data.node_id, label: ev.data.label}}); }} catch(e) {{}}
          }}
          if (cmd === "update_edge" && ev.data.edge_id) {{
            try {{
              var upd = {{id: ev.data.edge_id}};
              if (ev.data.label) upd.label = ev.data.label;
              if (ev.data.title) upd.title = ev.data.title;
              eds.update(upd);
            }} catch(e) {{}}
          }}
        }});
      }})();
    """

    if "</style>" in html_doc:
        html_doc = html_doc.replace("</style>", extra_css + "\n</style>", 1)

    wrapper = (
        '<div class="kuzu-outer"><div id="kuzu-detail-hint">'
        + html_module.escape(panel_hint)
        + '</div><aside id="kuzu-detail-panel" aria-live="polite"></aside>'
    )
    if '<div id="mynetwork" class="card-body"></div>' in html_doc:
        html_doc = html_doc.replace(
            '<div id="mynetwork" class="card-body"></div>',
            wrapper + '<div id="mynetwork" class="card-body"></div></div>',
            1,
        )
    elif 'id="mynetwork"' in html_doc:
        html_doc = re.sub(
            r'(<div id="mynetwork"[^>]*></div>)',
            wrapper + r"\1</div>",
            html_doc,
            count=1,
        )

    script = f'<script type="text/javascript">\n{click_js}\n</script>'
    if "</body>" in html_doc:
        html_doc = html_doc.replace("</body>", script + "\n</body>", 1)
    else:
        html_doc += script
    return html_doc


def _short_graph_label(text: str, max_chars: int) -> str:
    """画布上单行标签，避免 vis 按文字撑大节点；完整内容在 title / 侧栏。"""
    t = str(text).replace("\n", " ").strip()
    if not t:
        return " "
    if len(t) <= max_chars:
        return t
    return t[: max(1, max_chars - 1)] + "…"


def make_pyvis_html(
    nodes: Dict[str, Tuple[str, str, Dict[str, Any]]],
    edges: List[Tuple[str, str, str, str, str, Dict[str, Any]]],
    *,
    visual_theme: str = "data",
    panel_hint: Optional[str] = None,
    disable_internal_panel: bool = False,
    enable_manipulation: bool = False,
    allow_fullscreen_shortcuts: bool = False,
) -> str:
    """
    Pyvis 图 + 右侧 Explorer 风格面板。
    visual_theme: "data" | "schema"（粗白直线箭头；关系名 horizontal + vadjust 抬离边，避免白线穿字；浅灰底黑字）。
    """
    node_payloads = {nid: tpl[2] for nid, tpl in nodes.items()}
    edge_payloads: Dict[str, Any] = {}
    for _s, _d, eid, _lb, _ti, panel in edges:
        edge_payloads[eid] = panel

    if panel_hint is None:
        panel_hint = (
            "点击节点表或关系连线查看 Name / Type"
            if visual_theme == "schema"
            else "点击节点或边查看属性"
        )

    net = Network(
        height="600px",
        width="100%",
        bgcolor="#1a1a2e",
        font_color=False,
        directed=True,
    )
    net.toggle_physics(True)

    _spring = 180 if visual_theme == "schema" else 190
    _font_face = '"PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", ui-sans-serif, sans-serif'
    # Use a Python dict instead of a JSON string to bypass Pyvis's buggy
    # Options.set() which does .replace(" ", "") before json.loads(),
    # corrupting string values that contain spaces (e.g. font face names).
    # physics 在布局完成后保持开启：拖动节点时边作弹簧，邻居会受力跟随（类似持续力导向，非真 3D）。
    _opts = {
        "physics": {
            "enabled": True,
            "solver": "barnesHut",
            "barnesHut": {
                "gravitationalConstant": -6000,
                "centralGravity": 0.05,
                "springLength": 350,
                "springConstant": 0.02,
                "damping": 0.75,
                "avoidOverlap": 1
            },
            "stabilization": {
                "enabled": True,
                "iterations": 2500,
                "updateInterval": 25,
                "fit": True,
            },
            "maxVelocity": 8,
            "minVelocity": 0.3,
            "timestep": 0.3,
        },
        "interaction": {
            "hover": True,
            "tooltipDelay": 120,
            "selectConnectedEdges": False,
            "dragNodes": True,
            "dragView": True,
            "zoomView": True,
            "hideEdgesOnDrag": False,
            "hideNodesOnDrag": False,
        },
        "manipulation": False,
        "edges": {
            "width": 3,
            "arrowStrikethrough": False,
            "endPointOffset": {"from": 6, "to": 8},
            "font": {
                "size": 15,
                "align": "middle",
                "color": "#efd9ce",
                "strokeWidth": 0,
                "vadjust": -18,
                "face": _font_face,
            },
            "smooth": {"enabled": False},
            "color": {"color": "rgba(155, 151, 196, 0.5)", "highlight": "#e8b4b8"},
            "arrows": {"to": {"enabled": True, "scaleFactor": 0.5}},
            "shadow": {"enabled": False},
        },
        "nodes": {
            "shape": "circle",
            "font": {
                "size": 16,
                "color": "#efd9ce",
                "face": _font_face,
                "vadjust": 0,
                "align": "center",
            },
            "widthConstraint": {"minimum": 85, "maximum": 85},
            "heightConstraint": {"minimum": 85, "maximum": 85},
            "borderWidth": 2,
            "borderWidthSelected": 3,
            "color": {
                "background": "#e8b4b8",
                "border": "#f0c8cb",
                "highlight": {"background": "#f0c8cb", "border": "#d4a0a5"},
            },
            "shadow": {"enabled": True, "color": "rgba(232, 180, 184, 0.25)", "size": 15, "x": 0, "y": 0},
        },
    }
    net.options = _opts

    _fixed_node_size = 45
    _node_label_max_chars = 10
    node_font = {
        "size": 14,
        "color": "#efd9ce",
        "face": _font_face,
        "vadjust": 0,
        "align": "center",
    }
    _edge_width = 3.0
    _edge_line = "rgba(155, 151, 196, 0.5)"
    edge_font = {
        "size": 15,
        "align": "middle",
        "color": "#efd9ce",
        "strokeWidth": 0,
        "vadjust": -18,
        "face": _font_face,
    }

    for nid, (canvas_label, title, panel) in nodes.items():
        lab = _short_graph_label(canvas_label, _node_label_max_chars)
        full = str(canvas_label).strip()
        orig_tip = str(title).strip() if title else ""
        if full != lab:
            tip = "\n".join(x for x in (full, orig_tip) if x) or full
        else:
            tip = orig_tip or full or lab
        node_color, node_shadow = _node_color_config(panel)
        nf = node_font
        net.add_node(
            nid,
            label=lab,
            title=tip or " ",
            shape="circle",
            widthConstraint={"minimum": 85, "maximum": 85},
            heightConstraint={"minimum": 85, "maximum": 85},
            font=nf,
            color=node_color,
            shadow=node_shadow,
        )

    edge_group_indices: Dict[Tuple[str, str], List[int]] = {}
    edge_items: List[Tuple[str, str, str, str, str, Dict[str, Any]]] = []
    for src, dst, eid, elab, etitle, panel in edges:
        s_key, d_key = str(src), str(dst)
        pair_key = (s_key, d_key) if s_key <= d_key else (d_key, s_key)
        edge_group_indices.setdefault(pair_key, []).append(len(edge_items))
        edge_items.append((src, dst, eid, elab, etitle, panel))

    edge_lane_by_index: Dict[int, int] = {}
    for idx_list in edge_group_indices.values():
        if len(idx_list) <= 1:
            edge_lane_by_index[idx_list[0]] = 0
            continue
        slots: List[int] = []
        mag = 1
        while len(slots) < len(idx_list):
            slots.append(mag)
            if len(slots) < len(idx_list):
                slots.append(-mag)
            mag += 1
        for local_pos, edge_idx in enumerate(idx_list):
            edge_lane_by_index[edge_idx] = slots[local_pos]

    edge_group_size_by_index = {
        edge_idx: len(idx_list)
        for idx_list in edge_group_indices.values()
        for edge_idx in idx_list
    }

    for edge_idx, (src, dst, eid, elab, etitle, _panel) in enumerate(edge_items):
        s, d = str(src), str(dst)
        lab_e = _short_graph_label(elab, 36)
        full_e = str(elab).strip()
        orig_etip = str(etitle).strip() if etitle else ""
        if full_e != lab_e:
            etip = "\n".join(x for x in (full_e, orig_etip) if x) or full_e
        else:
            etip = orig_etip or full_e or lab_e
        lane = edge_lane_by_index.get(edge_idx, 0)
        pair_size = edge_group_size_by_index.get(edge_idx, 1)
        if lane == 0:
            net.add_edge(
                s,
                d,
                id=eid,
                label=lab_e,
                title=etip or " ",
                color=_edge_line,
                width=_edge_width,
                font=edge_font,
                arrowStrikethrough=False,
                smooth={"enabled": False},
                kuzuSrc=s,
                kuzuDst=d,
                kuzuLane=lane,
                kuzuPairSize=pair_size,
                kuzuLogicalId=eid,
            )
            continue

        # 曲率方向要结合“无向节点对”的规范顺序来决定；
        # 否则 A->B 与 B->A 的双边在 vis 中容易弯到同一侧，视觉上交叉/重叠。
        pair_forward = s <= d
        curve_sign = lane if pair_forward else -lane
        curve_roundness = min(0.14 + 0.06 * abs(lane), 0.34)
        net.add_edge(
            s,
            d,
            id=eid,
            label=lab_e,
            title=etip or " ",
            color=_edge_line,
            width=_edge_width,
            font=edge_font,
            arrowStrikethrough=False,
            smooth={
                "enabled": True,
                "type": "curvedCW" if curve_sign > 0 else "curvedCCW",
                "roundness": curve_roundness,
                "forceDirection": "none",
            },
            kuzuSrc=s,
            kuzuDst=d,
            kuzuLane=lane,
            kuzuPairSize=pair_size,
            kuzuLogicalId=eid,
        )

    html_doc = net.generate_html()

    # Inject addEdge manipulation callback dynamically since Pyvis serialization doesn't support raw JS functions
    if enable_manipulation:
        add_edge_js = """
      network.setOptions({
        manipulation: {
          enabled: false,
          addEdge: function(edgeData, callback) {
            if (edgeData.from === edgeData.to) return;
            edgeData.arrows = 'to';
            edgeData.color = {color: 'rgba(155,151,196,0.5)', highlight: '#e8b4b8'};
            callback(edgeData);
            try {
              for(var i=0; i<window.parent.frames.length; i++) {
                window.parent.frames[i].postMessage({kuzu_click: "add_edge|" + edgeData.from + "|" + edgeData.to}, "*");
              }
            } catch(e) {}
          }
        }
      });

      var _kzEdgeMode = false;
      var _kzBtn = document.createElement('div');
      _kzBtn.id = 'kz-add-edge-btn';
      _kzBtn.textContent = '🔌 拖拽连线';
      _kzBtn.onclick = function() {
        if (_kzEdgeMode) {
          network.disableEditMode();
          _kzBtn.textContent = '🔌 拖拽连线';
          _kzBtn.classList.remove('active');
          _kzEdgeMode = false;
        } else {
          network.addEdgeMode();
          _kzBtn.textContent = '✕ 退出连线';
          _kzBtn.classList.add('active');
          _kzEdgeMode = true;
        }
      };
      var _kzContainer = document.getElementById('mynetwork');
      if (_kzContainer) _kzContainer.appendChild(_kzBtn);
    """
    if enable_manipulation:
        if "return network;" in html_doc:
            html_doc = html_doc.replace("return network;", add_edge_js + "\nreturn network;", 1)
        elif "</body>" in html_doc:
            html_doc = html_doc.replace("</body>", f"<script>{add_edge_js}</script>\n</body>", 1)

    b64 = _panel_b64_payload(node_payloads, edge_payloads)
    return _inject_explorer_panel_and_click_handler(
        html_doc,
        b64,
        panel_hint=panel_hint,
        physics_stays_on=True,
        disable_internal_panel=disable_internal_panel,
        allow_fullscreen_shortcuts=allow_fullscreen_shortcuts,
    )
