# -*- coding: utf-8 -*-
"""Streamlit 全局视觉主题（自定义 CSS）。"""

from __future__ import annotations

import streamlit as st


def inject_theme() -> None:
    """注入字体与全局样式；在 set_page_config 之后尽早调用一次即可。"""
    st.markdown(
        """
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+SC:wght@400;500;600;700&family=Outfit:wght@500;600;700&display=swap" rel="stylesheet" />

<style>
  :root {
    --kz-font: "Outfit", "Noto Sans SC", system-ui, sans-serif;
    --kz-mono: ui-monospace, "Cascadia Code", "Segoe UI Mono", monospace;
    --kz-bg-0: #1a1a2e;
    --kz-bg-1: #2d2b55;
    --kz-panel: #2d2b55;
    --kz-panel-border: #3d3b6e;
    --kz-text: #efd9ce;
    --kz-muted: #9b97c4;
    --kz-accent: #e8b4b8;
    --kz-accent-hot: #c084fc;
    --kz-accent-soft: #67e8f9;
    --kz-glow: none;
  }

  html, body, [class*="css"]  {
    font-family: var(--kz-font) !important;
  }

  [data-testid="stAppViewContainer"] {
    background: linear-gradient(165deg, var(--kz-bg-0) 0%, var(--kz-bg-1) 100%) !important;
  }

  .main .block-container,
  .stMainBlockContainer.block-container,
  [data-testid="stMainBlockContainer"] {
    padding-top: 1rem !important;
    padding-bottom: 3rem;
    max-width: 1200px;
  }

  [data-testid="stHeader"] {
    background: transparent !important;
    height: 0 !important;
    min-height: 0 !important;
    padding: 0 !important;
  }

  section[data-testid="stSidebar"] {
    background: #16162b !important;
    border-right: 1px solid #3d3b6e;
    box-shadow: 4px 0 16px rgba(0, 0, 0, 0.3);
  }

  section[data-testid="stSidebar"] .block-container {
    padding-top: 1.5rem;
  }

  section[data-testid="stSidebar"] h1,
  section[data-testid="stSidebar"] h2,
  section[data-testid="stSidebar"] h3,
  section[data-testid="stSidebar"] p,
  section[data-testid="stSidebar"] span,
  section[data-testid="stSidebar"] label {
    color: #efd9ce !important;
  }

  section[data-testid="stSidebar"] [data-baseweb="typo-caption"],
  section[data-testid="stSidebar"] .stCaption {
    color: #9b97c4 !important;
  }

  section[data-testid="stSidebar"] .stRadio label {
    color: #efd9ce !important;
    font-weight: 500;
  }

  section[data-testid="stSidebar"] hr {
    border-color: #3d3b6e !important;
  }

  /* 侧栏文件上传：深色玻璃、高对比文字，去掉默认白底块 */
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] {
    color: #d4d0f0 !important;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] > div,
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] > div > div {
    background: transparent !important;
    background-color: transparent !important;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stFileUploaderDropzone"] {
    background: rgba(26, 26, 46, 0.9) !important;
    background-color: rgba(26, 26, 46, 0.9) !important;
    border: 2px dashed #3d3b6e !important;
    border-radius: 14px !important;
    color: #d4d0f0 !important;
    padding: 1rem 0.75rem !important;
    box-shadow: 0 10px 28px rgba(0, 0, 0, 0.35);
    transition: border-color 0.15s ease, box-shadow 0.15s ease;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stFileUploaderDropzone"]:hover {
    border-color: #5a5890 !important;
    box-shadow: 0 10px 28px rgba(0, 0, 0, 0.35);
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stFileUploaderDropzone"] * {
    color: #d4d0f0 !important;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stFileUploaderDropzone"] button {
    color: #1a1a2e !important;
    background: linear-gradient(135deg, #e8b4b8 0%, #efd9ce 100%) !important;
    border: none !important;
    font-weight: 600 !important;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stFileUploaderDropzone"] button:hover {
    filter: brightness(1.08);
    color: #1a1a2e !important;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-baseweb="file-drop-zone"],
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-baseweb="block"] {
    background: transparent !important;
    background-color: transparent !important;
    border-color: transparent !important;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] section[data-testid="stFileUploaderDropzone"] > div {
    background: transparent !important;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stFileUploaderFileData"],
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stFileUploaderFile"] {
    color: #d4d0f0 !important;
    background: rgba(26, 26, 46, 0.65) !important;
    border-radius: 10px !important;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stFileUploaderFileName"],
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] small,
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stText"] {
    color: #9b97c4 !important;
  }
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="baseButton-secondary"],
  section[data-testid="stSidebar"] [data-testid="stFileUploader"] button[kind="secondary"] {
    background: #3d3b6e !important;
    color: #d4d0f0 !important;
    border: 1px solid #5a5890 !important;
  }

  div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stTextInput"]),
  div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stCheckbox"]) {
    margin-bottom: 0.35rem;
  }

  .stTextInput input,
  .stTextArea textarea,
  .stTextArea [data-baseweb="textarea"] {
    background: #1a1a2e !important;
    color: #d4d0f0 !important;
    border: 1px solid #3d3b6e !important;
    border-radius: 10px !important;
    box-shadow: none !important;
    outline: none !important;
  }

  .stTextInput input:focus,
  .stTextArea textarea:focus,
  .stTextArea [data-baseweb="textarea"]:focus {
    border-color: #e8b4b8 !important;
    box-shadow: 0 0 0 2px rgba(232, 180, 184, 0.25) !important;
    outline: none !important;
  }

  .stTextArea textarea::placeholder,
  .stTextInput input::placeholder {
    color: #5a5890 !important;
    opacity: 1 !important;
  }

  .main .stTextInput label,
  .main .stTextArea label,
  label[data-testid="stWidgetLabel"] {
    color: #d4d0f0 !important;
    font-size: 0.92rem !important;
    font-weight: 500 !important;
  }

  section[data-testid="stSidebar"] label[data-testid="stWidgetLabel"] {
    color: #d4d0f0 !important;
  }

  .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
    font-family: var(--kz-font) !important;
    color: var(--kz-text) !important;
    letter-spacing: -0.02em;
  }

  .main h2 {
    font-size: 1.45rem !important;
    font-weight: 700 !important;
    color: var(--kz-text) !important;
    margin-bottom: 0.25rem !important;
  }

  .main .stCaption,
  .main [data-testid="caption"],
  div[data-testid="stCaptionContainer"] {
    color: #9b97c4 !important;
    font-size: 0.93rem !important;
    line-height: 1.55 !important;
  }

  .stButton > button {
    border-radius: 10px !important;
    font-weight: 600 !important;
    border: none !important;
    padding: 0.55rem 1.15rem !important;
    transition: transform 0.12s ease, box-shadow 0.12s ease, filter 0.12s ease;
  }

  .stButton > button:hover {
    transform: translateY(-1px);
    filter: brightness(1.08);
  }

  button[data-testid="baseButton-primary"] {
    background: linear-gradient(135deg, #e8b4b8 0%, #efd9ce 100%) !important;
    color: #1a1a2e !important;
    box-shadow: 0 8px 28px rgba(232, 180, 184, 0.15);
  }

  button[data-testid="baseButton-secondary"] {
    background: #3d3b6e !important;
    color: #d4d0f0 !important;
    border: 1px solid #5a5890 !important;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
  }
  button[data-testid="baseButton-secondary"]:empty {
    display: none !important;
  }

  [data-baseweb="notification"] {
    border-radius: 12px !important;
    border: 1px solid #3d3b6e !important;
    background: #1a1a2e !important;
    color: #efd9ce !important;
  }

  [data-testid="stAlert"] {
    border-radius: 12px !important;
    background: rgba(26, 26, 46, 0.72) !important;
    border: 1px solid #3d3b6e !important;
  }

  [data-testid="stAlert"] p,
  [data-testid="stAlert"] li,
  [data-testid="stAlert"] [data-testid="stMarkdownContainer"] {
    color: #d4d0f0 !important;
  }

  div[data-testid="stDecoration"] {
    background-image: linear-gradient(90deg, #e8b4b8, #c084fc, #67e8f9);
  }

  iframe[title="streamlit_graphviz"] {
    border-radius: 12px;
  }

  /* components.html：消除图预览 iframe 白边与白底露底 */
  div[data-testid="stIFrame"],
  [data-testid="stIFrame"] {
    background: #1a1a2e !important;
    border-radius: 12px !important;
    border: 1px solid #3d3b6e !important;
    overflow: hidden !important;
    line-height: 0 !important;
  }

  div[data-testid="stIFrame"] iframe,
  [data-testid="stIFrame"] iframe {
    background: #1a1a2e !important;
    border: none !important;
    display: block !important;
  }

  .kz-hero-band {
    text-align: center;
    padding: 28px 20px 8px;
    margin-bottom: 8px;
  }
  .kz-hero-band .kz-title {
    font-family: var(--kz-font);
    font-size: clamp(1.75rem, 4vw, 2.35rem);
    font-weight: 700;
    letter-spacing: -0.03em;
    margin: 0 0 10px;
    color: var(--kz-text);
  }
  .kz-hero-band .kz-sub {
    color: #9b97c4;
    font-size: 1.02rem;
    max-width: 560px;
    margin: 0 auto;
    line-height: 1.55;
  }

  .kz-card {
    background: #2d2b55;
    border: 1px solid #3d3b6e;
    border-radius: 12px;
    padding: 20px 22px 18px;
    margin-bottom: 1.25rem;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
  }
  .kz-card-title {
    font-family: var(--kz-font);
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #e8b4b8;
    margin: 0 0 14px;
  }

  /* 查询结果 HTML 表格（替代白底 st.dataframe，与深色主题一致） */
  .kz-df-outer {
    border-radius: 14px;
    border: 1px solid #3d3b6e;
    background: #2d2b55;
    box-shadow: 0 14px 44px rgba(0, 0, 0, 0.35);
    overflow: hidden;
    margin-top: 4px;
  }
  .kz-df-scroll {
    overflow: auto;
    max-height: min(58vh, 520px);
    overscroll-behavior: contain;
  }
  table.dataframe.kz-df {
    width: 100%;
    min-width: 520px;
    border-collapse: separate;
    border-spacing: 0;
    font-family: ui-monospace, "Cascadia Code", "SF Mono", "Noto Sans SC", monospace;
    font-size: 0.8rem;
  }
  table.dataframe.kz-df thead th {
    position: sticky;
    top: 0;
    z-index: 2;
    padding: 12px 14px;
    text-align: left;
    font-weight: 600;
    color: #e8b4b8 !important;
    background: #1a1a2e !important;
    border-bottom: 2px solid #3d3b6e;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
    white-space: nowrap;
  }
  table.dataframe.kz-df tbody th {
    padding: 9px 12px;
    text-align: right;
    color: #e8b4b8 !important;
    background: #2d2b55 !important;
    border-bottom: 1px solid #3d3b6e;
    font-weight: 500;
    white-space: nowrap;
  }
  table.dataframe.kz-df td {
    padding: 10px 14px;
    color: #d4d0f0 !important;
    background: #2d2b55 !important;
    border-bottom: 1px solid #3d3b6e;
    vertical-align: top;
    line-height: 1.5;
    word-break: break-word;
    max-width: min(520px, 50vw);
  }
  table.dataframe.kz-df tbody tr:nth-child(even) td {
    background: #1a1a2e !important;
  }
  table.dataframe.kz-df tbody tr:hover td {
    background: rgba(232, 180, 184, 0.08) !important;
  }

  [data-testid="stDataFrame"] {
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid #3d3b6e;
  }
</style>
        """,
        unsafe_allow_html=True,
    )
