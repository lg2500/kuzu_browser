# -*- coding: utf-8 -*-
"""本地上传：将浏览器中的 .kuzu 单文件库保存到临时目录并连接（Kuzu 0.11+ 单文件格式）。"""

from __future__ import annotations

import re
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Tuple


def remove_tree_quiet(path: Optional[str]) -> None:
    """目录须在系统临时目录之下才会删除（避免误删）。"""
    if not path:
        return
    try:
        p = Path(path).resolve()
        tmp = Path(tempfile.gettempdir()).resolve()
        if p == tmp:
            return
        p.relative_to(tmp)  # 非子路径则抛 ValueError
    except ValueError:
        return
    except OSError:
        return
    try:
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
    except OSError:
        pass


def _safe_basename(name: str) -> str:
    base = Path(name or "upload").name
    base = re.sub(r"[^a-zA-Z0-9._\u4e00-\u9fff-]+", "_", base).strip("._") or "upload"
    if not base.lower().endswith(".kuzu"):
        base = f"{base}.kuzu"
    return base[:180]


def validate_kuzu_path(path: Path) -> Tuple[bool, Optional[str]]:
    """试探能否作为 Kuzu 库打开（只读）。"""
    try:
        import kuzu

        db = kuzu.Database(str(path), read_only=True)
        conn = kuzu.Connection(db)
        del conn, db
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    return True, None


def save_kuzu_upload_to_temp(file_bytes: bytes, original_filename: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    将上传的 .kuzu 写入临时目录（同目录下可生成 .wal 等侧车文件）。

    返回 (数据库文件绝对路径, 临时目录路径用于清理, 错误信息)。
    """
    if not file_bytes:
        return None, None, "上传文件为空。"

    parent = Path(tempfile.mkdtemp(prefix="kuzu_browser_upload_"))
    target = parent / _safe_basename(original_filename)
    try:
        target.write_bytes(file_bytes)
    except OSError as exc:
        shutil.rmtree(parent, ignore_errors=True)
        return None, None, f"写入临时文件失败：{exc}"

    ok, err = validate_kuzu_path(target)
    if not ok:
        shutil.rmtree(parent, ignore_errors=True)
        return None, None, (
            "无法作为 Kuzu 数据库打开（需 Kuzu 0.11+ 单文件 .kuzu，"
            f"或与当前驱动版本不兼容）。{err}"
        )

    return str(target.resolve()), str(parent.resolve()), None


def prepare_kuzu_download(
    db_path: str,
    conn: Optional[object] = None,
    read_only: bool = False,
) -> Tuple[Optional[bytes], Optional[str]]:
    """
    读取 .kuzu 文件字节用于下载。
    若当前为读写模式且有连接，先执行 CHECKPOINT 确保 WAL 数据写入主文件。
    返回 (file_bytes, error_msg)。
    """
    p = Path(db_path)
    if not p.exists():
        return None, "数据库文件不存在，无法下载。"

    # 写模式下 CHECKPOINT，将 WAL 刷入主文件
    if conn is not None and not read_only:
        try:
            conn.execute("CHECKPOINT;")
        except Exception:  # noqa: BLE001
            pass  # 部分版本不支持或只读时忽略

    try:
        data = p.read_bytes()
    except OSError as exc:
        return None, f"读取数据库文件失败：{exc}"

    if not data:
        return None, "数据库文件为空。"

    return data, None
