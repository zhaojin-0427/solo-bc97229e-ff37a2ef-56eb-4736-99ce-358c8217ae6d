"""副本库（独立 SQLite）。

副本只由同步器写入，结构刻意与源库解耦：
  * specimens        — 标本数据副本；
  * sync_meta        — 单行表，保存阶段、已应用日志序号等持久进度；
  * _applied_changes — 已应用变更的序号流水（附带便于排查的审计信息）。

同步器保证：数据行的修改与 sync_meta 进度更新在 *同一个事务* 中提交，
因此“副本数据与进度共同提交”，崩溃后要么整批生效、要么整批回滚。
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

# 同步阶段
PHASE_INIT = "INIT"          # 尚未开始 / 初次复制未完成
PHASE_BOOTSTRAP = "BOOTSTRAP"  # 正在做首次后台复制
PHASE_RUNNING = "RUNNING"    # 已就绪，持续增量同步
PHASE_STOPPED = "STOPPED"    # 被用户停止
PHASE_ERROR = "ERROR"        # 同步出错


class _ReplicaConnection(sqlite3.Connection):
    """带身份标记的连接，便于与源库连接区分（例如测试中注入故障时）。"""

    _is_replica = True


def connect(path: str, *, timeout: float = 30.0) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(
        path, timeout=timeout, isolation_level=None, factory=_ReplicaConnection
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    # 外键关闭：副本是独立的派生库，不依赖源库对象
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


@contextmanager
def open_replica(path: str):
    conn = connect(path)
    try:
        yield conn
    finally:
        conn.close()


def init_replica(path: str) -> None:
    """创建副本结构（幂等）。"""
    with open_replica(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS specimens (
                id       INTEGER PRIMARY KEY,
                taxon    TEXT NOT NULL,
                locality TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_meta (
                id               INTEGER PRIMARY KEY CHECK (id = 1),
                phase            TEXT NOT NULL,
                last_applied_seq INTEGER NOT NULL DEFAULT 0,
                source_max_seq   INTEGER NOT NULL DEFAULT 0,
                bootstrap_copied INTEGER NOT NULL DEFAULT 0,
                last_error       TEXT,
                updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _applied_changes (
                seq        INTEGER PRIMARY KEY,
                op         TEXT NOT NULL,
                specimen_id INTEGER NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO sync_meta(id, phase) VALUES (1, ?)",
            (PHASE_INIT,),
        )


def get_meta(path: str) -> dict:
    fallback = {
        "phase": PHASE_INIT,
        "last_applied_seq": 0,
        "source_max_seq": 0,
        "bootstrap_copied": 0,
        "last_error": None,
    }
    if not os.path.exists(path):
        return fallback
    with open_replica(path) as conn:
        try:
            row = conn.execute("SELECT * FROM sync_meta WHERE id = 1").fetchone()
        except sqlite3.OperationalError:
            # 副本文件存在但表结构尚未初始化
            return fallback
        if row is None:
            return fallback
        return dict(row)


def is_ready(path: str) -> bool:
    """就绪前不供查询：只有 RUNNING 阶段才接受标本查询。"""
    return os.path.exists(path) and get_meta(path)["phase"] == PHASE_RUNNING


def get_specimen(path: str, specimen_id: int):
    with open_replica(path) as conn:
        row = conn.execute(
            "SELECT id, taxon, locality FROM specimens WHERE id = ?",
            (specimen_id,),
        ).fetchone()
        return dict(row) if row is not None else None


def reset_for_bootstrap(path: str) -> None:
    """初次复制（重新）开始：清空一切派生数据，回到 INIT 进度。

    初次复制中断后重做时调用，保证半成品快照不会残留。
    """
    with open_replica(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM specimens")
        conn.execute("DELETE FROM _applied_changes")
        conn.execute(
            """
            UPDATE sync_meta
               SET phase = ?, last_applied_seq = 0, source_max_seq = 0,
                   bootstrap_copied = 0, last_error = NULL,
                   updated_at = datetime('now')
             WHERE id = 1
            """,
            (PHASE_BOOTSTRAP,),
        )
        conn.execute("COMMIT")
