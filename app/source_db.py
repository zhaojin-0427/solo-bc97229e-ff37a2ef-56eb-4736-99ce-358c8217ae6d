"""源标本登记库（本地 SQLite, WAL）。

本模块负责：
  * 打开/初始化源库，开启 WAL；
  * 在 specimens 表上挂载 CDC 辅助表 _change_log 与三个触发器；
  * 提供示例数据与演示用的增/改/撤操作。

设计要点
========
触发器在源库 *内部* 运行，和触发它的 DML 属于同一个事务：
  * 事务 COMMIT -> 数据行与变更日志原子地一起可见；
  * 事务 ROLLBACK -> 数据行与变更日志一起消失，绝不传播；
  * 同步进程只以短事务只读打开源库，源端写入无需等待任何复制/同步完成。

_change_log.seq 使用 AUTOINCREMENT，保证序号单调递增、永不复用
（包括事务回滚也不会导致同一 seq 被再次分配），副本据此持久化进度。
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

# 同步器在快照阶段逐页读取的行数
SNAPSHOT_PAGE_SIZE = int(os.environ.get("SNAPSHOT_PAGE_SIZE", "500"))
# 增量阶段每批应用的日志条数
CHANGE_BATCH_SIZE = int(os.environ.get("CHANGE_BATCH_SIZE", "500"))
# 无新变更时空闲轮询间隔（秒）
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1.0"))


def connect(path: str, *, readonly: bool = False, timeout: float = 30.0) -> sqlite3.Connection:
    """打开一个源库连接。

    readonly=True 时以只读 URI 方式打开，同步器因此不可能误写源库；
    WAL 模式下只读连接与写连接可以同时存在。
    """
    if readonly:
        uri = f"file:{path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=timeout, isolation_level=None)
    else:
        conn = sqlite3.connect(path, timeout=timeout, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    return conn


@contextmanager
def writer(path: str):
    conn = connect(path)
    try:
        yield conn
    finally:
        conn.close()


def init_source(path: str) -> None:
    """初始化源库：WAL、specimens 表、CDC 辅助表与触发器（幂等）。"""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS specimens (
                id       INTEGER PRIMARY KEY,
                taxon    TEXT NOT NULL,
                locality TEXT NOT NULL
            )
            """
        )
        # CDC 日志：op I=插入 U=更正 D=撤回（删除）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _change_log (
                seq      INTEGER PRIMARY KEY AUTOINCREMENT,
                op       TEXT NOT NULL CHECK (op IN ('I', 'U', 'D')),
                id       INTEGER NOT NULL,
                taxon    TEXT,
                locality TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trg_specimens_ai AFTER INSERT ON specimens
            BEGIN
                INSERT INTO _change_log(op, id, taxon, locality)
                VALUES ('I', NEW.id, NEW.taxon, NEW.locality);
            END
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trg_specimens_au AFTER UPDATE ON specimens
            BEGIN
                INSERT INTO _change_log(op, id, taxon, locality)
                VALUES ('U', NEW.id, NEW.taxon, NEW.locality);
            END
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trg_specimens_ad AFTER DELETE ON specimens
            BEGIN
                INSERT INTO _change_log(op, id, taxon, locality)
                VALUES ('D', OLD.id, NULL, NULL);
            END
            """
        )
    finally:
        conn.close()


def backfill_change_log(path: str) -> int:
    """把已存在的标本登记为插入事件（幂等）。

    仅处理还没有任何 I/U 日志的 id。用于给“先有数据、后挂触发器”的库
    补齐历史。正常由 init_demo 建库流程调用，此时日志为空。
    返回写入的日志条数。
    """
    conn = connect(path)
    try:
        cur = conn.execute(
            """
            INSERT INTO _change_log(op, id, taxon, locality)
            SELECT 'I', s.id, s.taxon, s.locality
            FROM specimens s
            WHERE NOT EXISTS (
                SELECT 1 FROM _change_log c WHERE c.id = s.id AND c.op IN ('I', 'U')
            )
            """
        )
        return cur.rowcount
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 演示数据 / 演示写入
# --------------------------------------------------------------------------- #

DEMO_SPECIMENS = [
    (1, "Brachiosaurus altithorax", "Colorado, Morrison Formation"),
    (2, "Archaeopteryx lithographica", "Solnhofen Limestone, Bavaria"),
    (3, "Tyrannosaurus rex", "Hell Creek Formation, Montana"),
    (4, "Trilobita indet.", "Chengjiang Biota, Yunnan"),
    (5, "Ammonitida indet.", "Lyme Regis, Dorset"),
    (6, "Ginkgo biloba", "Jehol Biota, Liaoning"),
    (7, "Smilodon fatalis", "La Brea Tar Pits, California"),
    (8, "Ichthyosaurus communis", "Holzmaden, Baden-Wurttemberg"),
]


def init_demo(path: str, rows: list[tuple[int, str, str]] | None = None) -> None:
    """创建可演示的示例源库（已存在则保留不动）。"""
    if os.path.exists(path):
        init_source(path)
        return
    init_source(path)
    rows = DEMO_SPECIMENS if rows is None else rows
    conn = connect(path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            "INSERT INTO specimens(id, taxon, locality) VALUES (?, ?, ?)", rows
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    # 为历史行补 I 事件，确保一个全新副本总能通过日志到达一致状态
    backfill_change_log(path)


def insert_specimen(path: str, specimen_id: int, taxon: str, locality: str) -> None:
    with writer(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO specimens(id, taxon, locality) VALUES (?, ?, ?)",
                (specimen_id, taxon, locality),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def update_specimen(path: str, specimen_id: int, taxon: str, locality: str) -> None:
    with writer(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "UPDATE specimens SET taxon = ?, locality = ? WHERE id = ?",
            (taxon, locality, specimen_id),
        )
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            raise KeyError(f"specimen {specimen_id} not found in source")
        conn.execute("COMMIT")


def delete_specimen(path: str, specimen_id: int) -> None:
    """鉴定员撤回一条源记录。"""
    with writer(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute("DELETE FROM specimens WHERE id = ?", (specimen_id,))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            raise KeyError(f"specimen {specimen_id} not found in source")
        conn.execute("COMMIT")


def max_change_seq(path: str) -> int:
    conn = connect(path, readonly=True)
    try:
        row = conn.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM _change_log").fetchone()
        return int(row["m"])
    finally:
        conn.close()


def count_specimens(path: str) -> int:
    conn = connect(path, readonly=True)
    try:
        return int(conn.execute("SELECT COUNT(*) AS c FROM specimens").fetchone()["c"])
    finally:
        conn.close()
