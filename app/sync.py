"""同步引擎：源库 -> 副本库的持续增量复制。

正确性策略
==========

1. 变更捕获（源端）
   specimens 上的 AFTER INSERT/UPDATE/DELETE 触发器把每次行变更追加到
   _change_log（单调 AUTOINCREMENT 序号 seq）。触发器与 DML 同事务：
     - 提交 -> 行与日志原子可见；回滚 -> 一起消失，不传播。

2. 首次后台复制（bootstrap，允许源库并发提交）
   a. 记下源库当前最大序号 w0；
   b. 按主键分页把 specimens 快照写入副本（每页与进度同事务提交，
      不持有长读事务、不反复全表扫描）；
   c. 快照结束记下最大序号 w1；
   d. 重放日志区间 [1, w1]（INSERT OR REPLACE / DELETE）。
   区间内的日志覆盖快照期间发生的一切并发变更：
     - 快照后插入的行：其 I 事件在 (w0, w1]，重放插入；
     - 快照后删除的行：D 事件在 (w0, w1]，重放删除，不会“复活”；
     - 快照后更正的行：U 事件携带最新值，重放覆盖旧鉴定；
     - 区间起点从 1 开始，对快照已有行是幂等覆盖，无副作用。
   重放完成、last_applied_seq=w1 与数据在同一事务提交后，副本才置为
   RUNNING（就绪前不供查询）。

3. 持续增量
   只拉取 seq > last_applied_seq 的新日志（索引点查，不扫描全表），
   每批与进度在同一事务提交；没有新日志则短轮询等待。

4. 原子性与续跑
   每批“数据变更 + sync_meta + _applied_changes”同一事务提交。
   中断重启以持久化的 last_applied_seq 为准：
     - 仍为 0（初次复制未完成）：重做 bootstrap（reset 后重来，幂等）；
     - 大于 0（曾经就绪）：无论上次是 RUNNING/STOPPED/ERROR，都按持久
       进度接续增量，绝不重做快照、绝不重复传播。

5. 来源识别与换库重建（关键修复）
   序号只在 *同一个* 源库内单调；数据工程师可能停掉同步、用另一份 SQLite
   库替换原路径，而新库的变更日志序号又从 1 开始（与旧库序号重叠）。
   仅靠 last_applied_seq 会把新库误判为“已同步”，导致旧副本继续可查、
   新库标本永远漏掉。因此：
     - 每个源库初始化时在 _source_identity 表写入持久且唯一的 source_uid；
     - 副本 sync_meta 持久化当前服务来源的 source_uid；
     - 启动前 reconcile_source 对账：UID 相同 -> 按持久进度增量续跑；
       UID 不同（换库）-> 先把旧副本置为 REBUILDING 并清空（在 Web 首个
       查询之前就不可查），再整体重建；
     - 重建的最后一个事务把“新数据 + source_uid + last_applied_seq”
       一起提交，提交前还复核源 UID 未再次变化，之后才置 RUNNING 开放查询；
     - 重建中断/失败后进度保持 0、阶段非 RUNNING，重启继续重建且绝不
       放行旧数据；增量循环每轮也巡检 UID，运行中换库同样自动重建。

6. 不阻塞源写入
   对源库全部使用只读短事务（自动提交下的 SELECT），WAL 允许读写并发；
   同步快慢不影响鉴定员提交，重建期间源库仍可并发增删改。
"""
from __future__ import annotations

import os
import threading
import time
from typing import Optional

from . import replica_db, source_db
from .source_db import (
    CHANGE_BATCH_SIZE,
    POLL_INTERVAL,
    SNAPSHOT_PAGE_SIZE,
    connect as source_connect,
)


class SyncEngine:
    """后台同步线程的生命周期与状态管理。"""

    def __init__(self, source_path: str, replica_path: str,
                 poll_interval: float = POLL_INTERVAL):
        self.source_path = source_path
        self.replica_path = replica_path
        self.poll_interval = poll_interval
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        # bootstrap 开始/完成事件，便于外部（如测试）确定性地观察阶段
        self.bootstrap_started = threading.Event()
        self.bootstrap_completed = threading.Event()
        # 仅供 UI 展示的瞬时计数器（权威进度在副本 sync_meta 中）
        self._lock = threading.Lock()
        self._stats = {
            "running": False,
            "last_batch_at": None,
            "last_batch_changes": 0,
            "total_batches": 0,
        }

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def start(self) -> bool:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop.clear()
            self.bootstrap_started.clear()
            self.bootstrap_completed.clear()
            self._stats.update(running=True, last_error=None)
            t = threading.Thread(
                target=self._run_safely, name="sync-worker", daemon=True
            )
            self._thread = t
            t.start()
            return True

    def stop(self) -> bool:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                return False
            self._stop.set()
            return True

    def wait_until_idle(self, timeout: Optional[float] = None) -> None:
        """供测试使用：等待同步线程退出。"""
        t = self._thread
        if t is not None:
            t.join(timeout)

    def status(self) -> dict:
        meta = replica_db.get_meta(self.replica_path)
        with self._lock:
            stats = dict(self._stats)
            alive = self._thread is not None and self._thread.is_alive()
        return {
            **meta,
            "worker_alive": alive,
            "stop_requested": self._stop.is_set(),
            **stats,
        }

    # ------------------------------------------------------------------ #
    # 线程主体
    # ------------------------------------------------------------------ #
    def _run_safely(self) -> None:
        try:
            self._run()
        except Exception as exc:  # 记录到持久 meta，UI 可见，线程退出
            self._record_error(exc)
        finally:
            with self._lock:
                self._stats["running"] = False

    def _record_error(self, exc: Exception) -> None:
        try:
            with replica_db.open_replica(self.replica_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    UPDATE sync_meta
                       SET phase = ?, last_error = ?,
                           updated_at = datetime('now')
                     WHERE id = 1
                    """,
                    (replica_db.PHASE_ERROR, f"{type(exc).__name__}: {exc}"),
                )
                conn.execute("COMMIT")
        except Exception:
            # 副本本身不可写时，错误至少保留在线程状态里
            pass
        with self._lock:
            self._stats["last_error"] = f"{type(exc).__name__}: {exc}"

    def _run(self) -> None:
        replica_db.init_replica(self.replica_path)
        # 来源对账：同 UID 普通重启按持久进度增量续跑；换库（UID 不同，
        # 即使两边日志序号相同）先让旧副本失效再重建。
        action = reconcile_source(self.source_path, self.replica_path)
        if action == "resume":
            self._mark_running()
            self._incremental_loop(
                replica_db.get_meta(self.replica_path)["last_applied_seq"]
            )
            return
        if action == "missing":
            # 源库缺失：不清空已就绪副本，交给增量/复制路径打开源库并记录 ERROR
            meta = replica_db.get_meta(self.replica_path)
            if meta["last_applied_seq"] > 0:
                self._mark_running()
                self._incremental_loop(meta["last_applied_seq"])
                return

        rebuilding = action == "rebuild"
        # bootstrap（首次复制）或 rebuild（换库重建），中断后都可安全重做
        self._bootstrap(rebuilding=rebuilding)
        self.bootstrap_completed.set()
        meta = replica_db.get_meta(self.replica_path)
        self._incremental_loop(meta["last_applied_seq"])

    # ------------------------------------------------------------------ #
    # 初次复制 / 换库重建
    # ------------------------------------------------------------------ #
    def _bootstrap(self, *, rebuilding: bool = False) -> None:
        """从源库整体重建副本。

        rebuilding=False：全新副本的首次复制（BOOTSTRAP）。
        rebuilding=True ：检测到换库后的重建（REBUILDING）。重建期间旧数据
                          已在对账阶段清空且查询持续被拒绝，直到最后一个事务
                          把新数据、来源标识与进度一起提交后才开放查询。
        两种模式都可能在中断后被重新调用：上次进度仍为 0 时安全重做。
        """
        self.bootstrap_started.set()
        uid = source_db.source_uid(self.source_path)
        if rebuilding:
            # 幂等：即使上次重建中断（阶段已是 REBUILDING、残留半成品数据），
            # invalidate 也只在 last_applied_seq>0 时计数，重做不会重复计数
            replica_db.invalidate_for_rebuild(self.replica_path)
        else:
            replica_db.reset_for_bootstrap(self.replica_path)

        src = source_connect(self.source_path, readonly=True)
        try:
            w0 = self._source_max_seq(src)

            # --- 分页快照：按主键游标推进，每页独立短事务 ---------------
            last_id = 0
            copied = 0
            while not self._stop.is_set():
                rows = src.execute(
                    """
                    SELECT id, taxon, locality
                      FROM specimens
                     WHERE id > ?
                     ORDER BY id ASC
                     LIMIT ?
                    """,
                    (last_id, SNAPSHOT_PAGE_SIZE),
                ).fetchall()
                if not rows:
                    break
                with replica_db.open_replica(self.replica_path) as rconn:
                    rconn.execute("BEGIN IMMEDIATE")
                    rconn.executemany(
                        "INSERT OR REPLACE INTO specimens(id, taxon, locality)"
                        " VALUES (?, ?, ?)",
                        [(r["id"], r["taxon"], r["locality"]) for r in rows],
                    )
                    copied += len(rows)
                    rconn.execute(
                        """
                        UPDATE sync_meta
                           SET bootstrap_copied = ?,
                               source_max_seq = ?,
                               updated_at = datetime('now')
                         WHERE id = 1
                        """,
                        (copied, w0),
                    )
                    rconn.execute("COMMIT")
                last_id = rows[-1]["id"]
                if len(rows) < SNAPSHOT_PAGE_SIZE:
                    break

            if self._stop.is_set():
                self._mark_stopped()
                return

            # 快照结束时的源端最大序号：重放 [1, w1] 覆盖并发窗口
            w1 = self._source_max_seq(src)
        finally:
            src.close()

        self._catch_up(w1, source_uid=uid, rebuilding=rebuilding)

    # ------------------------------------------------------------------ #
    # 增量循环
    # ------------------------------------------------------------------ #
    def _incremental_loop(self, last_seq: int) -> None:
        while not self._stop.is_set():
            # 每轮巡检来源标识：运行中源文件被替换也能立刻发现，
            # 避免把新库从 1 开始的序号误判为“已应用”而漏掉新数据。
            current_uid = source_db.source_uid(self.source_path)
            replica_uid = replica_db.get_meta(self.replica_path)["source_uid"]
            if current_uid is not None and current_uid != replica_uid:
                # 换库：先让旧副本不可查，再整体重建
                replica_db.invalidate_for_rebuild(self.replica_path)
                self._bootstrap(rebuilding=True)
                self.bootstrap_completed.set()
                last_seq = replica_db.get_meta(self.replica_path)["last_applied_seq"]
                continue

            src = source_connect(self.source_path, readonly=True)
            try:
                source_max = self._source_max_seq(src)
            finally:
                src.close()

            if source_max <= last_seq:
                # 只更新“源端水位”，不改动 last_applied_seq
                self._touch_watermark(source_max)
                self._wait_poll()
                continue

            advanced = self._apply_batch(last_seq, source_max)
            if advanced is None:
                return  # 被停止
            last_seq = advanced

        self._mark_stopped()

    def _wait_poll(self) -> None:
        # 可被 stop 立即唤醒
        self._stop.wait(self.poll_interval)

    # ------------------------------------------------------------------ #
    # 日志重放（bootstrap 收尾与增量共用）
    # ------------------------------------------------------------------ #
    def _catch_up(
        self,
        target_seq: int,
        *,
        source_uid: Optional[str] = None,
        rebuilding: bool = False,
    ) -> None:
        """重放日志直到 last_applied_seq == target_seq，然后置为 RUNNING。

        bootstrap 用它把快照与并发窗口的全部日志补齐；只有数据与进度
        一起提交完成、副本真正一致后才进入 RUNNING（就绪前不供查询）。
        rebuilding=True 时还在同一个完成事务中原子写入来源标识，并在提交前
        复核源库 UID 未再次变化（防止重建期间源文件又被替换导致混搭）。
        """
        last_seq = replica_db.get_meta(self.replica_path)["last_applied_seq"]
        while last_seq < target_seq:
            if self._stop.is_set():
                self._mark_stopped()
                return
            advanced = self._apply_batch(last_seq, target_seq)
            if advanced is None:
                return
            if advanced == last_seq:  # 理论上不会出现，防御性退出
                break
            last_seq = advanced

        if rebuilding:
            # 提交前复核：重建期间源文件若又被替换，拒绝以混搭状态开放查询
            latest_uid = source_db.source_uid(self.source_path)
            if latest_uid is None or latest_uid != source_uid:
                raise RuntimeError(
                    "source identity changed while rebuild was in progress"
                )

        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            if source_uid is not None:
                # 首次复制与换库重建都在完成事务中绑定来源标识：
                # 新数据、来源标识、进度一致持久化后才进入 RUNNING
                rconn.execute(
                    """
                    UPDATE sync_meta
                       SET source_uid = ?, updated_at = datetime('now')
                     WHERE id = 1
                    """,
                    (source_uid,),
                )
            rconn.execute(
                """
                UPDATE sync_meta
                   SET phase = ?, last_applied_seq = ?,
                       source_max_seq = ?, last_error = NULL,
                       updated_at = datetime('now')
                 WHERE id = 1
                """,
                (replica_db.PHASE_RUNNING, target_seq, target_seq),
            )
            rconn.execute("COMMIT")

    def _apply_batch(self, last_seq: int, target_seq: int) -> Optional[int]:
        """读取源端 (last_seq, target_seq] 的一批日志并原子应用。

        返回新的 last_applied_seq；被停止返回 None。
        数据变更、审计流水、进度 meta 在副本的单个事务内共同提交。
        不修改 phase：阶段切换由调用方（_catch_up / _mark_stopped）负责。
        """
        src = source_connect(self.source_path, readonly=True)
        try:
            changes = src.execute(
                """
                SELECT seq, op, id, taxon, locality
                  FROM _change_log
                 WHERE seq > ? AND seq <= ?
                 ORDER BY seq ASC
                 LIMIT ?
                """,
                (last_seq, target_seq, CHANGE_BATCH_SIZE),
            ).fetchall()
        finally:
            src.close()

        if not changes:
            return last_seq

        new_last = int(changes[-1]["seq"])
        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            for ch in changes:
                if self._stop.is_set():
                    rconn.execute("ROLLBACK")
                    self._mark_stopped()
                    return None
                if ch["op"] in ("I", "U"):
                    # INSERT OR REPLACE：以日志携带的最新鉴定为准，
                    # 旧鉴定不会覆盖新值（日志按 seq 升序应用）
                    rconn.execute(
                        "INSERT OR REPLACE INTO specimens(id, taxon, locality)"
                        " VALUES (?, ?, ?)",
                        (ch["id"], ch["taxon"], ch["locality"]),
                    )
                else:  # 'D'：撤回，删除且不会被后续快照复活
                    rconn.execute(
                        "DELETE FROM specimens WHERE id = ?", (ch["id"],)
                    )
                rconn.execute(
                    "INSERT OR REPLACE INTO _applied_changes(seq, op, specimen_id)"
                    " VALUES (?, ?, ?)",
                    (ch["seq"], ch["op"], ch["id"]),
                )

            rconn.execute(
                """
                UPDATE sync_meta
                   SET last_applied_seq = ?,
                       source_max_seq = MAX(source_max_seq, ?),
                       last_error = NULL,
                       updated_at = datetime('now')
                 WHERE id = 1
                """,
                (new_last, target_seq),
            )
            rconn.execute("COMMIT")

        with self._lock:
            self._stats["last_batch_at"] = time.time()
            self._stats["last_batch_changes"] = len(changes)
            self._stats["total_batches"] += 1
        return new_last

    # ------------------------------------------------------------------ #
    # 辅助
    # ------------------------------------------------------------------ #
    @staticmethod
    def _source_max_seq(src) -> int:
        row = src.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM _change_log"
        ).fetchone()
        return int(row["m"])

    def _touch_watermark(self, source_max: int) -> None:
        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            rconn.execute(
                """
                UPDATE sync_meta
                   SET source_max_seq = ?, updated_at = datetime('now')
                 WHERE id = 1
                """,
                (source_max,),
            )
            rconn.execute("COMMIT")

    def _mark_stopped(self) -> None:
        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            rconn.execute(
                """
                UPDATE sync_meta
                   SET phase = ?, updated_at = datetime('now')
                 WHERE id = 1
                """,
                (replica_db.PHASE_STOPPED,),
            )
            rconn.execute("COMMIT")

    def _mark_running(self) -> None:
        """重启续跑时把阶段从 STOPPED/ERROR 切回 RUNNING（进度保持不变）。"""
        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            rconn.execute(
                """
                UPDATE sync_meta
                   SET phase = ?, last_error = NULL,
                       updated_at = datetime('now')
                 WHERE id = 1
                """,
                (replica_db.PHASE_RUNNING,),
            )
            rconn.execute("COMMIT")


# 便捷函数：一次性同步到当前源端水位（主要用于测试/脚本）
def reconcile_source(source_path: str, replica_path: str) -> str:
    """启动前来源对账，决定本次运行是增量续跑还是整体重建。

    返回：
      * ``"resume"``   —— 同一来源（UID 相同）且副本曾完成过复制，
                         按持久化的 last_applied_seq 增量续跑；
      * ``"bootstrap"`` —— 全新副本（尚未完成过首次复制）；
      * ``"rebuild"``  —— 检测到换库（UID 不同或新库尚无标识）。
                         换库时本函数在返回前就把旧副本置为 REBUILDING 并
                         清空旧数据：因此在 Web 首个查询之前旧副本已不可查；
      * ``"missing"``  —— 源文件不存在，不主动建库，交由引擎记录 ERROR。

    两个库的变更日志序号可能完全相同（都从 1 开始），序号本身不能识别
    来源，必须以持久 UID 为准。
    """
    replica_db.init_replica(replica_path)
    if not os.path.exists(source_path):
        # 源库缺失：不主动创建，交给引擎按原语义记录 ERROR
        return "missing"
    # 确保新替换进来的库具备 CDC 对象与来源标识
    uid = source_db.ensure_source_uid(source_path)
    meta = replica_db.get_meta(replica_path)

    if meta["last_applied_seq"] > 0 and meta.get("source_uid") == uid:
        return "resume"
    if meta["last_applied_seq"] > 0 or meta.get("source_uid") is not None:
        # 副本曾服务于另一个来源 -> 换库重建：先失效旧副本
        replica_db.invalidate_for_rebuild(replica_path)
        return "rebuild"
    return "bootstrap"


def sync_once(source_path: str, replica_path: str) -> dict:
    engine = SyncEngine(source_path, replica_path)
    action = reconcile_source(source_path, replica_path)
    if action == "missing":
        raise FileNotFoundError(source_path)
    if action != "resume":
        engine._bootstrap(rebuilding=(action == "rebuild"))  # noqa: SLF001
    meta = replica_db.get_meta(replica_path)
    src = source_connect(source_path, readonly=True)
    try:
        target = engine._source_max_seq(src)  # noqa: SLF001
    finally:
        src.close()
    if target > meta["last_applied_seq"]:
        engine._catch_up(target)  # noqa: SLF001
    return replica_db.get_meta(replica_path)
