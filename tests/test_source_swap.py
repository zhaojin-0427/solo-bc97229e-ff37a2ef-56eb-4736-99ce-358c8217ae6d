"""换库（来源替换）回归测试。

覆盖本系统最关键的失效模式：

* 数据工程师停掉同步、用另一份 SQLite 库替换源路径，两个库的变更日志
  序号可能完全相同（都从 1 开始）；
* 旧实现只认 last_applied_seq，重启后把新库误判为“已同步”，旧副本继续
  可查、新库标本永远漏掉。

本模块以持久来源标识（source_uid）为核心，验证：
  1. 同序号换库能被识别，旧副本在首个查询前就失效（REBUILDING + 503）；
  2. 重建在“新数据 + 来源标识 + 进度”同一事务持久化后才重新开放；
  3. 重建中断/失败后重启绝不放行旧数据，重试不重复计数；
  4. 同一来源的普通重启按持久进度增量续跑，不重建；
  5. 重建期间源库仍可并发提交增删改，最终一致；
  6. Web 端到端：换库后首次查询拿到 503，重建完成后查到的是新库数据。
"""
import os
import threading
import time

import pytest

# 必须在导入 app.web 前指向可写临时目录，避免模块级 app 使用默认 /data 路径
os.environ.setdefault("SOURCE_DB", "/tmp/museum-test-swapdb/source.db")
os.environ.setdefault("REPLICA_DB", "/tmp/museum-test-swapdb/replica.db")

from app import replica_db, source_db  # noqa: E402
from app.source_db import connect as source_connect  # noqa: E402
from app.sync import SyncEngine, reconcile_source, sync_once  # noqa: E402


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #
@pytest.fixture()
def paths(tmp_path):
    src = str(tmp_path / "source.db")
    rep = str(tmp_path / "replica.db")
    source_db.init_demo(
        src, rows=[(i, f"taxon-{i}", f"loc-{i}") for i in range(1, 9)]
    )
    return src, rep


def replica_rows(path):
    with replica_db.open_replica(path) as conn:
        return {
            r["id"]: (r["taxon"], r["locality"])
            for r in conn.execute("SELECT * FROM specimens ORDER BY id")
        }


def source_rows(path):
    conn = source_connect(path, readonly=True)
    try:
        return {
            r["id"]: (r["taxon"], r["locality"])
            for r in conn.execute("SELECT * FROM specimens ORDER BY id")
        }
    finally:
        conn.close()


def run_until_running(src, rep, poll=0.02, timeout=15):
    engine = SyncEngine(src, rep, poll_interval=poll)
    engine.start()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if replica_db.get_meta(rep)["phase"] == replica_db.PHASE_RUNNING:
            return engine
        time.sleep(0.02)
    engine.stop()
    engine.wait_until_idle(timeout)
    raise AssertionError(f"副本未进入 RUNNING: {replica_db.get_meta(rep)}")


def replace_source(src, rows):
    """模拟数据工程师用另一份全新 SQLite 库替换源路径。"""
    os.remove(src)
    for suffix in ("-wal", "-shm"):
        p = src + suffix
        if os.path.exists(p):
            os.remove(p)
    # 新库独立初始化，拥有独立的 _source_identity，日志序号重新从 1 开始
    source_db.init_demo(src, rows=rows)
    return source_db.source_uid(src)


def wait_caught_up(rep, src, timeout=15):
    deadline = time.time() + timeout
    target = source_db.max_change_seq(src)
    while time.time() < deadline:
        meta = replica_db.get_meta(rep)
        if meta["phase"] == replica_db.PHASE_RUNNING and meta["last_applied_seq"] >= target:
            return
        time.sleep(0.02)
    raise AssertionError("副本未能追上源端水位")


# --------------------------------------------------------------------------- #
# 1. 来源标识本身
# --------------------------------------------------------------------------- #
class TestSourceIdentity:
    def test_each_database_has_distinct_persistent_uid(self, tmp_path):
        a = str(tmp_path / "a.db")
        b = str(tmp_path / "b.db")
        source_db.init_demo(a, rows=[(1, "x", "y")])
        source_db.init_demo(b, rows=[(1, "x", "y")])
        uid_a = source_db.source_uid(a)
        uid_b = source_db.source_uid(b)
        assert uid_a and uid_b and uid_a != uid_b
        # 标识持久化：重复读取一致，重启进程后仍在
        assert source_db.source_uid(a) == uid_a

    def test_uid_stored_in_own_table_not_derived_from_seq(self, paths):
        src, _ = paths
        conn = source_connect(src, readonly=True)
        try:
            row = conn.execute(
                "SELECT source_uid FROM _source_identity WHERE id = 1"
            ).fetchone()
        finally:
            conn.close()
        assert row["source_uid"] == source_db.source_uid(src)

    def test_replica_records_source_uid_after_first_copy(self, paths):
        src, rep = paths
        sync_once(src, rep)
        meta = replica_db.get_meta(rep)
        assert meta["source_uid"] == source_db.source_uid(src)


# --------------------------------------------------------------------------- #
# 2. 同序号换库：识别、失效、重建、重新开放
# --------------------------------------------------------------------------- #
class TestSourceSwap:
    def test_reconcile_detects_swap_even_with_overlapping_seq(self, paths):
        """新库最大序号小于旧副本进度时，旧实现必然漏数；UID 对账必须识别。"""
        src, rep = paths
        engine = run_until_running(src, rep)
        engine.stop()
        engine.wait_until_idle(10)
        old_uid = replica_db.get_meta(rep)["source_uid"]
        old_progress = replica_db.get_meta(rep)["last_applied_seq"]

        # 新库只有 3 条 -> 最大序号 3，而旧副本进度是 8
        new_uid = replace_source(
            src, [(i, f"NEW-{i}", "new-loc") for i in range(1, 4)]
        )
        assert new_uid != old_uid
        assert source_db.max_change_seq(src) < old_progress

        action = reconcile_source(src, rep)
        assert action == "rebuild"

        # 对账返回前旧副本已不可查，旧数据被清空
        meta = replica_db.get_meta(rep)
        assert meta["phase"] == replica_db.PHASE_REBUILDING
        assert replica_db.is_ready(rep) is False
        assert replica_rows(rep) == {}
        assert meta["last_applied_seq"] == 0

    def test_rebuild_reaches_new_data_and_binds_new_identity(self, paths):
        src, rep = paths
        engine = run_until_running(src, rep)
        engine.stop()
        engine.wait_until_idle(10)
        old_uid = replica_db.get_meta(rep)["source_uid"]

        new_uid = replace_source(
            src, [(i, f"NEW-{i}", "new-loc") for i in range(1, 4)]
        )
        reconcile_source(src, rep)

        engine2 = run_until_running(src, rep)
        wait_caught_up(rep, src)
        engine2.stop()
        engine2.wait_until_idle(10)

        meta = replica_db.get_meta(rep)
        # 来源标识、数据、进度一致且都指向新库
        assert meta["source_uid"] == new_uid
        assert meta["source_uid"] != old_uid
        assert meta["last_applied_seq"] == source_db.max_change_seq(src)
        assert meta["rebuild_count"] == 1
        # 新库数据齐全；旧库独有行已消失，不残留
        assert replica_rows(rep) == source_rows(src)
        assert replica_db.get_specimen(rep, 1)["taxon"] == "NEW-1"
        assert replica_db.get_specimen(rep, 8) is None

    def test_reconcile_returns_resume_for_same_source_restart(self, paths):
        src, rep = paths
        sync_once(src, rep)
        # 同一物理库、同一 UID 的普通重启：必须续跑而不是重建
        assert reconcile_source(src, rep) == "resume"
        assert replica_db.get_meta(rep)["rebuild_count"] == 0

    def test_normal_restart_resumes_incrementally_without_rebuild(self, paths):
        """已有测试覆盖续跑，这里断言换库计数器与来源标识保持不变。"""
        src, rep = paths
        engine = run_until_running(src, rep)
        wait_caught_up(rep, src)
        engine.stop()
        engine.wait_until_idle(10)
        meta_before = replica_db.get_meta(rep)

        source_db.insert_specimen(src, 60, "after-restart", "x")
        engine2 = run_until_running(src, rep)
        wait_caught_up(rep, src)
        engine2.stop()
        engine2.wait_until_idle(10)

        meta_after = replica_db.get_meta(rep)
        assert meta_after["source_uid"] == meta_before["source_uid"]
        assert meta_after["rebuild_count"] == 0
        # 没有重做快照
        assert meta_after["bootstrap_copied"] == meta_before["bootstrap_copied"] == 8
        assert meta_after["last_applied_seq"] > meta_before["last_applied_seq"]
        assert replica_rows(rep) == source_rows(src)

    def test_second_swap_rebuilds_again_and_increments_counter(self, paths):
        src, rep = paths
        sync_once(src, rep)
        uid_b = replace_source(src, [(1, "B", "b")])
        assert reconcile_source(src, rep) == "rebuild"
        sync_once(src, rep)
        assert replica_db.get_meta(rep)["source_uid"] == uid_b
        assert replica_db.get_meta(rep)["rebuild_count"] == 1

        uid_c = replace_source(src, [(1, "C", "c"), (2, "C2", "c2")])
        assert reconcile_source(src, rep) == "rebuild"
        sync_once(src, rep)
        meta = replica_db.get_meta(rep)
        assert meta["source_uid"] == uid_c
        assert meta["rebuild_count"] == 2
        assert replica_rows(rep) == source_rows(src)

    def test_swap_with_identical_seq_numbers_still_copies_everything(self, paths):
        """两个库日志序号完全相同（都是 1..8）：不能因序号重叠而漏掉新库。"""
        src, rep = paths
        sync_once(src, rep)
        # 新库同样 8 条、同样 8 个序号，只是分类不同
        replace_source(
            src, [(i, f"REPLACED-{i}", f"loc-{i}") for i in range(1, 9)]
        )
        reconcile_source(src, rep)
        sync_once(src, rep)
        assert replica_rows(rep) == source_rows(src)
        assert replica_db.get_specimen(rep, 1)["taxon"] == "REPLACED-1"

    def test_swap_detected_while_engine_running(self, paths):
        """运行过程中源文件被替换：轮询巡检应主动失效并重建，无需人工重启。"""
        src, rep = paths
        engine = run_until_running(src, rep)
        try:
            old_uid = replica_db.get_meta(rep)["source_uid"]
            new_uid = replace_source(
                src, [(i, f"LIVE-{i}", "l") for i in range(1, 6)]
            )
            # 引擎在增量循环中巡检 UID，自动重建
            deadline = time.time() + 30
            while time.time() < deadline:
                meta = replica_db.get_meta(rep)
                if meta["phase"] == replica_db.PHASE_RUNNING and meta["source_uid"] == new_uid:
                    break
                time.sleep(0.02)
            meta = replica_db.get_meta(rep)
            assert meta["source_uid"] == new_uid != old_uid
            assert meta["rebuild_count"] == 1
            wait_caught_up(rep, src)
            assert replica_rows(rep) == source_rows(src)
        finally:
            engine.stop()
            engine.wait_until_idle(10)


# --------------------------------------------------------------------------- #
# 3. 重建中断 / 失败：绝不放行旧数据
# --------------------------------------------------------------------------- #
class TestRebuildInterruption:
    def test_interrupted_rebuild_keeps_replica_unqueryable(self, paths, monkeypatch):
        src, rep = paths
        engine = run_until_running(src, rep)
        engine.stop()
        engine.wait_until_idle(10)
        assert replica_db.is_ready(rep) or (
            replica_db.get_meta(rep)["phase"] == replica_db.PHASE_STOPPED
        )

        replace_source(src, [(i, f"NEW-{i}", "l") for i in range(1, 9)])
        monkeypatch.setattr(source_db, "SNAPSHOT_PAGE_SIZE", 2)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 2, raising=False)
        reconcile_source(src, rep)
        assert replica_db.is_ready(rep) is False

        # 故障注入：第一次读取源水位后崩溃，模拟重建进程在复制期间死亡
        original = SyncEngine._source_max_seq
        state = {"crashed": False}

        def crash_once(self, src_conn):
            if not state["crashed"]:
                state["crashed"] = True
                raise RuntimeError("simulated rebuild crash")
            return original(src_conn)

        monkeypatch.setattr(SyncEngine, "_source_max_seq", crash_once)
        broken = SyncEngine(src, rep, poll_interval=0.02)
        broken.start()
        broken.wait_until_idle(10)
        assert state["crashed"]
        meta = replica_db.get_meta(rep)
        assert meta["phase"] == replica_db.PHASE_ERROR
        assert meta["last_applied_seq"] == 0
        # 关键：崩溃后旧数据不放行，半成品快照也不可见
        assert replica_db.is_ready(rep) is False
        assert replica_db.get_specimen(rep, 1) is None
        monkeypatch.undo()

        # 重启恢复：完整重建成功后才重新开放
        engine2 = run_until_running(src, rep)
        wait_caught_up(rep, src)
        engine2.stop()
        engine2.wait_until_idle(10)
        meta = replica_db.get_meta(rep)
        assert meta["phase"] == replica_db.PHASE_STOPPED
        assert replica_rows(rep) == source_rows(src)
        assert replica_db.get_specimen(rep, 1)["taxon"] == "NEW-1"
        # 同一次换库的崩溃重试不重复计数
        assert meta["rebuild_count"] == 1

    def test_rebuild_restart_after_stop_mid_snapshot(self, paths, monkeypatch):
        """用户在重建快照中途停止，再启动：仍不可查直到重建完成。"""
        src, rep = paths
        sync_once(src, rep)
        replace_source(src, [(i, f"NEW-{i}", "l") for i in range(1, 12)])
        monkeypatch.setattr(source_db, "SNAPSHOT_PAGE_SIZE", 1)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 1, raising=False)
        reconcile_source(src, rep)

        engine = SyncEngine(src, rep, poll_interval=0.01)
        engine.start()
        assert engine.bootstrap_started.wait(5)
        engine.stop()
        engine.wait_until_idle(10)
        assert replica_db.is_ready(rep) is False
        assert replica_db.get_meta(rep)["last_applied_seq"] == 0

        engine2 = run_until_running(src, rep)
        wait_caught_up(rep, src)
        engine2.stop()
        engine2.wait_until_idle(10)
        assert replica_rows(rep) == source_rows(src)
        assert replica_db.get_meta(rep)["rebuild_count"] == 1

    def test_old_data_never_served_during_rebuild_window(self, paths, monkeypatch):
        """重建全流程内任意时刻查询都必须被拒绝，不会闪现旧库标本。"""
        src, rep = paths
        sync_once(src, rep)
        replace_source(src, [(i, f"NEW-{i}", "l") for i in range(1, 7)])
        monkeypatch.setattr(source_db, "SNAPSHOT_PAGE_SIZE", 1)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 1, raising=False)
        reconcile_source(src, rep)

        engine = SyncEngine(src, rep, poll_interval=0.01)
        engine.start()
        observations = []
        deadline = time.time() + 15
        while time.time() < deadline:
            ready = replica_db.is_ready(rep)
            if ready:
                break
            observations.append(ready)
            time.sleep(0.005)
        engine.stop()
        engine.wait_until_idle(10)
        assert observations and not any(observations)
        # 开放后第一时间查到的就是新库数据
        assert replica_db.get_specimen(rep, 1)["taxon"] == "NEW-1"


# --------------------------------------------------------------------------- #
# 4. 重建期间源库仍可并发写入
# --------------------------------------------------------------------------- #
class TestConcurrentWritesDuringRebuild:
    def test_writes_continue_while_rebuild_running(self, paths, monkeypatch):
        src, rep = paths
        sync_once(src, rep)
        replace_source(src, [(i, f"NEW-{i}", "l") for i in range(1, 5)])
        monkeypatch.setattr(source_db, "SNAPSHOT_PAGE_SIZE", 1)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 1, raising=False)
        reconcile_source(src, rep)

        stop_flag = threading.Event()

        def writer():
            n = 5000
            while not stop_flag.is_set():
                n += 1
                try:
                    source_db.insert_specimen(src, n, f"live-{n}", "concurrent")
                    if n % 4 == 0:
                        source_db.update_specimen(src, 1, f"NEW-1-rev-{n}", "l")
                except Exception:
                    pass
                time.sleep(0.002)

        t = threading.Thread(target=writer)
        t.start()
        engine = run_until_running(src, rep)
        try:
            stop_flag.set()
            t.join()
            wait_caught_up(rep, src, timeout=20)
        finally:
            engine.stop()
            engine.wait_until_idle(10)
        # 重建期间的并发提交全部收敛，最终与源库完全一致
        assert replica_rows(rep) == source_rows(src)
        assert replica_db.get_meta(rep)["source_uid"] == source_db.source_uid(src)


# --------------------------------------------------------------------------- #
# 5. Web 端到端：换库后首次查询 503，重建后返回新库数据
# --------------------------------------------------------------------------- #
class TestWebSourceSwap:
    def test_web_swap_flow(self, tmp_path, monkeypatch):
        from app.web import create_app

        src = str(tmp_path / "source.db")
        rep = str(tmp_path / "replica.db")
        source_db.init_demo(
            src, rows=[(i, f"OLD-{i}", "old-loc") for i in range(1, 6)]
        )
        app = create_app(src, rep, poll_interval=0.02, auto_start=False)
        app.config["TESTING"] = True

        with app.test_client() as c:
            engine = app.config["SYNC_ENGINE"]
            r = c.post("/api/sync/start")
            assert r.get_json()["started"] is True
            deadline = time.time() + 10
            while time.time() < deadline:
                if c.get("/api/specimen/1").status_code == 200:
                    break
                time.sleep(0.05)
            assert c.get("/api/specimen/1").get_json()["taxon"] == "OLD-1"
            c.post("/api/sync/stop")
            engine.wait_until_idle(10)

            # 换库：新库序号重新从 1 开始
            new_uid = replace_source(
                src, [(i, f"WEB-NEW-{i}", "new-loc") for i in range(1, 4)]
            )

            # 重新创建应用模拟 Web 进程重启；create_app 在服务任何查询前对账
            app2 = create_app(src, rep, poll_interval=0.02, auto_start=False)
            app2.config["TESTING"] = True
            with app2.test_client() as c2:
                # 首个查询：旧副本已失效，必须 503，不能返回 OLD-1
                r = c2.get("/api/specimen/1")
                assert r.status_code == 503
                assert r.get_json()["error"] == "REPLICA_NOT_READY"

                engine2 = app2.config["SYNC_ENGINE"]
                engine2.start()
                deadline = time.time() + 15
                body = None
                while time.time() < deadline:
                    got = c2.get("/api/specimen/1")
                    if got.status_code == 200:
                        body = got.get_json()
                        break
                    assert got.status_code == 503  # 重建期间持续拒绝
                    time.sleep(0.05)
                assert body is not None
                assert body["taxon"] == "WEB-NEW-1"
                # 旧库独有标本不再可查
                assert c2.get("/api/specimen/5").status_code == 404
                st = c2.get("/api/status").get_json()
                assert st["source_uid"] == new_uid
                assert st["rebuild_count"] == 1
                engine2.stop()
                engine2.wait_until_idle(10)
