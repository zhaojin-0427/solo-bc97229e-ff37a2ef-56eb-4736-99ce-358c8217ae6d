"""Web 控制台端到端测试（Flask test client + 真实同步线程）。"""
import os
import time

import pytest

# 必须在导入 app.web 前指向临时目录，避免模块级 app 使用默认 /data 路径
os.environ.setdefault("SOURCE_DB", "/tmp/museum-test-webdb/source.db")
os.environ.setdefault("REPLICA_DB", "/tmp/museum-test-webdb/replica.db")

from app import replica_db, source_db  # noqa: E402
from app.web import create_app  # noqa: E402


@pytest.fixture()
def app_paths(tmp_path):
    src = str(tmp_path / "source.db")
    rep = str(tmp_path / "replica.db")
    source_db.init_demo(src, rows=[(i, f"taxon-{i}", f"loc-{i}") for i in range(1, 6)])
    return src, rep


@pytest.fixture()
def client(app_paths):
    src, rep = app_paths
    app = create_app(src, rep, poll_interval=0.05, auto_start=False)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c, app.config["SYNC_ENGINE"]


def wait_phase(engine, predicate, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate(replica_db.get_meta(engine.replica_path)):
            return True
        time.sleep(0.02)
    return False


class TestWebAPI:
    def test_healthz(self, client):
        c, _ = client
        assert c.get("/healthz").status_code == 200

    def test_query_503_before_ready(self, client):
        c, _ = client
        r = c.get("/api/specimen/1")
        assert r.status_code == 503
        assert r.get_json()["error"] == "REPLICA_NOT_READY"

    def test_full_flow_start_query_and_demo_writes(self, client):
        c, engine = client

        # 启动同步并等待就绪
        r = c.post("/api/sync/start")
        assert r.status_code == 200 and r.get_json()["started"] is True
        assert wait_phase(engine, lambda m: m["phase"] == replica_db.PHASE_RUNNING)

        # 就绪后查询副本
        r = c.get("/api/specimen/3")
        assert r.status_code == 200
        body = r.get_json()
        assert body == {"id": 3, "taxon": "taxon-3", "locality": "loc-3"}

        # 不存在的标本 -> 404
        assert c.get("/api/specimen/9999").status_code == 404

        # 通过网页在源库新增 -> 副本自动追上
        r = c.post("/api/demo/insert", json={
            "id": 100, "taxon": "Web taxon", "locality": "Web locality"})
        assert r.status_code == 200
        deadline = time.time() + 10
        while time.time() < deadline:
            got = c.get("/api/specimen/100")
            if got.status_code == 200:
                assert got.get_json()["taxon"] == "Web taxon"
                break
            time.sleep(0.05)
        else:
            pytest.fail("新增标本未同步到副本")

        # 通过网页更正 -> 副本以新鉴定覆盖
        r = c.post("/api/demo/update", json={
            "id": 100, "taxon": "Corrected taxon", "locality": "Corrected loc"})
        assert r.status_code == 200
        deadline = time.time() + 10
        while time.time() < deadline:
            got = c.get("/api/specimen/100").get_json()
            if got["taxon"] == "Corrected taxon":
                break
            time.sleep(0.05)
        else:
            pytest.fail("更正未同步到副本")

        # 通过网页撤回 -> 副本删除且 404
        assert c.post("/api/demo/delete", json={"id": 100}).status_code == 200
        deadline = time.time() + 10
        while time.time() < deadline:
            if c.get("/api/specimen/100").status_code == 404:
                break
            time.sleep(0.05)
        else:
            pytest.fail("撤回未传播到副本")

        # 停止
        r = c.post("/api/sync/stop")
        assert r.status_code == 200
        engine.wait_until_idle(10)

    def test_status_reports_progress_and_errors(self, client):
        c, engine = client
        c.post("/api/sync/start")
        assert wait_phase(engine, lambda m: m["phase"] == replica_db.PHASE_RUNNING)
        r = c.get("/api/status")
        assert r.status_code == 200
        st = r.get_json()
        assert st["phase"] == replica_db.PHASE_RUNNING
        assert st["last_applied_seq"] >= 5
        assert st["source"]["specimen_count"] == 5
        assert st["worker_alive"] is True
        c.post("/api/sync/stop")
        engine.wait_until_idle(10)

    def test_start_twice_is_idempotent(self, client):
        c, engine = client
        assert c.post("/api/sync/start").get_json()["started"] is True
        assert c.post("/api/sync/start").get_json()["started"] is False
        c.post("/api/sync/stop")
        engine.wait_until_idle(10)

    def test_source_listing_and_index_page(self, client):
        c, _ = client
        rows = c.get("/api/source/specimens").get_json()
        assert len(rows) == 5 and rows[0]["id"] == 1
        page = c.get("/")
        assert page.status_code == 200
        assert "标本登记库".encode() in page.data

    def test_demo_errors(self, client):
        c, engine = client
        c.post("/api/sync/start")
        wait_phase(engine, lambda m: m["phase"] == replica_db.PHASE_RUNNING)
        # 主键冲突
        r = c.post("/api/demo/insert", json={
            "id": 1, "taxon": "dup", "locality": "dup"})
        assert r.status_code == 409
        # 更正不存在的标本
        r = c.post("/api/demo/update", json={
            "id": 8888, "taxon": "x", "locality": "y"})
        assert r.status_code == 404
        # 撤回不存在的标本
        r = c.post("/api/demo/delete", json={"id": 8888})
        assert r.status_code == 404
        c.post("/api/sync/stop")
        engine.wait_until_idle(10)
