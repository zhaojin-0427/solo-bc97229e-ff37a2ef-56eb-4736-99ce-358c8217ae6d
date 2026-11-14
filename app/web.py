"""自然博物馆标本登记库同步 Web 控制台（Flask）。

页面与 API：
  GET  /                 控制台页面（进度、错误、操作按钮、查询框）
  POST /api/sync/start   启动后台同步
  POST /api/sync/stop    请求停止后台同步
  GET  /api/status       同步进度与错误（JSON）
  GET  /api/specimen/<id>  按标本号查询副本（就绪前返回 503）

  # 演示用：直接对源库提交增 / 改 / 撤（鉴定员的日常操作）
  POST /api/demo/insert
  POST /api/demo/update
  POST /api/demo/delete
  GET  /api/source/specimens  查看源库当前标本（仅演示辅助）
"""
from __future__ import annotations

import os

from flask import Flask, jsonify, render_template, request

from . import replica_db, source_db
from .sync import SyncEngine

SOURCE_DB = os.environ.get("SOURCE_DB", "/data/source/specimens.db")
REPLICA_DB = os.environ.get("REPLICA_DB", "/data/replica/specimens_replica.db")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1.0"))
# 容器启动时是否自动开始同步（1/0）
AUTO_START = os.environ.get("AUTO_START", "1") == "1"


def create_app(
    source_db_path: str | None = None,
    replica_db_path: str | None = None,
    poll_interval: float | None = None,
    auto_start: bool | None = None,
) -> Flask:
    app = Flask(__name__)
    source_path = source_db_path or SOURCE_DB
    replica_path = replica_db_path or REPLICA_DB
    interval = POLL_INTERVAL if poll_interval is None else poll_interval
    do_auto_start = AUTO_START if auto_start is None else auto_start

    engine = SyncEngine(source_path, replica_path, poll_interval=interval)
    app.config["SYNC_ENGINE"] = engine

    # 确保示例源库与副本结构存在（源库不存在则播种演示数据）
    source_db.init_demo(source_path)
    replica_db.init_replica(replica_path)
    if do_auto_start:
        engine.start()

    # ----------------------------- 页面 ----------------------------- #
    @app.get("/")
    def index():
        return render_template("index.html")

    # --------------------------- 同步控制 --------------------------- #
    @app.post("/api/sync/start")
    def sync_start():
        # 每次启动前确保源库 CDC 对象齐全（兼容外部替换的库文件）
        source_db.init_source(source_path)
        started = engine.start()
        return jsonify({"started": started, "status": engine.status()})

    @app.post("/api/sync/stop")
    def sync_stop():
        stopped = engine.stop()
        return jsonify({"stop_requested": stopped, "status": engine.status()})

    @app.get("/api/status")
    def status():
        st = engine.status()
        st["source"] = {
            "path": source_path,
            "specimen_count": _safe(source_db.count_specimens, source_path),
            "max_change_seq": _safe(source_db.max_change_seq, source_path),
        }
        return jsonify(st)

    # --------------------------- 副本查询 --------------------------- #
    @app.get("/api/specimen/<int:specimen_id>")
    def get_specimen(specimen_id: int):
        # 就绪前不供查询
        if not replica_db.is_ready(replica_path):
            meta = replica_db.get_meta(replica_path)
            return (
                jsonify(
                    {
                        "error": "REPLICA_NOT_READY",
                        "message": "副本尚未完成首次复制，暂不提供查询",
                        "phase": meta["phase"],
                    }
                ),
                503,
            )
        row = replica_db.get_specimen(replica_path, specimen_id)
        if row is None:
            return (
                jsonify(
                    {
                        "error": "NOT_FOUND",
                        "message": f"副本中不存在标本 {specimen_id}"
                        "（可能已被鉴定员撤回）",
                        "id": specimen_id,
                    }
                ),
                404,
            )
        return jsonify(row)

    # --------------------- 演示：源库增 / 改 / 撤 --------------------- #
    @app.post("/api/demo/insert")
    def demo_insert():
        data = request.get_json(force=True)
        try:
            specimen_id = int(data["id"])
            source_db.insert_specimen(
                source_path, specimen_id, str(data["taxon"]), str(data["locality"])
            )
        except KeyError as exc:
            return jsonify({"error": "BAD_REQUEST", "message": str(exc)}), 400
        except Exception as exc:  # 主键冲突等
            return jsonify({"error": "SOURCE_ERROR", "message": str(exc)}), 409
        return jsonify({"ok": True})

    @app.post("/api/demo/update")
    def demo_update():
        data = request.get_json(force=True)
        try:
            specimen_id = int(data["id"])
            source_db.update_specimen(
                source_path, specimen_id, str(data["taxon"]), str(data["locality"])
            )
        except KeyError as exc:
            return jsonify({"error": "NOT_FOUND", "message": str(exc)}), 404
        except Exception as exc:
            return jsonify({"error": "SOURCE_ERROR", "message": str(exc)}), 409
        return jsonify({"ok": True})

    @app.post("/api/demo/delete")
    def demo_delete():
        data = request.get_json(force=True)
        try:
            specimen_id = int(data["id"])
            source_db.delete_specimen(source_path, specimen_id)
        except KeyError as exc:
            return jsonify({"error": "NOT_FOUND", "message": str(exc)}), 404
        return jsonify({"ok": True})

    @app.get("/api/source/specimens")
    def source_specimens():
        conn = source_db.connect(source_path, readonly=True)
        try:
            rows = conn.execute(
                "SELECT id, taxon, locality FROM specimens ORDER BY id"
            ).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])

    @app.get("/healthz")
    def healthz():
        return jsonify({"ok": True})

    return app


def _safe(fn, *args):
    """源库可能被外部占用/锁定，状态接口不应因此 500。"""
    try:
        return fn(*args)
    except Exception as exc:  # pragma: no cover - 防御性
        return f"error: {type(exc).__name__}: {exc}"


app = create_app()
