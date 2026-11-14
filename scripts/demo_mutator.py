"""持续演示写入器：周期性对源库做增 / 改 / 撤，模拟鉴定员的日常操作。

通过 Docker Compose profile ``mutator`` 启用：
    docker compose --profile mutator up
也可以直接运行：
    SOURCE_DB=/data/source/specimens.db python -m scripts.demo_mutator
"""
import os
import random
import time

from app.source_db import (
    connect,
    delete_specimen,
    init_source,
    insert_specimen,
    update_specimen,
)

SOURCE_DB = os.environ.get("SOURCE_DB", "/data/source/specimens.db")
INTERVAL = float(os.environ.get("MUTATOR_INTERVAL", "3"))

TAXA = [
    ("Velociraptor mongoliensis", "Djadochta Formation, Gobi Desert"),
    ("Stegosaurus stenops", "Morrison Formation, Wyoming"),
    ("Pteranodon longiceps", "Niobrara Chalk, Kansas"),
    ("Anomalocaris canadensis", "Burgess Shale, British Columbia"),
    ("Metasequoia glyptostroboides", "Jehol Biota, Liaoning"),
]


def next_id(path: str) -> int:
    conn = connect(path, readonly=True)
    try:
        row = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 AS m FROM specimens").fetchone()
        return int(row["m"])
    finally:
        conn.close()


def main() -> None:
    init_source(SOURCE_DB)
    print(f"[mutator] 开始持续写入源库 {SOURCE_DB}，间隔 {INTERVAL}s", flush=True)
    while True:
        time.sleep(INTERVAL)
        action = random.choice(["insert", "insert", "update", "delete"])
        try:
            if action == "insert":
                taxon, locality = random.choice(TAXA)
                new_id = next_id(SOURCE_DB)
                insert_specimen(SOURCE_DB, new_id, taxon, locality)
                print(f"[mutator] INSERT id={new_id} {taxon}", flush=True)
            else:
                conn = connect(SOURCE_DB, readonly=True)
                try:
                    row = conn.execute(
                        "SELECT id FROM specimens ORDER BY RANDOM() LIMIT 1"
                    ).fetchone()
                finally:
                    conn.close()
                if row is None:
                    continue
                sid = row["id"]
                if action == "update":
                    taxon, locality = random.choice(TAXA)
                    update_specimen(SOURCE_DB, sid, f"{taxon} (rev.)", locality)
                    print(f"[mutator] UPDATE id={sid}", flush=True)
                else:
                    delete_specimen(SOURCE_DB, sid)
                    print(f"[mutator] DELETE id={sid}", flush=True)
        except Exception as exc:  # 写入器不能因单次错误退出
            print(f"[mutator] 操作失败（已忽略）: {type(exc).__name__}: {exc}", flush=True)


if __name__ == "__main__":
    main()
