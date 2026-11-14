"""初始化/播种示例源库（容器入口调用，也可本地运行）。"""
import os

from app.source_db import init_demo

SOURCE_DB = os.environ.get("SOURCE_DB", "/data/source/specimens.db")

if __name__ == "__main__":
    init_demo(SOURCE_DB)
    print(f"[seed] 示例源库就绪: {SOURCE_DB}")
