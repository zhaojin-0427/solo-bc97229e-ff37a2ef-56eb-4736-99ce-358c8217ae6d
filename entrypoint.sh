#!/bin/sh
# 容器入口：确保示例源库存在（含演示数据与 CDC 触发器），然后启动 Web + 后台同步。
set -e

echo "[entrypoint] 初始化示例源库: ${SOURCE_DB}"
python -m scripts.seed_source

echo "[entrypoint] 启动 gunicorn (1 worker, 多线程承载同步后台与 Web 请求)"
exec gunicorn --bind 0.0.0.0:8000 \
              --workers 1 \
              --threads 8 \
              --timeout 120 \
              --access-logfile - \
              --error-logfile - \
              wsgi:app
