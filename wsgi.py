"""gunicorn / flask 运行入口。"""
from app.web import app  # noqa: F401

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
