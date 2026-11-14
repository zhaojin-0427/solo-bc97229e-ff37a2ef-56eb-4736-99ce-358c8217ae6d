FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SOURCE_DB=/data/source/specimens.db \
    REPLICA_DB=/data/replica/specimens_replica.db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts
COPY wsgi.py entrypoint.sh ./
RUN chmod +x entrypoint.sh

# 源库与副本各自独立目录、独立挂载卷
RUN mkdir -p /data/source /data/replica

EXPOSE 8000
ENTRYPOINT ["./entrypoint.sh"]
