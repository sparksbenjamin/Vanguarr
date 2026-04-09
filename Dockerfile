# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data \
    DATABASE_URL=sqlite:////data/vanguarr.db \
    PROFILES_DIR=/data/profiles \
    LOGS_DIR=/data/logs \
    LOG_FILE=/data/logs/vanguarr.log \
    HOME=/tmp

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY config ./config
COPY README.md LICENSE .env.example ./

RUN mkdir -p /data/profiles /data/logs \
    && chgrp -R 0 /app /data \
    && chmod -R g=u /app /data

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
