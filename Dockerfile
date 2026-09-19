FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATABASE_PATH=/data/mailarchive.db

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 非 root 运行；数据目录挂载到该卷
RUN mkdir -p /data && useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /data /app
USER appuser

VOLUME ["/data"]
EXPOSE 8000

# 容器启动：init_db 建表由 FastAPI lifespan 完成，依赖已在镜像内装好
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
