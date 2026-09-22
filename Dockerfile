FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY scan_defi.py monitoring.py telegram_bot.py docker_guard.py sui_support.py config.yaml tokens.yaml ./
RUN mkdir -p /app/data /app/logs /app/reports /app/backups

CMD ["python", "scan_defi.py"]
