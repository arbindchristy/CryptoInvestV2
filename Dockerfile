FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

WORKDIR /app

COPY pyproject.toml requirements.txt README.md /app/
COPY src /app/src
COPY scripts /app/scripts

RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt

RUN chmod +x /app/scripts/run_api.py /app/scripts/run_worker.py

EXPOSE 8000
