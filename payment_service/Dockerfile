FROM python:3.11-slim

WORKDIR /app

COPY shared /packages/shared
COPY payment_service /app/service

RUN pip install --no-cache-dir /packages/shared \
    && pip install --no-cache-dir -r service/requirements.txt

WORKDIR /app/service

ENV PYTHONPATH=/app/service:/packages/shared

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
