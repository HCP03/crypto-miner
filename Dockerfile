FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=UTC

WORKDIR /app

COPY requirements-vm.txt .
RUN pip install --no-cache-dir -r requirements-vm.txt

COPY core/ ./core/
COPY markets/ ./markets/
COPY workers/ ./workers/

CMD python -u -m ${WORKER_MODULE:-workers.binance_worker}