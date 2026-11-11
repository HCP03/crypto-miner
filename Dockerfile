FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffer outputs for real-time logging
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=UTC

WORKDIR /app

# Install core dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project architecture
COPY core/ ./core/
COPY markets/ ./markets/
COPY workers/ ./workers/
# Note: We exclude analytics/ as that will run locally, saving RAM on the VM

# Run the supervisor by default
CMD ["python", "-m", "workers.main_worker", "--config", "config.yaml"]