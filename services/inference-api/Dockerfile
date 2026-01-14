FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    fastapi>=0.128.0 \
    uvicorn>=0.30.0 \
    redis>=7.1.0

# Copy application code
COPY services/common /app/common
COPY services/inference-api/main.py /app/

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]