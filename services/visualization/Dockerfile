# Stage 1: Build frontend
FROM node:20-alpine AS frontend-builder

WORKDIR /app/frontend

# Copy package.json first
COPY services/visualization/frontend/package.json .

# Install dependencies fresh
RUN npm cache clean --force && npm install

# Copy frontend source
COPY services/visualization/frontend/src ./src
COPY services/visualization/frontend/public ./public
COPY services/visualization/frontend/index.html .
COPY services/visualization/frontend/vite.config.ts .
COPY services/visualization/frontend/tsconfig.json .
COPY services/visualization/frontend/tailwind.config.js .
COPY services/visualization/frontend/postcss.config.js .

# Build
RUN npm run build


# Stage 2: Python backend with built frontend
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    fastapi[standard] \
    uvicorn[standard] \
    redis[hiredis] \
    prometheus-client \
    python-dotenv \
    websockets

# Copy common modules
COPY services/common /app/services/common

# Copy visualization service (backend)
COPY services/visualization/main.py /app/services/visualization/main.py

# Copy built frontend from builder stage
COPY --from=frontend-builder /app/frontend/dist /app/services/visualization/frontend/dist

EXPOSE 8003

CMD ["uvicorn", "services.visualization.main:app", "--host", "0.0.0.0", "--port", "8003"]
