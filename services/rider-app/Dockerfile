# Stage 1: Build frontend
FROM node:20-alpine AS frontend-builder

WORKDIR /app/frontend

COPY services/rider-app/frontend/package.json .
RUN npm cache clean --force && npm install

COPY services/rider-app/frontend/src ./src
COPY services/rider-app/frontend/public ./public
COPY services/rider-app/frontend/index.html .
COPY services/rider-app/frontend/vite.config.ts .
COPY services/rider-app/frontend/tsconfig.json .
COPY services/rider-app/frontend/tailwind.config.js .
COPY services/rider-app/frontend/postcss.config.js .

RUN npm run build


# Stage 2: Python backend with built frontend
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    fastapi[standard] \
    uvicorn[standard] \
    redis[hiredis] \
    httpx \
    prometheus-client \
    python-dotenv \
    websockets

# Copy common modules
COPY services/common /app/services/common

# Copy rider-app backend
COPY services/rider-app/*.py /app/services/rider-app/

# Copy built frontend from builder stage
COPY --from=frontend-builder /app/frontend/dist /app/services/rider-app/frontend/dist

EXPOSE 8004

WORKDIR /app/services/rider-app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8004"]
