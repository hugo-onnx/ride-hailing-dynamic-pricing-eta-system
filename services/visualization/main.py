"""
Visualization Backend Service
Real-time H3 hexagon demand data via WebSocket
"""

import asyncio
import json
import logging
import redis.asyncio as redis
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from services.common.config import REDIS_HOST, REDIS_PORT, CITY
from services.common.time_utils import floor_timestamp

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Metrics
WEBSOCKET_CONNECTIONS = Gauge(
    'viz_websocket_connections',
    'Active WebSocket connections',
    ['city']
)

HEXAGONS_SENT = Counter(
    'viz_hexagons_sent_total',
    'Total hexagon updates sent',
    ['city', 'window']
)

BROADCAST_LATENCY = Histogram(
    'viz_broadcast_latency_seconds',
    'Time to broadcast updates',
    ['city'],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)


class ConnectionManager:
    """Manages WebSocket connections and per-client window preferences"""

    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.client_windows: dict[WebSocket, int] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        self.client_windows[websocket] = 5  # default window
        WEBSOCKET_CONNECTIONS.labels(city=CITY).set(len(self.active_connections))
        logger.info(f"Client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        self.client_windows.pop(websocket, None)
        WEBSOCKET_CONNECTIONS.labels(city=CITY).set(len(self.active_connections))
        logger.info(f"Client disconnected. Total: {len(self.active_connections)}")

    def set_window(self, websocket: WebSocket, window: int):
        if websocket in self.client_windows:
            self.client_windows[websocket] = window

    async def broadcast(self, cached_data: dict):
        """Send each client data for its preferred window"""
        if not self.active_connections:
            return

        disconnected = []
        for connection in list(self.active_connections):
            window = self.client_windows.get(connection, 5)
            payload = cached_data.get(window)
            if payload is None:
                continue
            try:
                await connection.send_text(json.dumps(payload))
            except Exception as e:
                logger.warning(f"Failed to send to client: {e}")
                disconnected.append(connection)

        for conn in disconnected:
            self.disconnect(conn)


class DemandAggregator:
    """Fetches and aggregates H3 demand data from Redis"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
    
    async def get_current_demand(self, window_minutes: int = 5) -> list[dict]:
        """
        Get current demand for all H3 cells in the specified window.
        Returns list of {h3_index, ride_requests, idle_drivers, ...}
        """
        now = datetime.now(timezone.utc)
        window_start = floor_timestamp(now, window_minutes)
        
        # Pattern: madrid:h3_index:5m:timestamp
        pattern = f"{CITY}:*:{window_minutes}m:{window_start.isoformat()}"
        
        hexagons = []
        
        try:
            # Scan for matching keys
            cursor = 0
            keys = []
            while True:
                cursor, batch = await self.redis.scan(cursor, match=pattern, count=1000)
                keys.extend(batch)
                if cursor == 0:
                    break
            
            if not keys:
                return hexagons
            
            # Fetch all data in pipeline
            pipe = self.redis.pipeline()
            for key in keys:
                pipe.hgetall(key)
            
            results = await pipe.execute()
            
            for key, data in zip(keys, results):
                if not data:
                    continue
                
                h3_index = data.get('h3_res8', '')
                if not h3_index:
                    # Extract from key if not in data
                    parts = key.split(':')
                    if len(parts) >= 2:
                        h3_index = parts[1]
                
                ride_requests = int(data.get('ride_requests', 0))
                idle_drivers = int(data.get('idle_drivers', 0))
                on_trip_drivers = int(data.get('on_trip_drivers', 0))
                
                # Demand ratio based only on ride requests
                # Scale: 0-5 low, 5-15 medium, 15-30 high, 30+ critical
                demand_ratio = ride_requests
                
                hexagons.append({
                    'h3_index': h3_index,
                    'ride_requests': ride_requests,
                    'idle_drivers': idle_drivers,
                    'on_trip_drivers': on_trip_drivers,
                    'total_drivers': idle_drivers + on_trip_drivers,
                    'demand_ratio': demand_ratio,
                    'window_start': data.get('window_start', window_start.isoformat()),
                })
            
            return hexagons
            
        except Exception as e:
            logger.error(f"Error fetching demand data: {e}")
            return []
    
    async def get_stats(self, window_minutes: int = 5) -> dict:
        """Get aggregate statistics"""
        hexagons = await self.get_current_demand(window_minutes)

        if not hexagons:
            return {
                'total_hexagons': 0,
                'total_ride_requests': 0,
                'total_idle_drivers': 0,
                'total_on_trip_drivers': 0,
                'avg_demand_ratio': 0,
            }

        # Read live driver counts written by event-producer each second.
        # These reflect actual unique online drivers, avoiding the inflation
        # caused by summing per-cell window counts (drivers cross cell boundaries).
        live = await self.redis.hgetall(f"{CITY}:drivers:live")
        total_idle = int(live.get("idle", 0)) if live else sum(h['idle_drivers'] for h in hexagons)
        total_on_trip = int(live.get("on_trip", 0)) if live else sum(h['on_trip_drivers'] for h in hexagons)

        return {
            'total_hexagons': len(hexagons),
            'total_ride_requests': sum(h['ride_requests'] for h in hexagons),
            'total_idle_drivers': total_idle,
            'total_on_trip_drivers': total_on_trip,
            'avg_demand_ratio': round(
                sum(h['demand_ratio'] for h in hexagons) / len(hexagons), 2
            ),
        }


# Global instances
manager = ConnectionManager()
redis_client: Optional[redis.Redis] = None
aggregator: Optional[DemandAggregator] = None
broadcast_task: Optional[asyncio.Task] = None


async def broadcast_loop(interval: float = 1.0):
    """Background task to broadcast demand updates to all connected clients"""
    logger.info(f"Starting broadcast loop (interval: {interval}s)")

    while True:
        try:
            if manager.active_connections and aggregator:
                import time
                start = time.time()

                # Fetch data for all windows currently preferred by connected clients
                unique_windows = set(manager.client_windows.values())
                cached: dict[int, dict] = {}
                for w in unique_windows:
                    hexagons = await aggregator.get_current_demand(w)
                    stats = await aggregator.get_stats(w)
                    cached[w] = {
                        'type': 'demand_update',
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'window_minutes': w,
                        'hexagons': hexagons,
                        'stats': stats,
                    }
                    HEXAGONS_SENT.labels(city=CITY, window=f'{w}m').inc(len(hexagons))

                await manager.broadcast(cached)
                BROADCAST_LATENCY.labels(city=CITY).observe(time.time() - start)

        except Exception as e:
            logger.error(f"Broadcast error: {e}")

        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global redis_client, aggregator, broadcast_task
    
    # Startup
    logger.info("Starting visualization service...")
    
    # Connect to Redis
    while True:
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
            )
            await redis_client.ping()
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            break
        except Exception as e:
            logger.warning(f"Waiting for Redis... {e}")
            await asyncio.sleep(3)
    
    aggregator = DemandAggregator(redis_client)
    
    # Start broadcast loop
    broadcast_task = asyncio.create_task(broadcast_loop(1.0))
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    if broadcast_task:
        broadcast_task.cancel()
        try:
            await broadcast_task
        except asyncio.CancelledError:
            pass
    
    if redis_client:
        await redis_client.close()


app = FastAPI(
    title="Ride-Hailing Demand Visualization",
    description="Real-time H3 hexagon demand visualization",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "city": CITY}


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/demand")
async def get_demand(window: int = Query(default=5, ge=1, le=15)):
    """Get current demand data (REST endpoint)"""
    if not aggregator:
        return {"error": "Service not ready"}
    
    hexagons = await aggregator.get_current_demand(window)
    stats = await aggregator.get_stats(window)
    
    return {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'window_minutes': window,
        'hexagons': hexagons,
        'stats': stats,
    }


@app.get("/api/stats")
async def get_stats(window: int = Query(default=5, ge=1, le=15)):
    """Get aggregate statistics"""
    if not aggregator:
        return {"error": "Service not ready"}
    
    return await aggregator.get_stats(window)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await manager.connect(websocket)
    
    try:
        # Send initial data
        if aggregator:
            hexagons = await aggregator.get_current_demand(5)
            stats = await aggregator.get_stats(5)
            
            await websocket.send_json({
                'type': 'initial',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'window_minutes': 5,
                'hexagons': hexagons,
                'stats': stats,
            })
        
        # Keep connection alive and handle client messages
        while True:
            try:
                data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=30.0
                )
                
                # Handle ping/pong
                try:
                    message = json.loads(data)
                except json.JSONDecodeError:
                    logger.warning(f"Received invalid JSON from client: {data!r}")
                    continue
                if message.get('type') == 'ping':
                    await websocket.send_json({'type': 'pong'})
                elif message.get('type') == 'request_update':
                    window = message.get('window', 5)
                    # Persist window preference so broadcast loop uses it going forward
                    manager.set_window(websocket, window)
                    hexagons = await aggregator.get_current_demand(window)
                    stats = await aggregator.get_stats(window)
                    await websocket.send_json({
                        'type': 'demand_update',
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'window_minutes': window,
                        'hexagons': hexagons,
                        'stats': stats,
                    })
                    
            except asyncio.TimeoutError:
                # Send keepalive
                await websocket.send_json({'type': 'keepalive'})
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


# Serve static files (frontend)
frontend_path = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.exists(frontend_path):
    app.mount("/assets", StaticFiles(directory=os.path.join(frontend_path, "assets")), name="assets")
    
    @app.get("/")
    async def serve_frontend():
        return FileResponse(os.path.join(frontend_path, "index.html"))
else:
    @app.get("/")
    async def index():
        return {"message": "Visualization API", "docs": "/docs"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
