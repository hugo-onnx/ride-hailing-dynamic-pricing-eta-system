"""
Rider App Backend — consumer-facing ride-hailing demo.
"""

import asyncio
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Optional

import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.insert(0, os.path.dirname(__file__))
from services.common.config import REDIS_HOST, REDIS_PORT, CITY
from demand import DemandAggregator
from inference_client import InferenceClient
from osrm_client import AsyncOSRMClient
from ride_simulator import RideSimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Metrics
WS_CONNECTIONS = Gauge("rider_ws_connections", "Active WebSocket connections")
QUOTES_REQUESTED = Counter("rider_quotes_total", "Total quotes requested")
RIDES_REQUESTED = Counter("rider_rides_total", "Total rides requested")
QUOTE_LATENCY = Histogram(
    "rider_quote_latency_seconds", "Quote request latency",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# Global state
redis_client: Optional[redis.Redis] = None
aggregator: Optional[DemandAggregator] = None
inference: Optional[InferenceClient] = None
osrm: Optional[AsyncOSRMClient] = None
simulator: Optional[RideSimulator] = None

# WebSocket connections and their subscribed ride IDs
ws_connections: dict[WebSocket, dict] = {}


async def demand_broadcast_loop(interval: float = 5.0):
    """Broadcast demand heatmap data to all connected clients."""
    while True:
        try:
            if ws_connections and aggregator:
                # Collect unique windows
                windows = set()
                for info in ws_connections.values():
                    windows.add(info.get("demand_window", 5))

                cached = {}
                for w in windows:
                    hexagons = await aggregator.get_current_demand(w)
                    cached[w] = {
                        "type": "demand_update",
                        "window_minutes": w,
                        "hexagons": hexagons,
                    }

                disconnected = []
                for ws, info in list(ws_connections.items()):
                    w = info.get("demand_window", 5)
                    payload = cached.get(w)
                    if payload:
                        try:
                            await ws.send_text(json.dumps(payload))
                        except Exception:
                            disconnected.append(ws)

                for ws in disconnected:
                    ws_connections.pop(ws, None)
                    WS_CONNECTIONS.set(len(ws_connections))

        except Exception as e:
            logger.error(f"Demand broadcast error: {e}")

        await asyncio.sleep(interval)


async def ride_update_loop(interval: float = 0.5):
    """Push ride state updates to subscribed clients."""
    while True:
        try:
            if ws_connections and simulator:
                disconnected = []
                for ws, info in list(ws_connections.items()):
                    ride_id = info.get("ride_id")
                    if not ride_id:
                        continue
                    update = simulator.get_ride_update(ride_id)
                    if update:
                        try:
                            await ws.send_text(json.dumps(update))
                        except Exception:
                            disconnected.append(ws)

                for ws in disconnected:
                    ws_connections.pop(ws, None)
                    WS_CONNECTIONS.set(len(ws_connections))

        except Exception as e:
            logger.error(f"Ride update error: {e}")

        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, aggregator, inference, osrm, simulator

    logger.info("Starting rider-app service...")

    # Connect to Redis
    while True:
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
            )
            await redis_client.ping()
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            break
        except Exception as e:
            logger.warning(f"Waiting for Redis... {e}")
            await asyncio.sleep(3)

    aggregator = DemandAggregator(redis_client)
    inference = InferenceClient()
    osrm = AsyncOSRMClient()
    simulator = RideSimulator(osrm)

    demand_task = asyncio.create_task(demand_broadcast_loop())
    ride_task = asyncio.create_task(ride_update_loop())

    yield

    logger.info("Shutting down...")
    demand_task.cancel()
    ride_task.cancel()
    for t in [demand_task, ride_task]:
        try:
            await t
        except asyncio.CancelledError:
            pass

    if inference:
        await inference.close()
    if osrm:
        await osrm.close()
    if redis_client:
        await redis_client.close()


app = FastAPI(
    title="Rider App",
    description="Consumer-facing ride-hailing demo",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "rider-app", "city": CITY}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/quote")
async def get_quote(
    origin_lat: float = Query(...),
    origin_lng: float = Query(...),
    dest_lat: float = Query(...),
    dest_lng: float = Query(...),
):
    QUOTES_REQUESTED.inc()
    start = time.perf_counter()
    try:
        quote = await inference.get_quote(origin_lat, origin_lng, dest_lat, dest_lng)
        QUOTE_LATENCY.observe(time.perf_counter() - start)
        return quote
    except Exception as e:
        logger.error(f"Quote request failed: {e}")
        return {"error": str(e)}, 502


@app.post("/api/ride/request")
async def request_ride(
    origin_lat: float = Query(...),
    origin_lng: float = Query(...),
    dest_lat: float = Query(...),
    dest_lng: float = Query(...),
):
    RIDES_REQUESTED.inc()
    # Get quote first
    try:
        quote = await inference.get_quote(origin_lat, origin_lng, dest_lat, dest_lng)
    except Exception as e:
        logger.error(f"Quote failed during ride request: {e}")
        quote = {}

    session = simulator.create_ride(
        pickup_lat=origin_lat,
        pickup_lng=origin_lng,
        dropoff_lat=dest_lat,
        dropoff_lng=dest_lng,
        quote=quote,
    )
    return {"ride_id": session.ride_id, "state": session.state.value}


@app.post("/api/ride/{ride_id}/cancel")
async def cancel_ride(ride_id: str):
    success = simulator.cancel_ride(ride_id)
    if not success:
        return {"error": "Ride not found or already finished"}, 404
    return {"ride_id": ride_id, "state": "CANCELLED"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_connections[websocket] = {"demand_window": 5, "ride_id": None}
    WS_CONNECTIONS.set(len(ws_connections))
    logger.info(f"WS client connected. Total: {len(ws_connections)}")

    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                try:
                    msg = json.loads(data)
                except json.JSONDecodeError:
                    continue

                msg_type = msg.get("type")

                if msg_type == "ping":
                    await websocket.send_json({"type": "pong"})

                elif msg_type == "subscribe_ride":
                    ride_id = msg.get("ride_id")
                    if ride_id and websocket in ws_connections:
                        ws_connections[websocket]["ride_id"] = ride_id
                        # Send immediate state
                        update = simulator.get_ride_update(ride_id)
                        if update:
                            await websocket.send_text(json.dumps(update))

                elif msg_type == "set_demand_window":
                    window = msg.get("window", 5)
                    if websocket in ws_connections:
                        ws_connections[websocket]["demand_window"] = window

            except asyncio.TimeoutError:
                await websocket.send_json({"type": "keepalive"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        ws_connections.pop(websocket, None)
        WS_CONNECTIONS.set(len(ws_connections))
        logger.info(f"WS client disconnected. Total: {len(ws_connections)}")


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
        return {"message": "Rider App API", "docs": "/docs"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
