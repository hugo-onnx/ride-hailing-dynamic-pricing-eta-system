import redis
import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI

from services.common.config import REDIS_HOST, REDIS_PORT
from monitoring.metrics import metrics_app
from eta.pickup_model import PickupETAEstimator
from eta.dropoff_model import DropoffETAEstimator
from routing.osrm import get_osrm_client

from routers.health import router as health_router
from routers.features import router as features_router
from routers.pricing import router as pricing_router
from routers.eta import router as eta_router
from routers.routing import router as routing_router
from routers.quotes import router as quotes_router
from routers.monitoring import router as monitoring_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PICKUP_MODEL_PATH = "/app/models/pickup_eta_model.joblib"
DROPOFF_MODEL_PATH = "/app/models/dropoff_eta_model.joblib"

# ── OpenAPI tag metadata ─────────────────────────────────────────────────────

TAGS_METADATA = [
    {
        "name": "System",
        "description": "Health checks and service status.",
    },
    {
        "name": "Features",
        "description": (
            "Real-time marketplace features aggregated from Kafka events into "
            "Redis tumbling windows (1m / 5m / 15m). Includes supply/demand "
            "ratio, surge pressure, and driver efficiency metrics."
        ),
    },
    {
        "name": "Pricing",
        "description": (
            "Dynamic surge pricing computed from real-time marketplace conditions. "
            "Multiplier range: **1.0x** (normal) to **2.0x** (peak surge)."
        ),
    },
    {
        "name": "ETA",
        "description": (
            "Pickup ETA prediction powered by XGBoost. Takes trip distance "
            "and live marketplace features as input."
        ),
    },
    {
        "name": "Routing",
        "description": (
            "Point-to-point routing via OSRM (Madrid road network). "
            "Automatically falls back to haversine distance when OSRM is unavailable."
        ),
    },
    {
        "name": "Trip Quotes",
        "description": (
            "End-to-end trip quotes combining OSRM routing, ML-predicted ETAs, "
            "congestion adjustment, and dynamic pricing into a single response."
        ),
    },
    {
        "name": "Monitoring",
        "description": "Feature drift detection and observability endpoints.",
    },
]


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_keepalive=True,
    )

    try:
        app.state.redis_client.ping()
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    try:
        app.state.pickup_model = PickupETAEstimator(PICKUP_MODEL_PATH)
        logger.info(f"Loaded pickup ETA model from {PICKUP_MODEL_PATH}")
    except FileNotFoundError:
        logger.warning(f"Pickup ETA model not found at {PICKUP_MODEL_PATH}")
        app.state.pickup_model = None
    except Exception as e:
        logger.error(f"Failed to load pickup ETA model: {e}")
        app.state.pickup_model = None

    try:
        app.state.dropoff_model = DropoffETAEstimator(DROPOFF_MODEL_PATH)
        logger.info(f"Loaded dropoff ETA model from {DROPOFF_MODEL_PATH}")
    except FileNotFoundError:
        logger.warning(f"Dropoff ETA model not found at {DROPOFF_MODEL_PATH}")
        app.state.dropoff_model = None
    except Exception as e:
        logger.error(f"Failed to load dropoff ETA model: {e}")
        app.state.dropoff_model = None

    try:
        osrm_client = get_osrm_client()
        app.state.osrm_available = osrm_client.health_check()
        if app.state.osrm_available:
            logger.info("OSRM routing service is available")
        else:
            logger.warning("OSRM routing service is not available, falling back to haversine")
    except Exception as e:
        logger.warning(f"Could not connect to OSRM: {e}")
        app.state.osrm_available = False

    yield

    if app.state.redis_client:
        app.state.redis_client.close()
        logger.info("Redis connection closed")


# ── Application ───────────────────────────────────────────────────────────────

API_DESCRIPTION = """\
Real-time inference API for a ride-hailing simulation in **Madrid**. \
Serves dynamic surge pricing, ML-predicted ETAs, and OSRM routing \
using marketplace features aggregated from Kafka into Redis time windows.
"""

app = FastAPI(
    title="Ride-Hailing Inference API",
    summary="ML-powered pricing, ETA, and routing for real-time ride-hailing",
    description=API_DESCRIPTION,
    version="1.0.0",
    openapi_tags=TAGS_METADATA,
    lifespan=lifespan,
    license_info={
        "name": "MIT",
    },
)

app.mount("/metrics", metrics_app)

app.include_router(health_router)
app.include_router(features_router)
app.include_router(pricing_router)
app.include_router(eta_router)
app.include_router(routing_router)
app.include_router(quotes_router)
app.include_router(monitoring_router)
