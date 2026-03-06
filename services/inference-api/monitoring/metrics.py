from datetime import datetime, timezone
import redis

from prometheus_client import Counter, Histogram, make_asgi_app

METRIC_TTL = 3600  # 1 hour

# --- Prometheus metrics ---

REQUEST_COUNTER = Counter(
    "inference_api_requests_total",
    "Total requests to inference API endpoints",
    ["endpoint", "status"],
)

REQUEST_LATENCY = Histogram(
    "inference_api_request_duration_seconds",
    "End-to-end request latency for inference API endpoints",
    ["endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

FEATURE_FETCH_LATENCY = Histogram(
    "inference_api_feature_fetch_duration_seconds",
    "Time spent fetching features from Redis",
    ["endpoint"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
)

MODEL_INFERENCE_LATENCY = Histogram(
    "inference_api_model_inference_duration_seconds",
    "Time spent running ML model inference",
    ["model"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1],
)

OSRM_FALLBACK_COUNTER = Counter(
    "inference_api_osrm_fallbacks_total",
    "Number of times haversine fallback was used instead of OSRM",
)

metrics_app = make_asgi_app()


def record_latency(redis_client: redis.Redis, key: str, duration_ms: float):
    """Record latency metric in a sorted set with timestamp as score."""
    ts = datetime.now(timezone.utc).isoformat()
    redis_client.zadd(
        f"metrics:latency:{key}",
        {ts: duration_ms},
    )
    redis_client.expire(f"metrics:latency:{key}", METRIC_TTL)


def record_feature_freshness(redis_client: redis.Redis, window: int, delay_sec: int):
    """Record feature freshness delay in a list."""
    redis_client.lpush(
        f"metrics:feature_freshness:{window}m",
        delay_sec,
    )
    redis_client.ltrim(f"metrics:feature_freshness:{window}m", 0, 1000)
