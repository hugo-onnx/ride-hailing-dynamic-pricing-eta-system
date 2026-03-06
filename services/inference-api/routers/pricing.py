import time

from fastapi import APIRouter, Depends, Query

from services.common.config import CITY
from dependencies import get_redis, parse_timestamp, resolve_h3
from features import fetch_window
from derive import derive_features
from pricing import compute_price_multiplier
from monitoring.metrics import (
    record_latency,
    REQUEST_COUNTER,
    REQUEST_LATENCY,
    FEATURE_FETCH_LATENCY,
)
from monitoring.drift import record_feature_snapshot
from schemas import PricingQuoteResponse

router = APIRouter(prefix="/v1/pricing", tags=["Pricing"])


@router.post(
    "/quote",
    response_model=PricingQuoteResponse,
    summary="Get a dynamic pricing quote",
    description=(
        "Computes a surge pricing multiplier for the given location based on "
        "real-time supply/demand conditions in the 5-minute feature window.\n\n"
        "**Pricing factors:**\n"
        "- Supply-demand imbalance (primary driver)\n"
        "- Deadhead distance inefficiency\n"
        "- Low-volume dampening when < 5 requests in the window\n\n"
        "Multiplier is clamped to **1.0x – 2.0x**."
    ),
)
def pricing_quote(
    lat: float = Query(..., description="Latitude in decimal degrees", examples=[40.4168]),
    lng: float = Query(..., description="Longitude in decimal degrees", examples=[-3.7038]),
    timestamp: str | None = Query(None, description="ISO 8601 timestamp (defaults to now)"),
    redis_client=Depends(get_redis),
):
    start = time.perf_counter()
    ts = parse_timestamp(timestamp)
    h3_index = resolve_h3(lat, lng)

    feature_start = time.perf_counter()
    raw_5m = fetch_window(
        redis_client=redis_client,
        city=CITY,
        h3_index=h3_index,
        window=5,
        ts=ts,
    )
    FEATURE_FETCH_LATENCY.labels(endpoint="pricing_quote").observe(time.perf_counter() - feature_start)

    features_5m = derive_features(raw_5m)
    pricing_result = compute_price_multiplier(features_5m)

    record_feature_snapshot(
        redis_client=redis_client,
        city=CITY,
        features=features_5m,
    )

    latency_ms = (time.perf_counter() - start) * 1000
    record_latency(redis_client, "pricing", latency_ms)
    REQUEST_LATENCY.labels(endpoint="pricing_quote").observe(latency_ms / 1000)
    REQUEST_COUNTER.labels(endpoint="pricing_quote", status="200").inc()

    return {
        "city": CITY,
        "h3_res8": h3_index,
        "timestamp": ts.isoformat(),
        "features": features_5m,
        "pricing": pricing_result,
        "latency_ms": round(latency_ms, 2),
    }
