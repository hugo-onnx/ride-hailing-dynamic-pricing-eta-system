import time

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from services.common.config import CITY
from dependencies import get_redis, parse_timestamp, resolve_h3
from features import fetch_window
from derive import derive_features
from eta.features import assemble_pickup_features
from monitoring.metrics import (
    record_latency,
    REQUEST_COUNTER,
    REQUEST_LATENCY,
    FEATURE_FETCH_LATENCY,
    MODEL_INFERENCE_LATENCY,
)
from schemas import ETAQuoteResponse

router = APIRouter(prefix="/v1/eta", tags=["ETA"])


@router.post(
    "/quote",
    response_model=ETAQuoteResponse,
    summary="Predict pickup ETA",
    description=(
        "Uses an XGBoost model to predict estimated time of arrival for driver pickup. "
        "The model takes trip distance and real-time marketplace features "
        "(supply/demand ratio, surge pressure, driver availability) as inputs.\n\n"
        "**Output range:** 2 – 60 minutes (clamped)."
    ),
    responses={503: {"description": "ETA model not loaded"}},
)
def eta_quote(
    request: Request,
    lat: float = Query(..., description="Pickup latitude", examples=[40.4168]),
    lng: float = Query(..., description="Pickup longitude", examples=[-3.7038]),
    trip_distance_km: float = Query(..., gt=0, description="Trip distance in km", examples=[5.2]),
    timestamp: str | None = Query(None, description="ISO 8601 timestamp (defaults to now)"),
    redis_client=Depends(get_redis),
):
    pickup_model = request.app.state.pickup_model
    if pickup_model is None:
        REQUEST_COUNTER.labels(endpoint="eta_quote", status="503").inc()
        raise HTTPException(
            status_code=503,
            detail="ETA model not loaded. Ensure the model file exists at the configured path.",
        )

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
    FEATURE_FETCH_LATENCY.labels(endpoint="eta_quote").observe(time.perf_counter() - feature_start)

    features_5m = derive_features(raw_5m)
    eta_features = assemble_pickup_features(
        trip_distance_km=trip_distance_km,
        features_5m=features_5m,
    )

    model_start = time.perf_counter()
    eta_seconds = pickup_model.predict(eta_features)
    MODEL_INFERENCE_LATENCY.labels(model="pickup_eta").observe(time.perf_counter() - model_start)

    latency_ms = (time.perf_counter() - start) * 1000
    record_latency(redis_client, "eta", latency_ms)
    REQUEST_LATENCY.labels(endpoint="eta_quote").observe(latency_ms / 1000)
    REQUEST_COUNTER.labels(endpoint="eta_quote", status="200").inc()

    return {
        "city": CITY,
        "h3_res8": h3_index,
        "trip_distance_km": trip_distance_km,
        "eta_seconds": int(eta_seconds),
        "eta_minutes": round(eta_seconds / 60, 1),
        "latency_ms": round(latency_ms, 2),
    }
