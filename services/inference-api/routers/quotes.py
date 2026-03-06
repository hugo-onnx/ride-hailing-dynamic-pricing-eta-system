import time
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from services.common.config import CITY
from dependencies import get_redis, parse_timestamp, resolve_h3
from features import fetch_window
from derive import derive_features
from pricing import compute_price_multiplier
from eta.features import assemble_pickup_features
from eta.dropoff_adjust import adjust_dropoff_eta, compute_congestion_factor
from routing.osrm import get_osrm_client, OSRMError
from geo.utils import haversine_km
from monitoring.metrics import (
    record_latency,
    REQUEST_COUNTER,
    REQUEST_LATENCY,
    FEATURE_FETCH_LATENCY,
    MODEL_INFERENCE_LATENCY,
    OSRM_FALLBACK_COUNTER,
)
from schemas import TripQuoteResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["Trip Quotes"])

HAVERSINE_FALLBACK_SPEED_KMH = 35


@router.post(
    "/quote",
    response_model=TripQuoteResponse,
    summary="Get a complete trip quote",
    description=(
        "Returns a full trip quote combining OSRM routing, ML-predicted pickup ETA, "
        "congestion-adjusted dropoff ETA, and dynamic surge pricing.\n\n"
        "**Pipeline:**\n"
        "1. Route calculation via OSRM (haversine fallback)\n"
        "2. Feature retrieval from Redis (5-min window)\n"
        "3. Pickup ETA prediction (XGBoost)\n"
        "4. Dropoff ETA = OSRM duration x congestion factor\n"
        "5. Price = (base fare + distance x rate) x surge multiplier\n\n"
        "**Fare structure:** base = 1.50 EUR, per-km = 1.20 EUR/km."
    ),
    responses={
        400: {"description": "Invalid coordinates or origin/destination too close (< 100m)"},
        503: {"description": "ML models not loaded"},
    },
)
def trip_quote(
    request: Request,
    origin_lat: float = Query(..., description="Pickup latitude", examples=[40.4168]),
    origin_lng: float = Query(..., description="Pickup longitude", examples=[-3.7038]),
    dest_lat: float = Query(..., description="Destination latitude", examples=[40.4530]),
    dest_lng: float = Query(..., description="Destination longitude", examples=[-3.6883]),
    timestamp: str | None = Query(None, description="ISO 8601 timestamp (defaults to now)"),
    redis_client=Depends(get_redis),
):
    pickup_model = request.app.state.pickup_model
    dropoff_model = request.app.state.dropoff_model

    if pickup_model is None:
        REQUEST_COUNTER.labels(endpoint="trip_quote", status="503").inc()
        raise HTTPException(
            status_code=503,
            detail="Pickup ETA model not loaded. Please ensure model file exists.",
        )

    if dropoff_model is None:
        REQUEST_COUNTER.labels(endpoint="trip_quote", status="503").inc()
        raise HTTPException(
            status_code=503,
            detail="Dropoff ETA model not loaded. Please ensure model file exists.",
        )

    start = time.perf_counter()
    ts = parse_timestamp(timestamp)
    h3_origin = resolve_h3(origin_lat, origin_lng)

    # --- ROUTING ---
    osrm_client = get_osrm_client()
    routing_source = "osrm"

    route_geometry = None
    try:
        route = osrm_client.get_route(
            origin=(origin_lng, origin_lat),
            destination=(dest_lng, dest_lat),
        )
        trip_distance_km = route.distance_km
        osrm_duration_s = route.duration_s
        route_geometry = route.geometry
    except OSRMError as e:
        logger.warning(f"OSRM unavailable, using haversine fallback: {e}")
        routing_source = "haversine_fallback"
        OSRM_FALLBACK_COUNTER.inc()
        trip_distance_km = haversine_km(origin_lat, origin_lng, dest_lat, dest_lng)
        osrm_duration_s = (trip_distance_km / HAVERSINE_FALLBACK_SPEED_KMH) * 3600

    if trip_distance_km < 0.1:
        REQUEST_COUNTER.labels(endpoint="trip_quote", status="400").inc()
        raise HTTPException(status_code=400, detail="Origin and destination too close")

    # --- FEATURES ---
    feature_start = time.perf_counter()
    raw_5m = fetch_window(
        redis_client=redis_client,
        city=CITY,
        h3_index=h3_origin,
        window=5,
        ts=ts,
    )
    FEATURE_FETCH_LATENCY.labels(endpoint="trip_quote").observe(time.perf_counter() - feature_start)
    features_5m = derive_features(raw_5m)

    # --- PICKUP ETA ---
    pickup_features = assemble_pickup_features(
        trip_distance_km=trip_distance_km,
        features_5m=features_5m,
    )
    model_start = time.perf_counter()
    pickup_eta = pickup_model.predict(pickup_features)
    MODEL_INFERENCE_LATENCY.labels(model="pickup_eta").observe(time.perf_counter() - model_start)

    # --- DROPOFF ETA ---
    surge_pressure = features_5m["surge_pressure"]
    dropoff_eta = adjust_dropoff_eta(
        osrm_duration_s=osrm_duration_s,
        surge_pressure=surge_pressure,
    )
    congestion_factor = compute_congestion_factor(surge_pressure)

    # --- TOTAL ETA ---
    total_eta = pickup_eta + dropoff_eta

    # --- PRICING ---
    pricing = compute_price_multiplier(features_5m)
    base_fare = 1.5
    price_per_km = 1.2
    price = (base_fare + trip_distance_km * price_per_km) * pricing["multiplier"]

    # --- MONITORING ---
    latency_ms = (time.perf_counter() - start) * 1000
    record_latency(redis_client, "trip_quote", latency_ms)
    REQUEST_LATENCY.labels(endpoint="trip_quote").observe(latency_ms / 1000)
    REQUEST_COUNTER.labels(endpoint="trip_quote", status="200").inc()

    return {
        "city": CITY,
        "h3_origin": h3_origin,
        "route": {
            "source": routing_source,
            "distance_km": round(trip_distance_km, 2),
            "osrm_duration_min": round(osrm_duration_s / 60, 1),
            "geometry": route_geometry,
        },
        "eta": {
            "pickup_seconds": int(pickup_eta),
            "dropoff_seconds": int(dropoff_eta),
            "dropoff_free_flow_seconds": int(osrm_duration_s),
            "congestion_factor": round(congestion_factor, 2),
            "total_seconds": int(total_eta),
            "total_minutes": round(total_eta / 60, 1),
        },
        "price": {
            "amount_eur": round(price, 2),
            "multiplier": pricing["multiplier"],
            "surge_level": pricing["surge_level"],
            "reasons": pricing["reasons"],
        },
        "latency_ms": round(latency_ms, 2),
    }
