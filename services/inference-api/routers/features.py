from fastapi import APIRouter, Depends, Query

from services.common.config import CITY
from dependencies import get_redis, parse_timestamp, resolve_h3
from features import fetch_window, WINDOWS
from derive import derive_features
from schemas import FeaturesResponse, FeaturesDebugResponse

router = APIRouter(prefix="/v1/features", tags=["Features"])


@router.get(
    "",
    response_model=FeaturesResponse,
    summary="Get marketplace features for a location",
    description=(
        "Returns derived marketplace features (supply/demand ratio, surge pressure, etc.) "
        "aggregated over 1-minute, 5-minute, and 15-minute tumbling windows. "
        "Features are sourced from Redis and reflect the most recent **completed** window."
    ),
)
def get_features(
    lat: float = Query(..., description="Latitude in decimal degrees", examples=[40.4168]),
    lng: float = Query(..., description="Longitude in decimal degrees", examples=[-3.7038]),
    timestamp: str | None = Query(None, description="ISO 8601 timestamp (defaults to now)"),
    redis_client=Depends(get_redis),
):
    ts = parse_timestamp(timestamp)
    h3_index = resolve_h3(lat, lng)

    feature_vector = {}
    for window in WINDOWS:
        raw = fetch_window(
            redis_client=redis_client,
            city=CITY,
            h3_index=h3_index,
            window=window,
            ts=ts,
        )
        feature_vector[f"{window}m"] = derive_features(raw)

    return {
        "h3_res8": h3_index,
        "timestamp": ts.isoformat(),
        "features": feature_vector,
    }


@router.get(
    "/debug",
    response_model=FeaturesDebugResponse,
    summary="Debug: inspect raw and derived features",
    description=(
        "Same as the features endpoint but also includes the raw Redis hash data "
        "for each window. Intended for development and debugging."
    ),
)
def get_features_debug(
    lat: float = Query(..., description="Latitude in decimal degrees", examples=[40.4168]),
    lng: float = Query(..., description="Longitude in decimal degrees", examples=[-3.7038]),
    timestamp: str | None = Query(None, description="ISO 8601 timestamp (defaults to now)"),
    redis_client=Depends(get_redis),
):
    ts = parse_timestamp(timestamp)
    h3_index = resolve_h3(lat, lng)

    feature_vector = {}
    raw_snapshots = {}

    for window in WINDOWS:
        raw = fetch_window(
            redis_client=redis_client,
            city=CITY,
            h3_index=h3_index,
            window=window,
            ts=ts,
        )
        feature_vector[f"{window}m"] = derive_features(raw)
        raw_snapshots[f"{window}m"] = raw

    return {
        "h3_res8": h3_index,
        "timestamp": ts.isoformat(),
        "features": feature_vector,
        "raw": raw_snapshots,
    }
