from fastapi import APIRouter, Query

from routing.osrm import get_osrm_client, OSRMError
from geo.utils import haversine_km
from schemas import RouteResponse

HAVERSINE_FALLBACK_SPEED_KMH = 35

router = APIRouter(prefix="/v1", tags=["Routing"])


@router.get(
    "/route",
    response_model=RouteResponse,
    summary="Get route between two points",
    description=(
        "Calculates road-network distance and free-flow travel time using OSRM. "
        "Falls back to haversine (great-circle) distance with a 35 km/h speed "
        "assumption when OSRM is unavailable.\n\n"
        "**Note:** Coordinates use `(lat, lng)` order."
    ),
)
def get_route(
    origin_lat: float = Query(..., description="Origin latitude", examples=[40.4168]),
    origin_lng: float = Query(..., description="Origin longitude", examples=[-3.7038]),
    dest_lat: float = Query(..., description="Destination latitude", examples=[40.4530]),
    dest_lng: float = Query(..., description="Destination longitude", examples=[-3.6883]),
):
    osrm_client = get_osrm_client()

    try:
        route = osrm_client.get_route(
            origin=(origin_lng, origin_lat),
            destination=(dest_lng, dest_lat),
        )
        return {
            "source": "osrm",
            "distance_km": round(route.distance_km, 2),
            "duration_min": round(route.duration_min, 1),
            "duration_s": round(route.duration_s, 0),
        }
    except OSRMError as e:
        distance_km = haversine_km(origin_lat, origin_lng, dest_lat, dest_lng)
        duration_min = (distance_km / HAVERSINE_FALLBACK_SPEED_KMH) * 60

        return {
            "source": "haversine_fallback",
            "distance_km": round(distance_km, 2),
            "duration_min": round(duration_min, 1),
            "duration_s": round(duration_min * 60, 0),
            "warning": f"OSRM unavailable: {e}",
        }
