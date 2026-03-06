from fastapi import APIRouter, Depends, HTTPException

from dependencies import get_redis, get_osrm_available
from schemas import HealthResponse, ConnectionStatus

router = APIRouter(tags=["System"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Service health check",
    description="Returns the connection status of Redis and OSRM dependencies.",
)
def health(
    redis_client=Depends(get_redis),
    osrm_available: bool = Depends(get_osrm_available),
):
    try:
        redis_client.ping()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unhealthy: {e}")

    return HealthResponse(
        status="ok",
        redis=ConnectionStatus.connected,
        osrm=ConnectionStatus.connected if osrm_available else ConnectionStatus.unavailable,
    )
