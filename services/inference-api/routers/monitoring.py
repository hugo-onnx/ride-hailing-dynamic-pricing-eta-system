import json

import numpy as np
from fastapi import APIRouter, Depends

from services.common.config import CITY
from dependencies import get_redis
from schemas import DriftResponse

router = APIRouter(prefix="/v1/monitoring", tags=["Monitoring"])


@router.get(
    "/drift",
    response_model=DriftResponse,
    summary="Feature drift summary",
    description=(
        "Analyzes stored feature snapshots to compute percentile statistics "
        "(p50, p95) for key marketplace features. Useful for detecting "
        "distribution drift over time.\n\n"
        "Requires at least **50 samples** to produce meaningful statistics."
    ),
    responses={200: {"description": "Drift statistics or insufficient data notice"}},
)
def drift_summary(redis_client=Depends(get_redis)):
    key = f"drift:{CITY}"
    data = redis_client.lrange(key, 0, -1)

    if len(data) < 50:
        return {"status": "insufficient_data", "samples": len(data)}

    parsed = [json.loads(x)["features"] for x in data]

    def summarize(field):
        values = [f[field] for f in parsed if field in f]
        if not values:
            return {"p50": 0.0, "p95": 0.0}
        return {
            "p50": round(float(np.percentile(values, 50)), 3),
            "p95": round(float(np.percentile(values, 95)), 3),
        }

    return {
        "city": CITY,
        "samples": len(parsed),
        "features": {
            "supply_demand_ratio": summarize("supply_demand_ratio"),
            "deadhead_km_avg": summarize("deadhead_km_avg"),
            "surge_pressure": summarize("surge_pressure"),
        },
    }
