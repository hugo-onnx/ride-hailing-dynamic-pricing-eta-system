from pydantic import BaseModel, Field
from enum import Enum


# ── Common ────────────────────────────────────────────────────────────────────

class LocationQuery(BaseModel):
    lat: float = Field(..., description="Latitude in decimal degrees", examples=[40.4168])
    lng: float = Field(..., description="Longitude in decimal degrees", examples=[-3.7038])
    timestamp: str | None = Field(
        None,
        description="ISO 8601 timestamp. Defaults to current UTC time if omitted.",
        examples=["2025-06-15T14:30:00+00:00"],
    )


class TripQuery(BaseModel):
    origin_lat: float = Field(..., description="Pickup latitude", examples=[40.4168])
    origin_lng: float = Field(..., description="Pickup longitude", examples=[-3.7038])
    dest_lat: float = Field(..., description="Destination latitude", examples=[40.4530])
    dest_lng: float = Field(..., description="Destination longitude", examples=[-3.6883])
    timestamp: str | None = Field(
        None,
        description="ISO 8601 timestamp. Defaults to current UTC time if omitted.",
        examples=["2025-06-15T14:30:00+00:00"],
    )


class RouteQuery(BaseModel):
    origin_lat: float = Field(..., description="Origin latitude", examples=[40.4168])
    origin_lng: float = Field(..., description="Origin longitude", examples=[-3.7038])
    dest_lat: float = Field(..., description="Destination latitude", examples=[40.4530])
    dest_lng: float = Field(..., description="Destination longitude", examples=[-3.6883])


class ETAQuery(BaseModel):
    lat: float = Field(..., description="Pickup latitude", examples=[40.4168])
    lng: float = Field(..., description="Pickup longitude", examples=[-3.7038])
    trip_distance_km: float = Field(
        ..., gt=0, description="Estimated trip distance in kilometers", examples=[5.2],
    )
    timestamp: str | None = Field(
        None,
        description="ISO 8601 timestamp. Defaults to current UTC time if omitted.",
        examples=["2025-06-15T14:30:00+00:00"],
    )


# ── Health ────────────────────────────────────────────────────────────────────

class ConnectionStatus(str, Enum):
    connected = "connected"
    unavailable = "unavailable"


class HealthResponse(BaseModel):
    status: str = Field(..., examples=["ok"])
    redis: ConnectionStatus
    osrm: ConnectionStatus

    model_config = {"json_schema_extra": {
        "examples": [{"status": "ok", "redis": "connected", "osrm": "connected"}],
    }}


# ── Features ──────────────────────────────────────────────────────────────────

class DerivedFeatures(BaseModel):
    ride_requests: int = Field(..., description="Number of ride requests in window")
    idle_drivers: int = Field(..., description="Number of idle drivers in window")
    on_trip_drivers: int = Field(..., description="Number of drivers on active trips")
    deadhead_km_avg: float = Field(..., description="Average idle driving distance (km)")
    supply_demand_ratio: float = Field(..., description="Idle drivers / ride requests")
    surge_pressure: float = Field(..., ge=0, le=1, description="Supply shortage score (0=surplus, 1=severe shortage)")


class FeaturesResponse(BaseModel):
    h3_res8: str = Field(..., description="H3 hexagonal cell index at resolution 8")
    timestamp: str = Field(..., description="ISO 8601 timestamp used for the query")
    features: dict[str, DerivedFeatures] = Field(
        ..., description="Derived features keyed by window duration (1m, 5m, 15m)",
    )


class FeaturesDebugResponse(FeaturesResponse):
    raw: dict[str, dict] = Field(
        ..., description="Raw Redis hash data for each window (debug only)",
    )


# ── Pricing ───────────────────────────────────────────────────────────────────

class SurgeLevel(str, Enum):
    normal = "normal"
    moderate = "moderate"
    high = "high"


class PricingBreakdown(BaseModel):
    multiplier: float = Field(..., ge=1.0, le=2.0, description="Dynamic price multiplier (1.0x–2.0x)")
    surge_level: SurgeLevel = Field(..., description="Human-readable surge category")
    reasons: list[str] = Field(..., description="Factors contributing to the price multiplier")


class PricingQuoteResponse(BaseModel):
    city: str
    h3_res8: str = Field(..., description="H3 cell at pickup location")
    timestamp: str
    features: DerivedFeatures
    pricing: PricingBreakdown
    latency_ms: float = Field(..., description="Server-side processing time in milliseconds")


# ── ETA ───────────────────────────────────────────────────────────────────────

class ETAQuoteResponse(BaseModel):
    city: str
    h3_res8: str = Field(..., description="H3 cell at pickup location")
    trip_distance_km: float
    eta_seconds: int = Field(..., description="Predicted pickup ETA in seconds")
    eta_minutes: float = Field(..., description="Predicted pickup ETA in minutes")
    latency_ms: float


# ── Routing ───────────────────────────────────────────────────────────────────

class RoutingSource(str, Enum):
    osrm = "osrm"
    haversine_fallback = "haversine_fallback"


class RouteResponse(BaseModel):
    source: RoutingSource
    distance_km: float = Field(..., description="Route distance in kilometers")
    duration_min: float = Field(..., description="Estimated travel time in minutes")
    duration_s: float = Field(..., description="Estimated travel time in seconds")
    geometry: dict | None = Field(None, description="GeoJSON LineString route geometry")
    warning: str | None = Field(None, description="Present when OSRM fallback was used")


# ── Trip Quote ────────────────────────────────────────────────────────────────

class RouteInfo(BaseModel):
    source: RoutingSource
    distance_km: float
    osrm_duration_min: float = Field(..., description="Free-flow duration from OSRM (or estimate)")
    geometry: dict | None = Field(None, description="GeoJSON LineString route geometry")


class ETABreakdown(BaseModel):
    pickup_seconds: int = Field(..., description="ML-predicted time until driver arrives")
    dropoff_seconds: int = Field(..., description="Congestion-adjusted trip duration")
    dropoff_free_flow_seconds: int = Field(..., description="OSRM free-flow trip duration")
    congestion_factor: float = Field(..., description="Traffic congestion multiplier applied")
    total_seconds: int = Field(..., description="Total time: pickup + dropoff")
    total_minutes: float


class PriceBreakdown(BaseModel):
    amount_eur: float = Field(..., description="Final price in EUR")
    multiplier: float = Field(..., description="Surge multiplier applied")
    surge_level: SurgeLevel
    reasons: list[str]


class TripQuoteResponse(BaseModel):
    city: str
    h3_origin: str = Field(..., description="H3 cell at pickup location")
    route: RouteInfo
    eta: ETABreakdown
    price: PriceBreakdown
    latency_ms: float


# ── Monitoring ────────────────────────────────────────────────────────────────

class PercentileSummary(BaseModel):
    p50: float = Field(..., description="Median value")
    p95: float = Field(..., description="95th percentile value")


class DriftFeaturesDetail(BaseModel):
    supply_demand_ratio: PercentileSummary
    deadhead_km_avg: PercentileSummary
    surge_pressure: PercentileSummary


class DriftResponse(BaseModel):
    city: str | None = None
    status: str | None = Field(None, description="Set to 'insufficient_data' when < 50 samples")
    samples: int = Field(..., description="Number of feature snapshots analyzed")
    features: DriftFeaturesDetail | None = None
