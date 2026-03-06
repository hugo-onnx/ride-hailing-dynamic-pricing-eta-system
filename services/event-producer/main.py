import h3
import json
import time
import uuid
import random
import logging
import math

from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from confluent_kafka import Producer
from prometheus_client import (
    Counter, Gauge, Histogram, Summary,
    start_http_server
)

import redis as redis_lib
from services.common.config import KAFKA_BOOTSTRAP_SERVERS, REDIS_HOST, REDIS_PORT, CITY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
RIDE_TOPIC = f"rides.requested.{CITY}"
DRIVER_TOPIC = f"drivers.location.{CITY}"
H3_RESOLUTION = 8
METRICS_PORT = 8001

RIDES_PRODUCED = Counter(
    'event_producer_rides_produced_total',
    'Total number of ride request events produced',
    ['city', 'zone']
)

DRIVER_PINGS_PRODUCED = Counter(
    'event_producer_driver_pings_produced_total',
    'Total number of driver location events produced',
    ['city', 'status']
)

KAFKA_ERRORS = Counter(
    'event_producer_kafka_errors_total',
    'Total number of Kafka delivery errors',
    ['city', 'topic']
)

ACTIVE_DRIVERS = Gauge(
    'event_producer_active_drivers',
    'Current number of simulated drivers',
    ['city', 'status']
)

BATCH_SIZE_GAUGE = Gauge(
    'event_producer_batch_size',
    'Current batch size configuration',
    ['city']
)

PRODUCE_LATENCY = Histogram(
    'event_producer_produce_latency_seconds',
    'Time taken to produce a batch of events',
    ['city'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

EVENTS_PER_SECOND = Summary(
    'event_producer_events_per_second',
    'Events produced per second',
    ['city', 'type']
)

EVENT_INTERVAL = 2.0
RIDES_PER_BATCH = 2         # peak-hour baseline; scaled down off-peak
NUM_DRIVERS = 600           # registered driver pool for this platform
DRIVER_PING_INTERVAL = 5
DRIVER_DRIFT_STRENGTH = 0.3
DRIVER_MOVE_DELTA = 0.002

# Fraction of the registered pool that is online each hour.
# Single-platform Madrid estimates (600-driver pool):
#   peak evening (18-20h) → ~600 online (100%)
#   morning rush (7-9h)   → ~430 online (72%)
#   midday                → ~380 online (63%)
#   deep night (2-4h)     → ~65  online (11%)
HOURLY_DRIVER_SUPPLY: list[float] = [
    0.18, 0.14, 0.11, 0.10, 0.12, 0.22,   # 0–5   (deep night → very early)
    0.45, 0.68, 0.75, 0.72, 0.62, 0.65,   # 6–11  (morning ramp-up)
    0.68, 0.65, 0.58, 0.62, 0.72, 0.90,   # 12–17 (afternoon → pre-peak)
    1.00, 0.95, 0.85, 0.72, 0.55, 0.35,   # 18–23 (evening peak → ramp-down)
]

# Max share of fleet that can transition online/offline per tick (~2%)
# Prevents jarring supply jumps when hour changes
SUPPLY_CHANGE_RATE = 0.02

# Fraction of rides placed at random urban positions (fills inter-zone gaps)
BACKGROUND_NOISE_FRACTION = 0.15

# Madrid is UTC+1 (CET). Using fixed offset — close enough for simulation.
MADRID_UTC_OFFSET = timedelta(hours=1)

MADRID_BOUNDS = {
    "lat_min": 40.32, "lat_max": 40.58,
    "lng_min": -3.84, "lng_max": -3.53,
}

# ---------------------------------------------------------------------------
# Time-of-day demand multipliers (24 values, one per hour in local Madrid time)
# Each zone type has its own rhythm for a typical weekday.
# ---------------------------------------------------------------------------
HOURLY_DEMAND: dict[str, list[float]] = {
    # Nightlife areas: quiet day, peak 22h–2h
    "nightlife": [
        1.2, 0.8, 0.5, 0.3, 0.2, 0.2,
        0.3, 0.4, 0.5, 0.5, 0.6, 0.7,
        0.9, 1.0, 0.9, 0.8, 0.8, 0.9,
        1.1, 1.2, 1.4, 1.5, 1.6, 1.4,
    ],
    # Business districts: classic 9–19 weekday shape
    "business": [
        0.05, 0.05, 0.05, 0.05, 0.1, 0.3,
        0.7,  1.0,  1.3,  1.1,  0.9, 0.8,
        1.0,  0.9,  0.8,  0.8,  0.9, 1.2,
        1.3,  1.0,  0.6,  0.3,  0.2, 0.1,
    ],
    # Transport hubs: sharp morning + evening commute peaks
    "transport": [
        0.2, 0.1, 0.1, 0.1, 0.3, 0.7,
        1.2, 1.5, 1.2, 0.8, 0.7, 0.8,
        0.9, 0.8, 0.7, 0.8, 1.0, 1.4,
        1.5, 1.2, 0.9, 0.6, 0.4, 0.3,
    ],
    # Airport: early-morning departures, afternoon/evening arrivals
    "airport": [
        0.4, 0.3, 0.2, 0.2, 0.6, 1.1,
        1.2, 1.0, 0.9, 0.8, 0.8, 0.8,
        0.9, 0.9, 1.0, 1.1, 1.2, 1.1,
        1.0, 0.9, 0.8, 0.7, 0.5, 0.4,
    ],
    # Residential: commute bookends, quiet midday + night
    "residential": [
        0.1, 0.05, 0.05, 0.05, 0.15, 0.5,
        1.0,  1.3,  1.1,  0.7,  0.6,  0.7,
        0.9,  0.8,  0.6,  0.6,  0.8,  1.2,
        1.3,  1.1,  0.7,  0.4,  0.3,  0.2,
    ],
    # Leisure / hospitals / parks: steady daytime, quieter at night
    "leisure": [
        0.15, 0.1, 0.1, 0.1, 0.15, 0.3,
        0.4,  0.5, 0.7, 0.9, 1.0,  1.1,
        1.1,  1.1, 1.0, 1.0, 0.9,  0.9,
        1.0,  1.0, 0.9, 0.7, 0.5,  0.3,
    ],
}

# Global demand volume by hour: scales RIDES_PER_BATCH up/down
# Represents total city-wide trip volume across the day
HOURLY_GLOBAL_DEMAND: list[float] = [
    0.30, 0.20, 0.15, 0.10, 0.20, 0.45,   # 0–5   (deep night → very early)
    0.75, 1.00, 1.10, 0.90, 0.80, 0.90,   # 6–11  (morning rush)
    1.00, 0.95, 0.80, 0.80, 0.90, 1.10,   # 12–17 (lunch + afternoon)
    1.20, 1.10, 0.95, 0.85, 0.75, 0.50,   # 18–23 (evening rush → night)
]


@dataclass
class Zone:
    """Represents a demand zone in the city"""
    name: str
    lat: float
    lng: float
    radius: float
    demand_weight: float
    driver_weight: float
    zone_type: str = "residential"


MADRID_ZONES = [
    # City centre — nightlife character
    Zone("Sol-Gran Via",         40.4169, -3.7034, 0.8, 12, 14, "nightlife"),
    Zone("Plaza Mayor",          40.4155, -3.7074, 0.5,  8,  9, "nightlife"),
    Zone("Malasana",             40.4260, -3.7060, 0.5,  8,  9, "nightlife"),
    Zone("Chueca",               40.4225, -3.6970, 0.5,  7,  8, "nightlife"),
    Zone("La Latina",            40.4115, -3.7115, 0.5,  6,  7, "nightlife"),
    Zone("Lavapies",             40.4085, -3.7015, 0.5,  6,  7, "nightlife"),
    Zone("Retiro",               40.4153, -3.6845, 0.6,  6,  6, "leisure"),

    # Transport hubs
    Zone("Atocha",               40.4065, -3.6895, 0.6, 12, 13, "transport"),
    Zone("Chamartin",            40.4722, -3.6824, 0.7, 10, 12, "transport"),
    Zone("Principe Pio",         40.4210, -3.7205, 0.5,  8,  9, "transport"),
    Zone("Mendez Alvaro",        40.3977, -3.6690, 0.6,  7,  8, "transport"),

    # Airport
    Zone("Barajas T1-T3",        40.4719, -3.5674, 0.9, 12, 11, "airport"),
    Zone("Barajas T4",           40.4929, -3.5922, 0.6,  8,  8, "airport"),

    # Business districts
    Zone("Azca",                 40.4505, -3.6925, 0.6,  9, 10, "business"),
    Zone("Cuatro Torres",        40.4748, -3.6875, 0.5,  8,  9, "business"),
    Zone("IFEMA",                40.4653, -3.6024, 0.7,  6,  7, "business"),
    Zone("Salamanca",            40.4280, -3.6820, 0.7,  8,  9, "business"),
    Zone("Goya",                 40.4235, -3.6755, 0.5,  6,  7, "business"),

    # Inner residential / mixed
    Zone("Tetuan",               40.4605, -3.6985, 0.6,  6,  7, "residential"),
    Zone("Moncloa",              40.4350, -3.7195, 0.6,  6,  7, "residential"),
    Zone("Arguelles",            40.4305, -3.7145, 0.5,  5,  6, "residential"),

    # Outer north / north-east residential
    Zone("Hortaleza",            40.4900, -3.6250, 1.1,  7,  8, "residential"),
    Zone("Sanchinarro",          40.5050, -3.6550, 1.0,  6,  7, "residential"),
    Zone("San Blas-Canillejas",  40.4300, -3.6000, 0.9,  5,  6, "residential"),
    Zone("Vicálvaro",            40.4050, -3.6050, 0.9,  5,  5, "residential"),
    Zone("Moratalaz",            40.4050, -3.6400, 0.8,  5,  5, "residential"),

    # West
    Zone("Ciudad Universitaria", 40.4485, -3.7295, 0.6,  5,  5, "leisure"),
    Zone("Pozuelo-Aravaca",      40.4380, -3.7820, 1.1,  5,  6, "residential"),
    Zone("Casa de Campo",        40.4195, -3.7495, 0.9,  3,  3, "leisure"),

    # South residential
    Zone("Vallecas",             40.3785, -3.6515, 1.1,  8,  8, "residential"),
    Zone("Carabanchel",          40.3850, -3.7350, 1.1,  8,  8, "residential"),
    Zone("Usera",                40.3855, -3.7015, 0.7,  6,  6, "residential"),
    Zone("Villaverde",           40.3450, -3.6950, 1.1,  7,  7, "residential"),
    Zone("Getafe Norte",         40.3480, -3.6800, 1.0,  5,  5, "residential"),
    Zone("Leganés Norte",        40.3480, -3.7600, 1.0,  5,  5, "residential"),

    # Hospitals
    Zone("Hospital La Paz",      40.4815, -3.6875, 0.4,  4,  4, "leisure"),
    Zone("Hospital 12 Octubre",  40.3755, -3.6975, 0.4,  4,  4, "leisure"),

    # Stadiums / venues
    Zone("Santiago Bernabeu",    40.4531, -3.6883, 0.4,  5,  6, "leisure"),
    Zone("Wanda Metropolitano",  40.4362, -3.5995, 0.5,  4,  4, "leisure"),
]


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def get_madrid_hour() -> int:
    """Current hour in Madrid local time (UTC+1, approximate)."""
    return (datetime.now(timezone.utc) + MADRID_UTC_OFFSET).hour


def get_zone_multiplier(zone: Zone, hour: int) -> float:
    return HOURLY_DEMAND[zone.zone_type][hour]


def get_time_weighted_demand(hour: int) -> list[float]:
    """Return demand weights for all zones adjusted to the current hour."""
    raw = [z.demand_weight * get_zone_multiplier(z, hour) for z in MADRID_ZONES]
    total = sum(raw)
    return [w / total for w in raw]


def get_time_weighted_driver(hour: int) -> list[float]:
    """Return driver-supply weights for all zones adjusted to the current hour."""
    raw = [z.driver_weight * get_zone_multiplier(z, hour) for z in MADRID_ZONES]
    total = sum(raw)
    return [w / total for w in raw]


# ---------------------------------------------------------------------------
# Spatial helpers
# ---------------------------------------------------------------------------

def sample_urban_background() -> tuple[float, float]:
    """Uniform random point within the urban extent (rejection sampling).
    Rejects points > 6 km from every zone to avoid rural/empty areas.
    Falls back to a random point within bounds after 1000 failed attempts."""
    max_dist = 0.055
    for _ in range(1000):
        lat = random.uniform(MADRID_BOUNDS["lat_min"], MADRID_BOUNDS["lat_max"])
        lng = random.uniform(MADRID_BOUNDS["lng_min"], MADRID_BOUNDS["lng_max"])
        for zone in MADRID_ZONES:
            if math.sqrt((lat - zone.lat) ** 2 + (lng - zone.lng) ** 2) < max_dist:
                return lat, lng
    return (
        random.uniform(MADRID_BOUNDS["lat_min"], MADRID_BOUNDS["lat_max"]),
        random.uniform(MADRID_BOUNDS["lng_min"], MADRID_BOUNDS["lng_max"]),
    )


def normalize_weights(zones: list[Zone], attr: str) -> list[float]:
    weights = [getattr(z, attr) for z in zones]
    total = sum(weights)
    return [w / total for w in weights]


def sample_point_in_zone(zone: Zone) -> tuple[float, float]:
    sigma_lat = zone.radius / 111.0 / 2
    sigma_lng = zone.radius / (111.0 * math.cos(math.radians(zone.lat))) / 2
    lat = random.gauss(zone.lat, sigma_lat)
    lng = random.gauss(zone.lng, sigma_lng)
    return lat, lng


def generate_ride_location(hour: int) -> tuple[float, float, str]:
    if random.random() < BACKGROUND_NOISE_FRACTION:
        lat, lng = sample_urban_background()
        return lat, lng, "background"
    weights = get_time_weighted_demand(hour)
    zone = random.choices(MADRID_ZONES, weights=weights)[0]
    lat, lng = sample_point_in_zone(zone)
    return lat, lng, zone.name


# ---------------------------------------------------------------------------
# Driver simulator
# ---------------------------------------------------------------------------

class DriverSimulator:
    def __init__(self, num_drivers: int, ping_interval: float, event_interval: float):
        self.drivers = {}
        self.ping_interval = ping_interval
        self.event_interval = event_interval
        self.tick_count = 0
        self.pings_per_interval = max(1, int(ping_interval / event_interval))
        self._init_drivers(num_drivers)

    def _init_drivers(self, num_drivers: int):
        hour = get_madrid_hour()
        driver_weights = get_time_weighted_driver(hour)
        initial_online_fraction = HOURLY_DRIVER_SUPPLY[hour]

        for i in range(num_drivers):
            driver_id = f"d_{i:05d}"
            online = random.random() < initial_online_fraction

            if random.random() < 0.10:
                lat, lng = sample_urban_background()
            else:
                zone = random.choices(MADRID_ZONES, weights=driver_weights)[0]
                lat, lng = sample_point_in_zone(zone)

            self.drivers[driver_id] = {
                "lat": lat,
                "lng": lng,
                "status": "available",
                "online": online,
                "ticks_in_status": 0,
                "ping_offset": i % self.pings_per_interval,
            }

        online_count = sum(1 for d in self.drivers.values() if d["online"])
        logger.info(
            f"Initialized {num_drivers} drivers — "
            f"{online_count} online at hour {hour:02d}h "
            f"({initial_online_fraction*100:.0f}% supply)"
        )

    def _get_nearest_demand_zone(self, lat: float, lng: float, hour: int) -> Zone:
        """Weighted random selection — dist² penalty keeps drivers local while
        allowing cross-city movement proportional to time-adjusted demand."""
        demand_weights = get_time_weighted_demand(hour)
        scores = []
        for zone, weight in zip(MADRID_ZONES, demand_weights):
            dist = math.sqrt((lat - zone.lat) ** 2 + (lng - zone.lng) ** 2)
            scores.append(weight / max(dist ** 2, 0.0001))
        return random.choices(MADRID_ZONES, weights=scores)[0]

    def _move_driver(self, driver_id: str, state: dict, hour: int) -> tuple[float, float]:
        lat, lng = state["lat"], state["lng"]

        if state["status"] == "available":
            target_zone = self._get_nearest_demand_zone(lat, lng, hour)
            dlat = target_zone.lat - lat
            dlng = target_zone.lng - lng
            dist = math.sqrt(dlat ** 2 + dlng ** 2)

            if dist > 0.001:
                dlat /= dist
                dlng /= dist
                random_lat = random.uniform(-1, 1)
                random_lng = random.uniform(-1, 1)
                move_lat = DRIVER_DRIFT_STRENGTH * dlat + (1 - DRIVER_DRIFT_STRENGTH) * random_lat
                move_lng = DRIVER_DRIFT_STRENGTH * dlng + (1 - DRIVER_DRIFT_STRENGTH) * random_lng
                move_dist = math.sqrt(move_lat ** 2 + move_lng ** 2)
                if move_dist > 0:
                    lat += (move_lat / move_dist) * DRIVER_MOVE_DELTA
                    lng += (move_lng / move_dist) * DRIVER_MOVE_DELTA
        else:
            lat += random.uniform(-DRIVER_MOVE_DELTA * 2, DRIVER_MOVE_DELTA * 2)
            lng += random.uniform(-DRIVER_MOVE_DELTA * 2, DRIVER_MOVE_DELTA * 2)

        return lat, lng

    def _update_status(self, state: dict, global_demand: float) -> str:
        """Trip acceptance scales with city-wide demand level — more trips
        complete faster during peak hours, drivers go idle more at night."""
        state["ticks_in_status"] += 1

        if state["status"] == "available":
            # Higher demand → higher pickup probability
            base_chance = 0.02 + state["ticks_in_status"] * 0.005
            trip_chance = min(base_chance * global_demand, 0.20)
            if random.random() < trip_chance:
                state["status"] = "on_trip"
                state["ticks_in_status"] = 0
        else:
            # Trips complete at the same rate regardless of demand
            complete_chance = state["ticks_in_status"] / 50.0
            if random.random() < complete_chance:
                state["status"] = "available"
                state["ticks_in_status"] = 0

        return state["status"]

    def _rebalance_supply(self, hour: int):
        """Smoothly bring driver supply toward the hourly target.
        Only idles drivers go offline; only offline drivers come online."""
        target = round(len(self.drivers) * HOURLY_DRIVER_SUPPLY[hour])
        current_online = sum(1 for d in self.drivers.values() if d["online"])
        max_change = max(1, round(len(self.drivers) * SUPPLY_CHANGE_RATE))
        delta = target - current_online

        if delta > 0:
            offline = [did for did, d in self.drivers.items() if not d["online"]]
            for did in random.sample(offline, min(max_change, delta, len(offline))):
                self.drivers[did]["online"] = True
                self.drivers[did]["status"] = "available"
                self.drivers[did]["ticks_in_status"] = 0
        elif delta < 0:
            idle_online = [
                did for did, d in self.drivers.items()
                if d["online"] and d["status"] == "available"
            ]
            for did in random.sample(idle_online, min(max_change, -delta, len(idle_online))):
                self.drivers[did]["online"] = False

    def generate_events(self, hour: int, global_demand: float) -> list[dict]:
        self._rebalance_supply(hour)

        events = []
        current_offset = self.tick_count % self.pings_per_interval

        for driver_id, state in self.drivers.items():
            if not state["online"]:
                continue
            if state["ping_offset"] != current_offset:
                continue

            lat, lng = self._move_driver(driver_id, state, hour)
            state["lat"], state["lng"] = lat, lng
            status = self._update_status(state, global_demand)

            idle_seconds = (
                state["ticks_in_status"] * self.ping_interval
                if status == "available" else 0
            )

            events.append({
                "driver_id": driver_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "lat": lat,
                "lng": lng,
                "h3_res8": h3.latlng_to_cell(lat, lng, H3_RESOLUTION),
                "status": status,
                "idle_seconds": int(idle_seconds),
            })

        self.tick_count += 1
        return events

    def get_stats(self) -> dict:
        online = [d for d in self.drivers.values() if d["online"]]
        available = sum(1 for d in online if d["status"] == "available")
        on_trip = len(online) - available
        return {
            "total": len(self.drivers),
            "online": len(online),
            "available": available,
            "on_trip": on_trip,
            "availability_rate": available / max(len(online), 1) * 100,
        }


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def get_producer():
    while True:
        try:
            p = Producer(KAFKA_CONFIG)
            p.poll(0)
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return p
        except Exception as e:
            logger.warning(f"Waiting for Kafka... {e}")
            time.sleep(3)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
        KAFKA_ERRORS.labels(city=CITY, topic=msg.topic()).inc()
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


def generate_ride_event(hour: int) -> dict:
    lat, lng, zone = generate_ride_location(hour)
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lat": lat,
        "lng": lng,
        "h3_res8": h3.latlng_to_cell(lat, lng, H3_RESOLUTION),
        "zone": zone,
    }


def get_redis_client():
    """Connect to Redis. Returns client on success, None if unavailable."""
    try:
        client = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        client.ping()
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return client
    except Exception as e:
        logger.warning(f"Redis unavailable, live driver stats will not be written: {e}")
        return None


def update_driver_metrics(driver_sim: DriverSimulator, redis_client=None):
    stats = driver_sim.get_stats()
    ACTIVE_DRIVERS.labels(city=CITY, status="available").set(stats["available"])
    ACTIVE_DRIVERS.labels(city=CITY, status="on_trip").set(stats["on_trip"])
    if redis_client:
        try:
            redis_client.hset(
                f"{CITY}:drivers:live",
                mapping={"idle": stats["available"], "on_trip": stats["on_trip"]},
            )
            redis_client.expire(f"{CITY}:drivers:live", 30)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    start_http_server(METRICS_PORT)
    logger.info(f"Prometheus metrics server started on port {METRICS_PORT}")

    producer = get_producer()
    redis_client = get_redis_client()
    driver_sim = DriverSimulator(NUM_DRIVERS, DRIVER_PING_INTERVAL, EVENT_INTERVAL)

    BATCH_SIZE_GAUGE.labels(city=CITY).set(RIDES_PER_BATCH)

    logger.info("=" * 60)
    logger.info("Event Producer — time-aware Madrid simulation")
    logger.info("=" * 60)
    logger.info(f"  Metrics: http://localhost:{METRICS_PORT}/metrics")
    logger.info(f"  Rides topic:  {RIDE_TOPIC}")
    logger.info(f"  Driver topic: {DRIVER_TOPIC}")
    logger.info(f"  Drivers: {NUM_DRIVERS} | Base rides/batch: {RIDES_PER_BATCH}")
    logger.info("=" * 60)

    batch_count = 0
    total_rides = 0
    total_driver_pings = 0
    start_time = time.time()
    last_log_time = start_time
    last_metric_time = start_time

    try:
        while True:
            batch_start = time.time()

            hour = get_madrid_hour()
            global_demand = HOURLY_GLOBAL_DEMAND[hour]

            # Scale ride volume by time-of-day global demand
            rides_this_batch = max(1, round(RIDES_PER_BATCH * global_demand))

            for _ in range(rides_this_batch):
                ride_event = generate_ride_event(hour)
                producer.produce(
                    topic=RIDE_TOPIC,
                    value=json.dumps(ride_event).encode("utf-8"),
                    callback=delivery_report,
                )
                total_rides += 1
                RIDES_PRODUCED.labels(city=CITY, zone=ride_event["zone"]).inc()

            driver_events = driver_sim.generate_events(hour, global_demand)
            for event in driver_events:
                producer.produce(
                    topic=DRIVER_TOPIC,
                    key=event["driver_id"].encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                total_driver_pings += 1
                DRIVER_PINGS_PRODUCED.labels(city=CITY, status=event["status"]).inc()

            producer.poll(0)
            batch_count += 1

            batch_duration = time.time() - batch_start
            PRODUCE_LATENCY.labels(city=CITY).observe(batch_duration)

            if time.time() - last_metric_time >= 1.0:
                update_driver_metrics(driver_sim, redis_client)
                last_metric_time = time.time()

            if time.time() - last_log_time >= 10:
                elapsed = time.time() - start_time
                stats = driver_sim.get_stats()

                EVENTS_PER_SECOND.labels(city=CITY, type="rides").observe(total_rides / elapsed)
                EVENTS_PER_SECOND.labels(city=CITY, type="driver_pings").observe(total_driver_pings / elapsed)

                logger.info(
                    f"[{elapsed:.0f}s] hour={hour:02d}h demand={global_demand:.2f} | "
                    f"rides/batch={rides_this_batch} | "
                    f"Rides: {total_rides} ({total_rides/elapsed:.1f}/s) | "
                    f"Pings: {total_driver_pings} ({total_driver_pings/elapsed:.1f}/s) | "
                    f"Drivers: {stats['online']} online "
                    f"({stats['available']} idle / {stats['on_trip']} on trip)"
                )
                last_log_time = time.time()

            sleep_time = max(0, EVENT_INTERVAL - batch_duration)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        producer.flush()
        logger.info("Event producer stopped")


if __name__ == "__main__":
    main()
