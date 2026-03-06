"""
Ride lifecycle simulator with state machine and driver movement interpolation.
"""

import asyncio
import math
import random
import time
import uuid
import logging
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

DRIVER_NAMES = [
    "Carlos M.", "Ana G.", "Miguel R.", "Laura S.", "Javier P.",
    "Maria L.", "Pablo F.", "Elena D.", "Diego T.", "Sofia N.",
]

CAR_MODELS = [
    "Toyota Corolla", "Seat Leon", "Hyundai Tucson", "Volkswagen Golf",
    "Renault Megane", "Ford Focus", "Peugeot 308", "Kia Sportage",
]


class RideState(str, Enum):
    REQUESTING = "REQUESTING"
    DRIVER_ASSIGNED = "DRIVER_ASSIGNED"
    DRIVER_APPROACHING = "DRIVER_APPROACHING"
    DRIVER_ARRIVED = "DRIVER_ARRIVED"
    ON_TRIP = "ON_TRIP"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


@dataclass
class Driver:
    name: str
    car_model: str
    plate: str
    rating: float
    lat: float = 0.0
    lng: float = 0.0


@dataclass
class RideSession:
    ride_id: str
    state: RideState
    pickup_lat: float
    pickup_lng: float
    dropoff_lat: float
    dropoff_lng: float
    trip_geometry: dict | None = None
    quote: dict | None = None
    driver: Driver | None = None
    approach_coords: list = field(default_factory=list)
    trip_coords: list = field(default_factory=list)
    coord_index: int = 0
    created_at: float = field(default_factory=time.time)
    completed_at: float | None = None
    bearing: float = 0.0


def _random_plate() -> str:
    digits = random.randint(1000, 9999)
    letters = "".join(random.choices("BCDFGHJKLMNPRSTVWXYZ", k=3))
    return f"{digits} {letters}"


def _interpolate_coords(coords: list[list[float]], num_points: int) -> list[list[float]]:
    """Interpolate along a polyline to get evenly-spaced points."""
    if len(coords) < 2 or num_points < 2:
        return coords

    # Compute cumulative distances
    dists = [0.0]
    for i in range(1, len(coords)):
        dx = coords[i][0] - coords[i - 1][0]
        dy = coords[i][1] - coords[i - 1][1]
        dists.append(dists[-1] + math.sqrt(dx * dx + dy * dy))

    total = dists[-1]
    if total == 0:
        return [coords[0]] * num_points

    result = []
    seg = 0
    for i in range(num_points):
        target = (i / (num_points - 1)) * total
        while seg < len(dists) - 2 and dists[seg + 1] < target:
            seg += 1
        seg_len = dists[seg + 1] - dists[seg]
        t = (target - dists[seg]) / seg_len if seg_len > 0 else 0
        lng = coords[seg][0] + t * (coords[seg + 1][0] - coords[seg][0])
        lat = coords[seg][1] + t * (coords[seg + 1][1] - coords[seg][1])
        result.append([lng, lat])

    return result


def _bearing(lng1: float, lat1: float, lng2: float, lat2: float) -> float:
    dlon = math.radians(lng2 - lng1)
    lat1r = math.radians(lat1)
    lat2r = math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2r)
    y = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


class RideSimulator:
    def __init__(self, osrm_client):
        self.rides: dict[str, RideSession] = {}
        self.osrm = osrm_client
        self._tasks: dict[str, asyncio.Task] = {}

    def create_ride(
        self,
        pickup_lat: float,
        pickup_lng: float,
        dropoff_lat: float,
        dropoff_lng: float,
        quote: dict,
    ) -> RideSession:
        ride_id = str(uuid.uuid4())[:8]
        trip_geometry = None
        if quote and quote.get("route", {}).get("geometry"):
            trip_geometry = quote["route"]["geometry"]

        session = RideSession(
            ride_id=ride_id,
            state=RideState.REQUESTING,
            pickup_lat=pickup_lat,
            pickup_lng=pickup_lng,
            dropoff_lat=dropoff_lat,
            dropoff_lng=dropoff_lng,
            trip_geometry=trip_geometry,
            quote=quote,
        )
        self.rides[ride_id] = session

        task = asyncio.create_task(self._run_lifecycle(session))
        self._tasks[ride_id] = task
        return session

    def cancel_ride(self, ride_id: str) -> bool:
        session = self.rides.get(ride_id)
        if not session or session.state in (RideState.COMPLETED, RideState.CANCELLED):
            return False
        session.state = RideState.CANCELLED
        task = self._tasks.get(ride_id)
        if task:
            task.cancel()
        return True

    def get_ride(self, ride_id: str) -> RideSession | None:
        return self.rides.get(ride_id)

    async def _run_lifecycle(self, session: RideSession):
        try:
            # REQUESTING → DRIVER_ASSIGNED (2-4s delay)
            await asyncio.sleep(random.uniform(2.0, 4.0))
            if session.state == RideState.CANCELLED:
                return

            driver = Driver(
                name=random.choice(DRIVER_NAMES),
                car_model=random.choice(CAR_MODELS),
                plate=_random_plate(),
                rating=round(random.uniform(4.5, 5.0), 1),
            )
            session.driver = driver
            session.state = RideState.DRIVER_ASSIGNED
            await asyncio.sleep(1.0)

            if session.state == RideState.CANCELLED:
                return

            # Spawn driver 1-2km away from pickup
            angle = random.uniform(0, 2 * math.pi)
            offset_km = random.uniform(1.0, 2.0)
            dlat = (offset_km / 111.32) * math.cos(angle)
            dlng = (offset_km / (111.32 * math.cos(math.radians(session.pickup_lat)))) * math.sin(angle)
            driver.lat = session.pickup_lat + dlat
            driver.lng = session.pickup_lng + dlng

            # Get approach route
            approach = await self.osrm.get_route(
                driver.lng, driver.lat,
                session.pickup_lng, session.pickup_lat,
            )
            approach_duration = approach["duration_s"]
            approach_geom = approach.get("geometry", {})
            approach_raw = approach_geom.get("coordinates", [
                [driver.lng, driver.lat],
                [session.pickup_lng, session.pickup_lat],
            ])

            # Interpolate approach at 500ms ticks
            num_ticks = max(int(approach_duration / 0.5), 10)
            # Cap approach animation to reasonable length
            num_ticks = min(num_ticks, 60)
            session.approach_coords = _interpolate_coords(approach_raw, num_ticks)
            session.coord_index = 0

            # DRIVER_APPROACHING
            session.state = RideState.DRIVER_APPROACHING
            for i, coord in enumerate(session.approach_coords):
                if session.state == RideState.CANCELLED:
                    return
                session.coord_index = i
                driver.lng = coord[0]
                driver.lat = coord[1]
                if i + 1 < len(session.approach_coords):
                    nxt = session.approach_coords[i + 1]
                    session.bearing = _bearing(coord[0], coord[1], nxt[0], nxt[1])
                await asyncio.sleep(0.5)

            # DRIVER_ARRIVED
            driver.lat = session.pickup_lat
            driver.lng = session.pickup_lng
            session.state = RideState.DRIVER_ARRIVED
            await asyncio.sleep(3.0)

            if session.state == RideState.CANCELLED:
                return

            # ON_TRIP — interpolate along trip route
            trip_raw = []
            if session.trip_geometry and session.trip_geometry.get("coordinates"):
                trip_raw = session.trip_geometry["coordinates"]
            else:
                trip_raw = [
                    [session.pickup_lng, session.pickup_lat],
                    [session.dropoff_lng, session.dropoff_lat],
                ]

            trip_duration = 30.0  # default
            if session.quote and session.quote.get("eta", {}).get("dropoff_seconds"):
                trip_duration = session.quote["eta"]["dropoff_seconds"]

            num_trip_ticks = max(int(trip_duration / 0.5), 20)
            # Cap trip animation
            num_trip_ticks = min(num_trip_ticks, 120)
            session.trip_coords = _interpolate_coords(trip_raw, num_trip_ticks)
            session.coord_index = 0

            session.state = RideState.ON_TRIP
            for i, coord in enumerate(session.trip_coords):
                if session.state == RideState.CANCELLED:
                    return
                session.coord_index = i
                driver.lng = coord[0]
                driver.lat = coord[1]
                if i + 1 < len(session.trip_coords):
                    nxt = session.trip_coords[i + 1]
                    session.bearing = _bearing(coord[0], coord[1], nxt[0], nxt[1])
                await asyncio.sleep(0.5)

            # COMPLETED
            driver.lat = session.dropoff_lat
            driver.lng = session.dropoff_lng
            session.state = RideState.COMPLETED
            session.completed_at = time.time()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Ride lifecycle error for {session.ride_id}: {e}")

    def get_ride_update(self, ride_id: str) -> dict | None:
        session = self.rides.get(ride_id)
        if not session:
            return None

        data = {
            "type": "ride_update",
            "ride_id": session.ride_id,
            "state": session.state.value,
        }

        if session.driver:
            data["driver"] = {
                "name": session.driver.name,
                "car_model": session.driver.car_model,
                "plate": session.driver.plate,
                "rating": session.driver.rating,
                "lat": session.driver.lat,
                "lng": session.driver.lng,
            }
            data["bearing"] = session.bearing

        if session.state == RideState.DRIVER_APPROACHING:
            total = len(session.approach_coords)
            idx = session.coord_index
            data["progress"] = idx / max(total - 1, 1)
            remaining_ticks = total - idx
            data["eta_seconds"] = int(remaining_ticks * 0.5)

        elif session.state == RideState.ON_TRIP:
            total = len(session.trip_coords)
            idx = session.coord_index
            data["progress"] = idx / max(total - 1, 1)
            remaining_ticks = total - idx
            data["eta_seconds"] = int(remaining_ticks * 0.5)

        elif session.state == RideState.COMPLETED and session.quote:
            data["summary"] = {
                "distance_km": session.quote.get("route", {}).get("distance_km", 0),
                "duration_min": round((session.completed_at - session.created_at) / 60, 1)
                if session.completed_at
                else 0,
                "price_eur": session.quote.get("price", {}).get("amount_eur", 0),
            }

        return data
