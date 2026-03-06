export type RideState =
  | 'IDLE'
  | 'PICKING_PICKUP'
  | 'PICKING_DROPOFF'
  | 'QUOTED'
  | 'REQUESTING'
  | 'DRIVER_ASSIGNED'
  | 'DRIVER_APPROACHING'
  | 'DRIVER_ARRIVED'
  | 'ON_TRIP'
  | 'COMPLETED'
  | 'CANCELLED';

export interface LatLng {
  lat: number;
  lng: number;
}

export interface Quote {
  city: string;
  h3_origin: string;
  route: {
    source: string;
    distance_km: number;
    osrm_duration_min: number;
    geometry: GeoJSONLineString | null;
  };
  eta: {
    pickup_seconds: number;
    dropoff_seconds: number;
    dropoff_free_flow_seconds: number;
    congestion_factor: number;
    total_seconds: number;
    total_minutes: number;
  };
  price: {
    amount_eur: number;
    multiplier: number;
    surge_level: string;
    reasons: string[];
  };
  latency_ms: number;
}

export interface GeoJSONLineString {
  type: 'LineString';
  coordinates: [number, number][];
}

export interface DriverInfo {
  name: string;
  car_model: string;
  plate: string;
  rating: number;
  lat: number;
  lng: number;
}

export interface RideUpdate {
  type: 'ride_update';
  ride_id: string;
  state: string;
  driver?: DriverInfo;
  bearing?: number;
  progress?: number;
  eta_seconds?: number;
  summary?: {
    distance_km: number;
    duration_min: number;
    price_eur: number;
  };
}

export interface DemandHexagon {
  h3_index: string;
  ride_requests: number;
  idle_drivers: number;
  on_trip_drivers: number;
  demand_ratio: number;
}

export interface DemandUpdate {
  type: 'demand_update';
  window_minutes: number;
  hexagons: DemandHexagon[];
}
