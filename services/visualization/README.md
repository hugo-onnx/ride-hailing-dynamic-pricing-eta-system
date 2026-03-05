# Visualization Service

Real-time demand heatmap for Madrid. A FastAPI backend reads aggregated H3 data from Redis every second and broadcasts it to browser clients over WebSocket. The React frontend renders the hexagons as GeoJSON fill layers on a MapLibre GL map.

## Architecture

```
Feature Consumer ──▶ Redis ──▶ Viz Backend (FastAPI) ──▶ WebSocket ──▶ React UI
                                     │
                                     └──▶ REST /api/demand  (on-demand snapshot)
```

## Tech Stack

| Layer | Technology |
|-------|------------|
| Backend | Python 3.12 · FastAPI · `redis.asyncio` |
| Frontend | React 18 · TypeScript (strict) · Vite |
| Map | MapLibre GL JS v4 · CARTO dark basemap |
| Spatial | H3 resolution 8 cells (≈ 0.74 km²) |
| Styling | Tailwind CSS v3 |

## Features

- **Real-time updates** — WebSocket broadcast every second; per-client window preference
- **Three metrics** — ride requests, idle drivers, or supply/demand ratio (selectable)
- **Time windows** — 1 min, 5 min, 15 min aggregation windows (switchable without reconnect)
- **Shortage alerts** — pulsing red overlay on cells where `ride_requests ≥ 3` and `idle_drivers / (ride_requests + 1) < 0.5`
- **Animated transitions** — 700 ms ease-out interpolation between data ticks
- **Cell detail panel** — click any hexagon for per-cell stats
- **Connection resilience** — exponential-free auto-reconnect with keepalive pings

## Frontend Source Layout

```
src/
  types.ts                # Shared domain interfaces (Hexagon, Stats, MetricType …)
  main.tsx                # Entry point
  App.tsx                 # Root component: state, filtering, layout
  hooks/
    useWebSocket.ts       # WS lifecycle: connect, reconnect, ping, requestUpdate
  utils/
    colors.ts             # METRIC_CONFIGS, MapLibre expressions, demand-level helpers
  components/
    DemandMap.tsx         # MapLibre map, GeoJSON layers, animation frame loop
    StatsPanel.tsx        # Left panel: stats cards, window + metric toggles
    HexDetail.tsx         # Right panel: per-cell detail on click
    Legend.tsx            # Color-scale legend
    Tooltip.tsx           # Portal-based tooltip (never clipped by overflow ancestors)
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Serves the compiled frontend |
| `GET` | `/health` | `{"status":"healthy","city":"madrid"}` |
| `GET` | `/metrics` | Prometheus metrics (text/plain) |
| `GET` | `/api/demand?window=5` | Demand snapshot for the given window (1/5/15 min) |
| `GET` | `/api/stats?window=5` | Aggregate statistics |
| `WS` | `/ws` | Real-time push stream |

## WebSocket Protocol

### Server → client

```jsonc
{
  "type": "demand_update",        // or "initial" on first connect
  "timestamp": "2024-01-15T10:30:00Z",
  "window_minutes": 5,
  "hexagons": [
    {
      "h3_index": "883969a4c3fffff",
      "ride_requests": 15,
      "idle_drivers": 3,
      "on_trip_drivers": 5,
      "total_drivers": 8,
      "demand_ratio": 15,         // equals ride_requests (raw count, not a ratio)
      "window_start": "2024-01-15T10:25:00Z"
    }
  ],
  "stats": {
    "total_hexagons": 150,
    "total_ride_requests": 450,
    "total_idle_drivers": 200,
    "total_on_trip_drivers": 120,
    "avg_demand_ratio": 5.25
  }
}
```

> **Note on `demand_ratio`**: the backend sets this field to `ride_requests` (a raw count, not a
> supply/demand ratio). The "Ratio" metric shown in the UI is computed client-side as
> `idle_drivers / (ride_requests + 1)`.

### Client → server

```json
{ "type": "ping" }
{ "type": "request_update", "window": 5 }
```

`request_update` persists the window preference server-side so subsequent broadcast ticks
automatically use the new window without the client sending another message.

## Demand Levels

Derived from `demand_ratio` (= `ride_requests`) in `src/utils/colors.ts`:

| `demand_ratio` | Label | Tailwind class |
|----------------|-------|----------------|
| < 2 | Low | `text-emerald-400` |
| 2 – 4 | Moderate | `text-amber-400` |
| 5 – 9 | High | `text-orange-400` |
| ≥ 10 | Critical | `text-red-400` |

## Prometheus Metrics

| Metric | Labels | Description |
|--------|--------|-------------|
| `viz_websocket_connections` | `city` | Active WebSocket connections (gauge) |
| `viz_hexagons_sent_total` | `city`, `window` | Hexagon batches sent (counter) |
| `viz_broadcast_latency_seconds` | `city` | Broadcast loop duration (histogram) |

## Development

### Prerequisites

- Python 3.12, `uv`
- Node.js 20+
- Redis (or run `docker compose up redis`)

### Backend

```bash
cd services/visualization
uv run uvicorn main:app --reload --port 8003
```

### Frontend (dev server with HMR)

```bash
cd services/visualization/frontend
npm install
npm run dev          # http://localhost:5173 — proxies /ws and /api to :8003
```

### Type-check

```bash
npm run typecheck    # tsc --noEmit (strict mode, zero errors required)
```

### Production build

```bash
npm run build        # outputs to dist/, served by the FastAPI backend
```

### Full stack (Docker)

```bash
docker compose up visualization --build
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `CITY` | `madrid` | City prefix used in Redis key patterns |
