import { useState, useCallback, useMemo } from 'react'
import DemandMap from './components/DemandMap'
import StatsPanel from './components/StatsPanel'
import Legend from './components/Legend'
import HexDetail from './components/HexDetail'
import { useWebSocket } from './hooks/useWebSocket'
import type { Hexagon, MetricType, WindowMinutes } from './types'

const getWebSocketUrl = (): string => {
  if (import.meta.env.DEV) return 'ws://localhost:8003/ws'
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/ws`
}

export default function App() {
  const [windowMinutes, setWindowMinutes] = useState<WindowMinutes>(5)
  const [metricType, setMetricType] = useState<MetricType>('requests')
  const [selectedHexId, setSelectedHexId] = useState<string | null>(null)

  const { data: hexagons, stats, isConnected, error, lastUpdate, requestUpdate } =
    useWebSocket(getWebSocketUrl())

  const handleWindowChange = useCallback(
    (w: WindowMinutes) => {
      setWindowMinutes(w)
      requestUpdate(w)
    },
    [requestUpdate],
  )

  // Store only the id — live data is derived below so the panel always reflects the latest tick
  const handleHexClick = useCallback((hex: Hexagon | null) => {
    setSelectedHexId(hex?.h3_index ?? null)
  }, [])

  const filteredHexagons = useMemo((): Hexagon[] => {
    if (!hexagons) return []
    switch (metricType) {
      case 'requests': return hexagons.filter((h) => h.ride_requests > 0)
      case 'drivers':  return hexagons.filter((h) => h.idle_drivers > 0)
      case 'ratio':    return hexagons.filter((h) => h.ride_requests > 0 || h.idle_drivers > 0)
    }
  }, [hexagons, metricType])

  const selectedHex = useMemo(
    () => (selectedHexId ? filteredHexagons.find((h) => h.h3_index === selectedHexId) ?? null : null),
    [selectedHexId, filteredHexagons],
  )

  return (
    // min-w-[1024px] prevents sub-desktop layouts from silently breaking
    <div className="relative w-full h-full min-w-[1024px] bg-night-950">
      {/* Full-screen map */}
      <DemandMap
        hexagons={filteredHexagons}
        metricType={metricType}
        onHexClick={handleHexClick}
      />

      {/* Left panel: stats + controls */}
      <StatsPanel
        stats={stats}
        hexagonCount={filteredHexagons.length}
        isConnected={isConnected}
        lastUpdate={lastUpdate}
        windowMinutes={windowMinutes}
        metricType={metricType}
        onWindowChange={handleWindowChange}
        onMetricChange={setMetricType}
      />

      {/* Bottom-left: legend */}
      <Legend metricType={metricType} />

      {/* Right panel: hex detail (shown on click) */}
      <HexDetail hex={selectedHex} onClose={() => setSelectedHexId(null)} />

      {/* Connection error banner — top-center to avoid conflicting with HexDetail */}
      {error && !isConnected && (
        <div className="absolute top-4 left-1/2 -translate-x-1/2 z-30">
          <div className="glass-panel rounded-xl px-4 py-3 border-l-4 border-red-500">
            <div className="flex items-center gap-3">
              <span className="text-red-400">⚠</span>
              <div>
                <p className="text-sm font-medium text-night-100">Connection Lost</p>
                <p className="text-xs text-night-400">Attempting to reconnect...</p>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Branding chip */}
      <div className="absolute bottom-6 right-4 z-10">
        <div className="glass-panel rounded-lg px-3 py-2">
          <span className="text-[10px] text-night-500 font-display tracking-wider">
            H3 RES-8 · {windowMinutes}MIN WINDOW
          </span>
        </div>
      </div>
    </div>
  )
}
