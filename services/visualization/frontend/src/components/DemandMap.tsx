import { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import * as h3 from 'h3-js'
import { getMetricExpression, getMetricValue } from '../utils/colors'
import type { Hexagon, MetricType } from '../types'
import type { Feature, Polygon } from 'geojson'

const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json'
const INITIAL_CENTER: [number, number] = [-3.7038, 40.4168] // Madrid
const INITIAL_ZOOM = 11.5
const ANIM_DURATION = 700 // ms for demand value interpolation
// Shortage alert thresholds — cell must have meaningful demand AND too few drivers
const ALERT_MIN_REQUESTS = 3
const ALERT_RATIO_THRESHOLD = 0.5 // idle_drivers / (ride_requests + 1)

interface HexProperties {
  h3_index: string
  demand_value: number
  ride_requests: number
  idle_drivers: number
  on_trip_drivers: number
  demand_ratio: number
  ratio: number
}

function buildFeature(hex: Hexagon, demandValue: number): Feature<Polygon, HexProperties> {
  const boundary = h3.cellToBoundary(hex.h3_index, true) // [lng, lat] pairs
  return {
    type: 'Feature',
    id: hex.h3_index, // stable string ID — enables feature state for hover/select
    properties: {
      h3_index: hex.h3_index,
      demand_value: demandValue,
      ride_requests: hex.ride_requests,
      idle_drivers: hex.idle_drivers,
      on_trip_drivers: hex.on_trip_drivers,
      demand_ratio: hex.demand_ratio,
      ratio: hex.idle_drivers / (hex.ride_requests + 1),
    },
    geometry: { type: 'Polygon', coordinates: [boundary] },
  }
}

function easeOutCubic(t: number): number {
  return 1 - Math.pow(1 - t, 3)
}

interface DemandMapProps {
  hexagons: Hexagon[]
  metricType: MetricType
  onHexClick: (hex: Hexagon | null) => void
}

export default function DemandMap({ hexagons, metricType, onHexClick }: DemandMapProps) {
  const mapContainer = useRef<HTMLDivElement | null>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const popupRef = useRef<maplibregl.Popup | null>(null)
  const hoveredIdRef = useRef<string | number | undefined>(undefined)
  const animationRef = useRef<number | null>(null)
  const pulseIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const prevValuesRef = useRef<Record<string, number>>({}) // h3_index → last rendered demand_value
  const onHexClickRef = useRef<(hex: Hexagon | null) => void>(onHexClick)
  const [mapLoaded, setMapLoaded] = useState(false)

  // Keep callback ref fresh without re-registering map events
  useEffect(() => { onHexClickRef.current = onHexClick }, [onHexClick])

  // Initialize map once
  useEffect(() => {
    if (mapRef.current) return

    const map = new maplibregl.Map({
      container: mapContainer.current!,
      style: MAP_STYLE,
      center: INITIAL_CENTER,
      zoom: INITIAL_ZOOM,
      attributionControl: {},
    })

    const popup = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false,
      className: 'hexagon-popup',
      offset: 15,
    })
    popupRef.current = popup

    map.on('load', () => {
      map.addSource('hexagons', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
        // No generateId — h3_index is the stable feature ID
      })

      map.addLayer({
        id: 'hexagons-fill',
        type: 'fill',
        source: 'hexagons',
        paint: {
          'fill-color': getMetricExpression('requests'),
          'fill-opacity': [
            'case',
            ['boolean', ['feature-state', 'hover'], false],
            0.92,
            0.72,
          ],
        },
      })

      map.addLayer({
        id: 'hexagons-outline',
        type: 'line',
        source: 'hexagons',
        paint: {
          'line-color': getMetricExpression('requests'),
          'line-width': [
            'case',
            ['boolean', ['feature-state', 'hover'], false],
            2,
            0.8,
          ],
          'line-opacity': 0.9,
        },
      })

      // *-transition keys are valid MapLibre style-spec properties but absent from
      // the library's TypeScript paint types. setPaintProperty accepts `name: string`,
      // so we set them post-hoc to avoid unsafe casts in the addLayer calls above.
      map.setPaintProperty('hexagons-fill', 'fill-color-transition', { duration: 600, delay: 0 })
      map.setPaintProperty('hexagons-fill', 'fill-opacity-transition', { duration: 250, delay: 0 })
      map.setPaintProperty('hexagons-outline', 'line-color-transition', { duration: 600, delay: 0 })

      // Shortage alert overlay — pulsing red fill on cells with critical supply shortage
      map.addLayer({
        id: 'hexagons-alert',
        type: 'fill',
        source: 'hexagons',
        filter: [
          'all',
          ['>=', ['get', 'ride_requests'], ALERT_MIN_REQUESTS],
          ['<', ['get', 'ratio'], ALERT_RATIO_THRESHOLD],
        ],
        paint: {
          'fill-color': '#ef4444',
          'fill-opacity': 0.25,
        },
      })

      // White outline for selected hex
      map.addLayer({
        id: 'hexagons-selected',
        type: 'line',
        source: 'hexagons',
        paint: {
          'line-color': '#ffffff',
          'line-width': [
            'case',
            ['boolean', ['feature-state', 'selected'], false],
            2.5,
            0,
          ],
          'line-opacity': 0.7,
        },
      })

      // Pulse the alert layer opacity to draw attention to shortage cells
      let pulseT = 0
      pulseIntervalRef.current = setInterval(() => {
        if (!mapRef.current) return
        pulseT += 0.08
        const opacity = 0.1 + 0.25 * (0.5 + 0.5 * Math.sin(pulseT))
        mapRef.current.setPaintProperty('hexagons-alert', 'fill-opacity', opacity)
      }, 50)

      setMapLoaded(true)
    })

    // Hover: update feature state + popup
    map.on('mousemove', 'hexagons-fill', (e) => {
      if (!e.features?.length) return
      map.getCanvas().style.cursor = 'pointer'

      const id = e.features[0].id as string | number | undefined
      if (hoveredIdRef.current !== id) {
        if (hoveredIdRef.current !== undefined) {
          map.setFeatureState({ source: 'hexagons', id: hoveredIdRef.current }, { hover: false })
        }
        hoveredIdRef.current = id
        if (id !== undefined) {
          map.setFeatureState({ source: 'hexagons', id }, { hover: true })
        }
      }

      const p = e.features[0].properties as HexProperties
      popup
        .setLngLat(e.lngLat)
        .setHTML(`
          <div class="popup-content">
            <div class="popup-title">${p.ride_requests} requests</div>
            <div class="popup-stats">
              <span>Idle: ${p.idle_drivers}</span>
              <span>On trip: ${p.on_trip_drivers}</span>
            </div>
          </div>
        `)
        .addTo(map)
    })

    map.on('mouseleave', 'hexagons-fill', () => {
      map.getCanvas().style.cursor = ''
      if (hoveredIdRef.current !== undefined) {
        map.setFeatureState({ source: 'hexagons', id: hoveredIdRef.current }, { hover: false })
        hoveredIdRef.current = undefined
      }
      popup.remove()
    })

    // Click on hexagon — open detail panel
    map.on('click', 'hexagons-fill', (e) => {
      if (!e.features?.length) return
      const p = e.features[0].properties as HexProperties
      onHexClickRef.current?.({
        h3_index: p.h3_index,
        ride_requests: p.ride_requests,
        idle_drivers: p.idle_drivers,
        on_trip_drivers: p.on_trip_drivers,
        demand_ratio: p.demand_ratio,
      })
    })

    // Click on empty map — deselect
    map.on('click', (e) => {
      const features = map.queryRenderedFeatures(e.point, { layers: ['hexagons-fill'] })
      if (!features.length) onHexClickRef.current?.(null)
    })

    mapRef.current = map

    return () => {
      if (animationRef.current) cancelAnimationFrame(animationRef.current)
      if (pulseIntervalRef.current) clearInterval(pulseIntervalRef.current)
      popup.remove()
      map.remove()
      mapRef.current = null
    }
  }, [])

  // Swap color expressions when metric changes — MapLibre transitions the colors
  useEffect(() => {
    if (!mapLoaded || !mapRef.current) return
    const expr = getMetricExpression(metricType)
    mapRef.current.setPaintProperty('hexagons-fill', 'fill-color', expr)
    mapRef.current.setPaintProperty('hexagons-outline', 'line-color', expr)
  }, [metricType, mapLoaded])

  // Animate data updates: interpolate demand_value from old → new over ANIM_DURATION
  useEffect(() => {
    if (!mapLoaded || !mapRef.current) return

    if (!hexagons || hexagons.length === 0) {
      ;(mapRef.current.getSource('hexagons') as maplibregl.GeoJSONSource | undefined)?.setData({
        type: 'FeatureCollection',
        features: [],
      })
      prevValuesRef.current = {}
      return
    }

    const targetValues: Record<string, number> = {}
    hexagons.forEach((h) => {
      targetValues[h.h3_index] = getMetricValue(h, metricType)
    })

    const oldValues = prevValuesRef.current
    if (animationRef.current) cancelAnimationFrame(animationRef.current)

    const startTime = performance.now()

    function frame(now: number) {
      if (!mapRef.current) return

      const t = easeOutCubic(Math.min((now - startTime) / ANIM_DURATION, 1))

      const features = hexagons.map((hex) => {
        const newVal = targetValues[hex.h3_index]
        const oldVal = oldValues[hex.h3_index] ?? 0
        return buildFeature(hex, oldVal + (newVal - oldVal) * t)
      })

      ;(mapRef.current.getSource('hexagons') as maplibregl.GeoJSONSource | undefined)?.setData({
        type: 'FeatureCollection',
        features,
      })

      if (t < 1) {
        animationRef.current = requestAnimationFrame(frame)
      } else {
        prevValuesRef.current = targetValues
        animationRef.current = null
      }
    }

    animationRef.current = requestAnimationFrame(frame)

    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current)
        animationRef.current = null
      }
    }
  }, [hexagons, metricType, mapLoaded])

  return (
    <div className="map-container">
      <div ref={mapContainer} style={{ width: '100%', height: '100%' }} />

      {(!hexagons || hexagons.length === 0) && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
          <div className="glass-panel rounded-2xl p-8 text-center">
            <div className="text-4xl mb-4">📡</div>
            <h2 className="text-lg font-semibold text-night-200 mb-2">
              Waiting for data...
            </h2>
            <p className="text-sm text-night-400">Connecting to real-time stream</p>
            <div className="mt-4 flex justify-center">
              <div className="shimmer w-32 h-2 rounded-full" />
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
