import { useEffect, useRef, useCallback } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { cellToBoundary } from 'h3-js';
import type { RideState, LatLng, GeoJSONLineString, DriverInfo, DemandHexagon } from '../types.ts';

interface Props {
  state: RideState;
  pickup: LatLng | null;
  dropoff: LatLng | null;
  routeGeometry: GeoJSONLineString | null;
  driver: DriverInfo | null;
  bearing: number;
  demandHexagons: DemandHexagon[];
  showDemand: boolean;
  onMapClick: (latlng: LatLng) => void;
}

const MADRID_CENTER: [number, number] = [-3.7038, 40.4168];
const BASEMAP = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';

function createMarkerEl(type: 'pickup' | 'dropoff' | 'driver'): HTMLDivElement {
  const el = document.createElement('div');

  if (type === 'pickup') {
    el.innerHTML = `
      <div style="width:28px;height:28px;border-radius:50%;background:#16a34a;border:3px solid white;box-shadow:0 2px 8px rgba(0,0,0,0.3);display:flex;align-items:center;justify-content:center;">
        <div style="width:8px;height:8px;border-radius:50%;background:white;"></div>
      </div>`;
  } else if (type === 'dropoff') {
    el.innerHTML = `
      <div style="width:28px;height:28px;border-radius:50%;background:#dc2626;border:3px solid white;box-shadow:0 2px 8px rgba(0,0,0,0.3);display:flex;align-items:center;justify-content:center;">
        <div style="width:8px;height:8px;border-radius:50%;background:white;"></div>
      </div>`;
  } else {
    el.innerHTML = `
      <div style="width:36px;height:36px;display:flex;align-items:center;justify-content:center;filter:drop-shadow(0 2px 4px rgba(0,0,0,0.3));transition:transform 0.3s ease;">
        <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
          <rect x="6" y="10" width="20" height="12" rx="3" fill="#111827"/>
          <rect x="4" y="18" width="5" height="3" rx="1.5" fill="#374151"/>
          <rect x="23" y="18" width="5" height="3" rx="1.5" fill="#374151"/>
          <rect x="10" y="12" width="5" height="4" rx="1" fill="#60A5FA"/>
          <rect x="17" y="12" width="5" height="4" rx="1" fill="#60A5FA"/>
          <circle cx="9" cy="23" r="2" fill="#374151"/>
          <circle cx="23" cy="23" r="2" fill="#374151"/>
        </svg>
      </div>`;
  }

  return el;
}

function hexagonsToGeoJSON(hexagons: DemandHexagon[]): GeoJSON.FeatureCollection {
  const features = hexagons
    .filter(h => h.ride_requests > 0)
    .map(h => {
      const boundary = cellToBoundary(h.h3_index, true);
      boundary.push(boundary[0]);
      return {
        type: 'Feature' as const,
        properties: {
          demand: h.ride_requests,
          intensity: Math.min(h.ride_requests / 20, 1),
        },
        geometry: {
          type: 'Polygon' as const,
          coordinates: [boundary],
        },
      };
    });

  return { type: 'FeatureCollection', features };
}

export default function RideMap({
  state, pickup, dropoff, routeGeometry, driver, bearing,
  demandHexagons, showDemand, onMapClick,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const pickupMarkerRef = useRef<maplibregl.Marker | null>(null);
  const dropoffMarkerRef = useRef<maplibregl.Marker | null>(null);
  const driverMarkerRef = useRef<maplibregl.Marker | null>(null);
  const mapReady = useRef(false);

  // Initialize map
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: BASEMAP,
      center: MADRID_CENTER,
      zoom: 12.5,
      attributionControl: false,
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-left');

    map.on('load', () => {
      mapReady.current = true;

      // Route line shadow
      map.addSource('route', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      });
      map.addLayer({
        id: 'route-shadow',
        type: 'line',
        source: 'route',
        paint: {
          'line-color': '#00000030',
          'line-width': 8,
          'line-blur': 3,
        },
      });
      map.addLayer({
        id: 'route-line',
        type: 'line',
        source: 'route',
        layout: {
          'line-cap': 'round',
          'line-join': 'round',
        },
        paint: {
          'line-color': '#111827',
          'line-width': 4,
        },
      });

      // Demand heatmap
      map.addSource('demand', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      });
      map.addLayer({
        id: 'demand-fill',
        type: 'fill',
        source: 'demand',
        paint: {
          'fill-color': [
            'interpolate', ['linear'], ['get', 'intensity'],
            0, '#fef08a',
            0.5, '#f97316',
            1, '#dc2626',
          ],
          'fill-opacity': 0.2,
        },
        layout: { visibility: 'none' },
      });
      map.addLayer({
        id: 'demand-outline',
        type: 'line',
        source: 'demand',
        paint: {
          'line-color': '#f9731640',
          'line-width': 1,
        },
        layout: { visibility: 'none' },
      });
    });

    mapRef.current = map;

    return () => { map.remove(); };
  }, []);

  // Map click handler
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    const handler = (e: maplibregl.MapMouseEvent) => {
      if (state === 'PICKING_PICKUP' || state === 'PICKING_DROPOFF') {
        onMapClick({ lat: e.lngLat.lat, lng: e.lngLat.lng });
      }
    };

    map.on('click', handler);
    return () => { map.off('click', handler); };
  }, [state, onMapClick]);

  // Cursor style
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    map.getCanvas().style.cursor =
      state === 'PICKING_PICKUP' || state === 'PICKING_DROPOFF' ? 'crosshair' : '';
  }, [state]);

  // Pickup marker
  useEffect(() => {
    if (!mapRef.current) return;
    if (pickupMarkerRef.current) {
      pickupMarkerRef.current.remove();
      pickupMarkerRef.current = null;
    }
    if (pickup) {
      const marker = new maplibregl.Marker({ element: createMarkerEl('pickup') })
        .setLngLat([pickup.lng, pickup.lat])
        .addTo(mapRef.current);
      pickupMarkerRef.current = marker;
    }
  }, [pickup]);

  // Dropoff marker
  useEffect(() => {
    if (!mapRef.current) return;
    if (dropoffMarkerRef.current) {
      dropoffMarkerRef.current.remove();
      dropoffMarkerRef.current = null;
    }
    if (dropoff) {
      const marker = new maplibregl.Marker({ element: createMarkerEl('dropoff') })
        .setLngLat([dropoff.lng, dropoff.lat])
        .addTo(mapRef.current);
      dropoffMarkerRef.current = marker;
    }
  }, [dropoff]);

  // Route geometry
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady.current) return;

    const source = map.getSource('route') as maplibregl.GeoJSONSource | undefined;
    if (!source) return;

    if (routeGeometry) {
      source.setData({
        type: 'Feature',
        properties: {},
        geometry: routeGeometry,
      });

      // Fit bounds to route
      const coords = routeGeometry.coordinates;
      if (coords.length > 1) {
        const bounds = new maplibregl.LngLatBounds(coords[0], coords[0]);
        coords.forEach(c => bounds.extend(c as [number, number]));
        map.fitBounds(bounds, { padding: { top: 120, bottom: 280, left: 80, right: 80 }, duration: 800 });
      }
    } else {
      source.setData({ type: 'FeatureCollection', features: [] });
    }
  }, [routeGeometry]);

  // Driver marker
  const updateDriver = useCallback((d: DriverInfo | null, b: number) => {
    const map = mapRef.current;
    if (!map) return;

    if (!d) {
      if (driverMarkerRef.current) {
        driverMarkerRef.current.remove();
        driverMarkerRef.current = null;
      }
      return;
    }

    if (!driverMarkerRef.current) {
      const el = createMarkerEl('driver');
      const marker = new maplibregl.Marker({ element: el, rotationAlignment: 'map' })
        .setLngLat([d.lng, d.lat])
        .addTo(map);
      driverMarkerRef.current = marker;
    } else {
      driverMarkerRef.current.setLngLat([d.lng, d.lat]);
    }
    driverMarkerRef.current.setRotation(b);
  }, []);

  useEffect(() => {
    updateDriver(driver, bearing);
  }, [driver, bearing, updateDriver]);

  // Demand heatmap visibility
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady.current) return;
    const vis = showDemand ? 'visible' : 'none';
    if (map.getLayer('demand-fill')) map.setLayoutProperty('demand-fill', 'visibility', vis);
    if (map.getLayer('demand-outline')) map.setLayoutProperty('demand-outline', 'visibility', vis);
  }, [showDemand]);

  // Demand data
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady.current) return;
    const source = map.getSource('demand') as maplibregl.GeoJSONSource | undefined;
    if (!source) return;
    source.setData(hexagonsToGeoJSON(demandHexagons));
  }, [demandHexagons]);

  // Clear markers on reset (IDLE/PICKING_PICKUP with no pickup)
  useEffect(() => {
    if (state === 'PICKING_PICKUP' && !pickup) {
      if (pickupMarkerRef.current) {
        pickupMarkerRef.current.remove();
        pickupMarkerRef.current = null;
      }
      if (dropoffMarkerRef.current) {
        dropoffMarkerRef.current.remove();
        dropoffMarkerRef.current = null;
      }
      if (driverMarkerRef.current) {
        driverMarkerRef.current.remove();
        driverMarkerRef.current = null;
      }
      const map = mapRef.current;
      if (map && mapReady.current) {
        const source = map.getSource('route') as maplibregl.GeoJSONSource | undefined;
        if (source) source.setData({ type: 'FeatureCollection', features: [] });
        map.flyTo({ center: MADRID_CENTER, zoom: 12.5, duration: 800 });
      }
    }
  }, [state, pickup]);

  return <div ref={containerRef} className="w-full h-full" />;
}
