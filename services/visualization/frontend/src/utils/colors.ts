import type { ExpressionSpecification } from 'maplibre-gl'
import type { DemandLevel, Hexagon, MetricConfig, MetricType } from '../types'

/**
 * Metric configurations — defines color scales and MapLibre expressions per metric type.
 */
export const METRIC_CONFIGS: Record<MetricType, MetricConfig> = {
  requests: {
    label: 'Ride Requests',
    unit: 'per cell',
    stops: [0, 2, 5, 10],
    colors: ['#10b981', '#f59e0b', '#f04438', '#b91c1c'],
    highLabel: '10+',
  },
  drivers: {
    label: 'Idle Drivers',
    unit: 'per cell',
    stops: [0, 2, 5, 10],
    colors: ['#1e3a5f', '#2563eb', '#06b6d4', '#10b981'],
    highLabel: '10+',
  },
  ratio: {
    label: 'Supply / Demand',
    unit: 'ratio',
    stops: [0, 0.5, 1.0, 2.0],
    colors: ['#ef4444', '#f59e0b', '#22c55e', '#10b981'],
    highLabel: '2×',
  },
}

/**
 * Build a MapLibre interpolate expression for a given metric.
 * Used in fill-color and line-color paint properties.
 */
export function getMetricExpression(metricType: MetricType): ExpressionSpecification {
  const config = METRIC_CONFIGS[metricType]
  const [s0, s1, s2, s3] = config.stops
  const [c0, c1, c2, c3] = config.colors
  return [
    'interpolate', ['linear'], ['get', 'demand_value'],
    s0, c0,
    s1, c1,
    s2, c2,
    s3, c3,
  ]
}

/**
 * Compute the numeric demand value for a hexagon given the active metric.
 */
export function getMetricValue(hex: Hexagon, metricType: MetricType): number {
  switch (metricType) {
    case 'requests': return hex.ride_requests
    case 'drivers': return hex.idle_drivers
    case 'ratio': return hex.idle_drivers / (hex.ride_requests + 1)
  }
}

export function getDemandLevel(demandRatio: number): DemandLevel {
  if (demandRatio < 2) return { label: 'Low', class: 'text-emerald-400' }
  if (demandRatio < 5) return { label: 'Moderate', class: 'text-amber-400' }
  if (demandRatio < 10) return { label: 'High', class: 'text-orange-400' }
  return { label: 'Critical', class: 'text-red-400' }
}

export function formatNumber(num: number): string {
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M'
  if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K'
  return num.toString()
}

export function formatRelativeTime(date: Date | null): string {
  if (!date) return 'Never'
  const diff = Math.floor((Date.now() - date.getTime()) / 1000)
  if (diff < 5) return 'Just now'
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  return `${Math.floor(diff / 3600)}h ago`
}
