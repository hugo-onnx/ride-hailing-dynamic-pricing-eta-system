export interface Hexagon {
  h3_index: string
  ride_requests: number
  idle_drivers: number
  on_trip_drivers: number
  demand_ratio: number
}

export interface Stats {
  total_ride_requests: number
  total_idle_drivers: number
  avg_demand_ratio: number
}

export type MetricType = 'requests' | 'drivers' | 'ratio'
export type WindowMinutes = 1 | 5 | 15
export type TooltipPosition = 'top' | 'right' | 'left'

export interface DemandLevel {
  label: 'Low' | 'Moderate' | 'High' | 'Critical'
  class: string
}

export interface MetricConfig {
  label: string
  unit: string
  stops: [number, number, number, number]
  colors: [string, string, string, string]
  highLabel: string
}
