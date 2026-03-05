import { useMemo } from 'react'
import { formatNumber, getDemandLevel } from '../utils/colors'
import Tooltip from './Tooltip'
import type { MetricType, Stats, WindowMinutes } from '../types'

interface WindowOption {
  value: WindowMinutes
  label: string
  tip: string
}

interface MetricOption {
  value: MetricType
  label: string
  tip: string
}

const WINDOWS: WindowOption[] = [
  {
    value: 1,
    label: '1m',
    tip: 'Aggregates events from the last 1 minute. Most responsive — ideal for tracking fast-moving demand spikes.',
  },
  {
    value: 5,
    label: '5m',
    tip: 'Aggregates events from the last 5 minutes. Balanced view that smooths out momentary noise.',
  },
  {
    value: 15,
    label: '15m',
    tip: 'Aggregates events from the last 15 minutes. Best for spotting sustained demand patterns across the city.',
  },
]

const METRICS: MetricOption[] = [
  {
    value: 'requests',
    label: 'Requests',
    tip: 'Colors cells by the number of ride requests originated in each H3 cell during the selected window. Red = high demand.',
  },
  {
    value: 'drivers',
    label: 'Drivers',
    tip: 'Colors cells by idle (available) driver count. Blue/cyan = many drivers waiting. Useful for spotting supply hotspots.',
  },
  {
    value: 'ratio',
    label: 'Ratio',
    tip: 'Supply ÷ Demand: idle drivers divided by ride requests. Green (>1) means surplus — drivers outnumber requests. Red (<1) means shortage — more demand than supply.',
  },
]

const STAT_TIPS = {
  requests: `Total ride requests city-wide during the selected time window. A rising number means demand is increasing.`,
  idle: `Total idle (available) drivers across all cells right now. These drivers have no active trip and can accept new requests immediately.`,
  cells: `Number of H3 hexagonal cells (≈ 0.74 km² each) that recorded at least one event in the current window. Reflects how broadly activity is spread across the city.`,
  intensity: `Average ride requests per active cell. Low = quiet city; Critical = sustained high demand across cells. Drives the coloured bar below.`,
}

interface SectionLabelProps {
  children: React.ReactNode
  tip: string
}

function SectionLabel({ children, tip }: SectionLabelProps) {
  return (
    <div className="flex items-center gap-1 mb-1.5">
      <p className="text-[10px] text-night-500 uppercase tracking-wider">{children}</p>
      <Tooltip text={tip} position="right">
        <span className="text-[10px] text-night-600 hover:text-night-400 cursor-default select-none leading-none">
          ⓘ
        </span>
      </Tooltip>
    </div>
  )
}

function WindowToggleGroup({ options, value, onChange }: {
  options: WindowOption[]
  value: WindowMinutes
  onChange: (v: WindowMinutes) => void
}) {
  return (
    <div className="flex bg-night-900/60 rounded-lg p-0.5 gap-0.5">
      {options.map((opt) => (
        <Tooltip key={opt.value} text={opt.tip}>
          <button
            onClick={() => onChange(opt.value)}
            className={`flex-1 py-1 px-2 text-xs font-medium rounded-md transition-all duration-200 ${
              value === opt.value
                ? 'bg-night-700 text-night-100 shadow-sm'
                : 'text-night-400 hover:text-night-200'
            }`}
          >
            {opt.label}
          </button>
        </Tooltip>
      ))}
    </div>
  )
}

function MetricToggleGroup({ options, value, onChange }: {
  options: MetricOption[]
  value: MetricType
  onChange: (v: MetricType) => void
}) {
  return (
    <div className="flex bg-night-900/60 rounded-lg p-0.5 gap-0.5">
      {options.map((opt) => (
        <Tooltip key={opt.value} text={opt.tip}>
          <button
            onClick={() => onChange(opt.value)}
            className={`flex-1 py-1 px-2 text-xs font-medium rounded-md transition-all duration-200 ${
              value === opt.value
                ? 'bg-night-700 text-night-100 shadow-sm'
                : 'text-night-400 hover:text-night-200'
            }`}
          >
            {opt.label}
          </button>
        </Tooltip>
      ))}
    </div>
  )
}

interface StatCardProps {
  label: string
  value: string | number
  subValue?: string
  tip: string
}

function StatCard({ label, value, subValue, tip }: StatCardProps) {
  return (
    <Tooltip text={tip}>
      <div className="stat-card glass-panel rounded-xl p-3 w-full cursor-default">
        <p className="text-night-400 text-[10px] font-medium uppercase tracking-wider truncate mb-1">
          {label}
        </p>
        <p className="stat-value text-xl font-semibold font-display text-night-100 truncate">
          {value}
        </p>
        {subValue && (
          <p className="text-[10px] text-night-500 truncate">{subValue}</p>
        )}
      </div>
    </Tooltip>
  )
}

interface StatsPanelProps {
  stats: Stats | null
  hexagonCount: number
  isConnected: boolean
  lastUpdate: Date | null
  windowMinutes: WindowMinutes
  metricType: MetricType
  onWindowChange: (w: WindowMinutes) => void
  onMetricChange: (m: MetricType) => void
}

export default function StatsPanel({
  stats,
  hexagonCount,
  isConnected,
  lastUpdate,
  windowMinutes,
  metricType,
  onWindowChange,
  onMetricChange,
}: StatsPanelProps) {
  const demandLevel = useMemo(
    () => getDemandLevel(stats?.avg_demand_ratio ?? 0),
    [stats?.avg_demand_ratio],
  )

  return (
    <div className="absolute top-4 left-4 z-10 w-72 max-h-[calc(100vh-2rem)] overflow-y-auto">
      {/* Header card */}
      <div className="glass-panel rounded-2xl p-4 mb-2">
        <div className="flex items-center justify-between mb-3">
          <div>
            <h1 className="text-lg font-semibold font-display text-night-100">Madrid</h1>
            <p className="text-xs text-night-400 mt-0.5">Real-time demand heatmap</p>
          </div>
          <div className="flex items-center gap-2">
            <div
              className={`status-indicator w-2.5 h-2.5 rounded-full ${
                isConnected ? 'bg-emerald-400' : 'bg-red-400'
              }`}
            />
            <span className="text-xs text-night-400">{isConnected ? 'Live' : 'Offline'}</span>
          </div>
        </div>

        {/* Demand intensity bar */}
        <div className="bg-night-900/50 rounded-xl p-2.5 border border-night-700/50 mb-3">
          <div className="flex items-center justify-between mb-1.5">
            <div className="flex items-center gap-1">
              <span className="text-[10px] text-night-400 uppercase tracking-wider">
                Avg Requests / Cell
              </span>
              <Tooltip text={STAT_TIPS.intensity} position="right">
                <span className="text-[10px] text-night-600 hover:text-night-400 cursor-default select-none leading-none">
                  ⓘ
                </span>
              </Tooltip>
            </div>
            <span className={`text-xs font-semibold ${demandLevel.class}`}>
              {demandLevel.label}
            </span>
          </div>
          <div className="h-1.5 bg-night-800 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full transition-all duration-500"
              style={{
                width: `${Math.min((stats?.avg_demand_ratio ?? 0) / 10 * 100, 100)}%`,
                background: 'linear-gradient(90deg, #10b981, #f59e0b, #f04438)',
              }}
            />
          </div>
        </div>

        {/* Time window toggle */}
        <div className="mb-2.5">
          <SectionLabel tip="Choose how far back in time to aggregate events. Shorter windows are more reactive; longer windows show sustained trends.">
            Time window
          </SectionLabel>
          <WindowToggleGroup options={WINDOWS} value={windowMinutes} onChange={onWindowChange} />
        </div>

        {/* Metric toggle */}
        <div>
          <SectionLabel tip="Choose which metric drives the cell colour on the map. Hover each option for details.">
            Color by
          </SectionLabel>
          <MetricToggleGroup options={METRICS} value={metricType} onChange={onMetricChange} />
        </div>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-3 gap-1.5">
        <StatCard
          label="Requests"
          value={formatNumber(stats?.total_ride_requests ?? 0)}
          subValue={`/${windowMinutes}m`}
          tip={STAT_TIPS.requests}
        />
        <StatCard
          label="Idle"
          value={formatNumber(stats?.total_idle_drivers ?? 0)}
          subValue="drivers"
          tip={STAT_TIPS.idle}
        />
        <StatCard
          label="Cells"
          value={hexagonCount || 0}
          subValue="active"
          tip={STAT_TIPS.cells}
        />
      </div>

      {lastUpdate && (
        <div className="mt-2 text-center">
          <span className="text-[10px] text-night-500 font-display">
            Updated {lastUpdate.toLocaleTimeString()}
          </span>
        </div>
      )}
    </div>
  )
}
