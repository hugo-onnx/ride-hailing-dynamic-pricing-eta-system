import { getDemandLevel } from '../utils/colors'
import Tooltip from './Tooltip'
import type { Hexagon } from '../types'

const TIPS = {
  ride_requests: 'Number of ride requests that originated in this cell during the active time window.',
  idle_drivers: 'Drivers currently in this cell with no active trip — available to accept new ride requests immediately.',
  on_trip_drivers: 'Drivers passing through or finishing a trip in this cell. They are not yet available for new requests.',
  total_drivers: 'Sum of idle and on-trip drivers. Represents total driver presence in this cell.',
  ratio: 'Idle drivers ÷ ride requests. Above 1 (green) means supply exceeds demand; below 1 (red) means more requests than available drivers.',
  intensity: 'Visual scale of ride request volume relative to the high-demand threshold (40 req). Reflects how busy this cell is.',
}

interface MetricRowProps {
  label: string
  value: string | number
  className?: string
  tip?: string
}

function MetricRow({ label, value, className, tip }: MetricRowProps) {
  return (
    <div className="flex justify-between items-center py-2 border-b border-night-800/60 last:border-0">
      <span className="flex items-center gap-1 text-night-400 text-sm">
        {label}
        {tip && (
          <Tooltip text={tip} position="left">
            <span className="text-[10px] text-night-600 hover:text-night-400 cursor-default select-none leading-none">
              ⓘ
            </span>
          </Tooltip>
        )}
      </span>
      <span className={`font-semibold font-display text-sm ${className ?? 'text-night-100'}`}>
        {value}
      </span>
    </div>
  )
}

interface HexDetailProps {
  hex: Hexagon | null
  onClose: () => void
}

export default function HexDetail({ hex, onClose }: HexDetailProps) {
  if (!hex) return null

  const ratio = hex.idle_drivers / (hex.ride_requests + 1)
  const demandLevel = getDemandLevel(hex.ride_requests)
  const ratioClass = ratio >= 1 ? 'text-emerald-400' : 'text-red-400'
  const isShortage = hex.ride_requests >= 3 && ratio < 0.5

  return (
    <div className="absolute top-4 right-4 z-20 w-72 hex-detail-enter">
      <div className="glass-panel rounded-2xl p-5">
        {/* Shortage alert banner */}
        {isShortage && (
          <div className="flex items-center gap-2 bg-red-500/15 border border-red-500/30 rounded-lg px-3 py-2 mb-3">
            <span className="text-red-400 text-sm leading-none">⚠</span>
            <div>
              <p className="text-xs font-semibold text-red-400">Supply Shortage</p>
              <p className="text-[10px] text-red-400/70">Demand exceeds available drivers</p>
            </div>
          </div>
        )}

        {/* Header */}
        <div className="flex items-start justify-between mb-4">
          <div>
            <h2 className="text-sm font-semibold text-night-100 mb-0.5">Cell Detail</h2>
            <span className={`text-xs font-medium ${demandLevel.class}`}>
              {demandLevel.label} Demand
            </span>
          </div>
          <button
            onClick={onClose}
            className="text-night-500 hover:text-night-200 transition-colors text-xl leading-none w-6 h-6 flex items-center justify-center"
            aria-label="Close"
          >
            ×
          </button>
        </div>

        {/* Metrics */}
        <div>
          <MetricRow
            label="Ride Requests"
            value={hex.ride_requests}
            className={demandLevel.class}
            tip={TIPS.ride_requests}
          />
          <MetricRow
            label="Idle Drivers"
            value={hex.idle_drivers}
            className="text-emerald-400"
            tip={TIPS.idle_drivers}
          />
          <MetricRow
            label="On-Trip Drivers"
            value={hex.on_trip_drivers}
            tip={TIPS.on_trip_drivers}
          />
          <MetricRow
            label="Total Drivers"
            value={hex.idle_drivers + hex.on_trip_drivers}
            tip={TIPS.total_drivers}
          />
          <MetricRow
            label="Supply / Demand"
            value={ratio.toFixed(2)}
            className={ratioClass}
            tip={TIPS.ratio}
          />
        </div>

        {/* Demand bar */}
        <div className="mt-4 pt-3 border-t border-night-800">
          <div className="flex items-center justify-between mb-1.5">
            <span className="flex items-center gap-1 text-[10px] text-night-500 uppercase tracking-wider">
              Intensity
              <Tooltip text={TIPS.intensity} position="left">
                <span className="text-[10px] text-night-600 hover:text-night-400 cursor-default select-none leading-none normal-case">
                  ⓘ
                </span>
              </Tooltip>
            </span>
            <span className={`text-[10px] font-medium ${demandLevel.class}`}>
              {hex.ride_requests} req
            </span>
          </div>
          <div className="h-1 bg-night-800 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full transition-all duration-500"
              style={{
                width: `${Math.min(hex.ride_requests / 10 * 100, 100)}%`,
                background: 'linear-gradient(90deg, #10b981, #f59e0b, #f04438)',
              }}
            />
          </div>
        </div>

        {/* H3 index */}
        <p className="mt-3 text-[10px] text-night-600 font-display break-all">
          {hex.h3_index}
        </p>
      </div>
    </div>
  )
}
