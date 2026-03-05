import { METRIC_CONFIGS } from '../utils/colors'
import type { MetricType } from '../types'

interface LegendProps {
  metricType: MetricType
}

export default function Legend({ metricType }: LegendProps) {
  const config = METRIC_CONFIGS[metricType]
  const [c0, c1, c2, c3] = config.colors
  const [s0, s1, s2] = config.stops

  return (
    <div className="absolute bottom-6 left-4 z-10">
      <div className="glass-panel rounded-xl p-4 w-64 transition-all duration-300">
        <h3 className="text-xs font-medium text-night-300 uppercase tracking-wider mb-2">
          {config.label}
          <span className="ml-1 text-night-500 font-normal normal-case">
            ({config.unit})
          </span>
        </h3>

        {/* Dynamic gradient bar */}
        <div
          className="h-3 rounded-full mb-2 transition-all duration-500"
          style={{ background: `linear-gradient(to right, ${c0}, ${c1}, ${c2}, ${c3})` }}
        />

        {/* Scale labels */}
        <div className="flex justify-between text-[10px] text-night-400 font-display">
          <span>{s0}</span>
          <span>{s1}</span>
          <span>{s2}</span>
          <span>{config.highLabel}</span>
        </div>

        <div className="mt-3 pt-3 border-t border-night-700/50">
          <p className="text-[10px] text-night-500 leading-relaxed">
            Color shows{' '}
            <span className="text-night-300">{config.label.toLowerCase()}</span>{' '}
            per hexagon in the selected window.
          </p>
        </div>

        <div className="mt-2 flex items-center gap-2 text-[10px] text-night-500">
          <kbd className="px-1.5 py-0.5 bg-night-800 rounded text-night-400 font-display">
            Scroll
          </kbd>
          <span>to zoom</span>
          <kbd className="px-1.5 py-0.5 bg-night-800 rounded text-night-400 font-display ml-2">
            Drag
          </kbd>
          <span>to pan</span>
        </div>
      </div>
    </div>
  )
}
