import type { RideState, LatLng } from '../types.ts';

interface Props {
  state: RideState;
  pickup: LatLng | null;
  loading: boolean;
  error: string | null;
}

export default function LocationPicker({ state, pickup, loading, error }: Props) {
  if (state !== 'IDLE' && state !== 'PICKING_PICKUP' && state !== 'PICKING_DROPOFF') {
    return null;
  }

  return (
    <div className="absolute top-6 left-1/2 -translate-x-1/2 z-10 animate-slide-up">
      <div className="bg-white rounded-2xl shadow-lg px-6 py-4 min-w-[320px]">
        <div className="flex items-center gap-3 mb-3">
          <div className="w-8 h-8 rounded-full bg-black flex items-center justify-center">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <circle cx="8" cy="8" r="3" fill="white" />
            </svg>
          </div>
          <div>
            <div className="text-sm font-medium text-gray-900">Where to?</div>
            <div className="text-xs text-gray-500">Click on the map to set your route</div>
          </div>
        </div>

        <div className="space-y-2">
          <div className="flex items-center gap-3">
            <div className={`w-3 h-3 rounded-full border-2 ${
              state === 'PICKING_PICKUP'
                ? 'border-green-500 bg-green-100 animate-pulse-dot'
                : pickup
                  ? 'border-green-500 bg-green-500'
                  : 'border-gray-300'
            }`} />
            <span className={`text-sm ${
              state === 'PICKING_PICKUP' ? 'text-gray-900 font-medium' : 'text-gray-500'
            }`}>
              {pickup
                ? `${pickup.lat.toFixed(4)}, ${pickup.lng.toFixed(4)}`
                : 'Click to set pickup'}
            </span>
          </div>

          <div className="ml-1.5 border-l border-dashed border-gray-300 h-3" />

          <div className="flex items-center gap-3">
            <div className={`w-3 h-3 rounded-full border-2 ${
              state === 'PICKING_DROPOFF'
                ? 'border-red-500 bg-red-100 animate-pulse-dot'
                : 'border-gray-300'
            }`} />
            <span className={`text-sm ${
              state === 'PICKING_DROPOFF' ? 'text-gray-900 font-medium' : 'text-gray-400'
            }`}>
              {state === 'PICKING_DROPOFF' ? 'Click to set destination' : 'Destination'}
            </span>
          </div>
        </div>

        {loading && (
          <div className="mt-3 flex items-center gap-2 text-sm text-gray-500">
            <div className="w-4 h-4 border-2 border-gray-300 border-t-black rounded-full animate-spin" />
            Getting your quote...
          </div>
        )}

        {error && (
          <div className="mt-3 text-sm text-red-600 bg-red-50 rounded-lg px-3 py-2">
            {error}
          </div>
        )}
      </div>
    </div>
  );
}
