import type { RideState, DriverInfo, Quote } from '../types.ts';

interface Props {
  state: RideState;
  driver: DriverInfo | null;
  progress: number;
  etaSeconds: number;
  summary: { distance_km: number; duration_min: number; price_eur: number } | null;
  quote: Quote | null;
  onCancel: () => void;
  onNewRide: () => void;
}

function ProgressBar({ value }: { value: number }) {
  return (
    <div className="w-full h-1.5 bg-gray-100 rounded-full overflow-hidden">
      <div
        className="h-full bg-black rounded-full transition-all duration-500"
        style={{ width: `${Math.min(value * 100, 100)}%` }}
      />
    </div>
  );
}

function DriverCard({ driver }: { driver: DriverInfo }) {
  return (
    <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-xl">
      <div className="w-10 h-10 rounded-full bg-gray-200 flex items-center justify-center text-lg">
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <circle cx="10" cy="7" r="3" stroke="#374151" strokeWidth="1.5" fill="none"/>
          <path d="M4 17c0-3.3 2.7-6 6-6s6 2.7 6 6" stroke="#374151" strokeWidth="1.5" fill="none"/>
        </svg>
      </div>
      <div className="flex-1">
        <div className="font-medium text-gray-900 text-sm">{driver.name}</div>
        <div className="text-xs text-gray-500">{driver.car_model} · {driver.plate}</div>
      </div>
      <div className="flex items-center gap-1 text-sm">
        <svg width="14" height="14" viewBox="0 0 14 14" fill="#FBBF24">
          <path d="M7 1l1.8 3.6L13 5.3l-3 2.9.7 4.1L7 10.4 3.3 12.3l.7-4.1-3-2.9 4.2-.7z"/>
        </svg>
        <span className="font-medium text-gray-700">{driver.rating}</span>
      </div>
    </div>
  );
}

export default function RideProgress({
  state, driver, progress, etaSeconds, summary, quote, onCancel, onNewRide,
}: Props) {
  const activeStates: RideState[] = [
    'REQUESTING', 'DRIVER_ASSIGNED', 'DRIVER_APPROACHING',
    'DRIVER_ARRIVED', 'ON_TRIP', 'COMPLETED', 'CANCELLED',
  ];
  if (!activeStates.includes(state)) return null;

  const canCancel = ['REQUESTING', 'DRIVER_ASSIGNED', 'DRIVER_APPROACHING', 'DRIVER_ARRIVED'].includes(state);
  const etaMin = Math.max(1, Math.ceil(etaSeconds / 60));

  return (
    <div className="absolute bottom-0 left-0 right-0 z-10 animate-slide-up">
      <div className="bg-white rounded-t-3xl shadow-2xl px-6 pt-5 pb-6 max-w-lg mx-auto">

        {state === 'REQUESTING' && (
          <div className="flex flex-col items-center py-4 gap-3">
            <div className="w-10 h-10 border-3 border-gray-200 border-t-black rounded-full animate-spin" />
            <div className="text-gray-900 font-medium">Finding your driver...</div>
            <div className="text-sm text-gray-400">This usually takes a few seconds</div>
          </div>
        )}

        {state === 'DRIVER_ASSIGNED' && driver && (
          <div className="space-y-3">
            <div className="text-gray-900 font-medium">Driver found!</div>
            <DriverCard driver={driver} />
          </div>
        )}

        {state === 'DRIVER_APPROACHING' && driver && (
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <div className="text-gray-900 font-medium">Driver is on the way</div>
              <div className="text-lg font-semibold text-gray-900">
                {etaMin}<span className="text-sm font-normal text-gray-400 ml-1">min</span>
              </div>
            </div>
            <ProgressBar value={progress} />
            <DriverCard driver={driver} />
          </div>
        )}

        {state === 'DRIVER_ARRIVED' && driver && (
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <div className="w-2.5 h-2.5 rounded-full bg-green-500 animate-pulse-dot" />
              <div className="text-gray-900 font-medium">Your driver has arrived</div>
            </div>
            <DriverCard driver={driver} />
          </div>
        )}

        {state === 'ON_TRIP' && driver && (
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <div className="text-gray-900 font-medium">On your way</div>
              <div className="text-lg font-semibold text-gray-900">
                {etaMin}<span className="text-sm font-normal text-gray-400 ml-1">min left</span>
              </div>
            </div>
            <ProgressBar value={progress} />
            <div className="flex items-center justify-between text-sm text-gray-500">
              <span>{quote?.route.distance_km} km</span>
              <span>{quote?.price.amount_eur.toFixed(2)} EUR</span>
            </div>
          </div>
        )}

        {state === 'COMPLETED' && (
          <div className="space-y-4">
            <div className="text-center">
              <div className="text-xl font-semibold text-gray-900 mb-1">Trip completed</div>
              <div className="text-sm text-gray-400">Thanks for riding!</div>
            </div>
            {summary && (
              <div className="grid grid-cols-3 gap-4 py-3 border-y border-gray-100">
                <div className="text-center">
                  <div className="text-xs text-gray-400">Distance</div>
                  <div className="font-semibold text-gray-900">{summary.distance_km} km</div>
                </div>
                <div className="text-center">
                  <div className="text-xs text-gray-400">Duration</div>
                  <div className="font-semibold text-gray-900">{summary.duration_min} min</div>
                </div>
                <div className="text-center">
                  <div className="text-xs text-gray-400">Price</div>
                  <div className="font-semibold text-gray-900">{summary.price_eur.toFixed(2)} EUR</div>
                </div>
              </div>
            )}
            <button
              onClick={onNewRide}
              className="w-full px-4 py-3 rounded-xl bg-black text-white font-medium text-sm hover:bg-gray-800 transition-colors"
            >
              New Ride
            </button>
          </div>
        )}

        {state === 'CANCELLED' && (
          <div className="space-y-4 text-center py-2">
            <div className="text-gray-900 font-medium">Ride cancelled</div>
            <button
              onClick={onNewRide}
              className="w-full px-4 py-3 rounded-xl bg-black text-white font-medium text-sm hover:bg-gray-800 transition-colors"
            >
              New Ride
            </button>
          </div>
        )}

        {canCancel && (
          <button
            onClick={onCancel}
            className="w-full mt-3 px-4 py-2.5 rounded-xl border border-gray-200 text-gray-500 text-sm hover:bg-gray-50 transition-colors"
          >
            Cancel ride
          </button>
        )}
      </div>
    </div>
  );
}
