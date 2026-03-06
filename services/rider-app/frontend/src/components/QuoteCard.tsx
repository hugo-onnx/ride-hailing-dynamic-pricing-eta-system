import type { Quote } from '../types.ts';

interface Props {
  quote: Quote;
  onRequest: () => void;
  onChangeRoute: () => void;
  loading: boolean;
}

export default function QuoteCard({ quote, onRequest, onChangeRoute, loading }: Props) {
  const surgeActive = quote.price.multiplier > 1.0;

  return (
    <div className="absolute bottom-0 left-0 right-0 z-10 animate-slide-up">
      <div className="bg-white rounded-t-3xl shadow-2xl px-6 pt-5 pb-6 max-w-lg mx-auto">
        {/* Route summary */}
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none" className="text-gray-400">
              <path d="M4 10h12M12 6l4 4-4 4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
            <span className="text-sm text-gray-500">
              {quote.route.distance_km} km
            </span>
          </div>
          {surgeActive && (
            <span className="text-xs font-medium px-2.5 py-1 rounded-full bg-amber-100 text-amber-700">
              {quote.price.multiplier.toFixed(1)}x surge
            </span>
          )}
        </div>

        {/* Stats grid */}
        <div className="grid grid-cols-3 gap-4 mb-5">
          <div>
            <div className="text-xs text-gray-400 uppercase tracking-wide">Pickup ETA</div>
            <div className="text-xl font-semibold text-gray-900">
              {Math.ceil(quote.eta.pickup_seconds / 60)}
              <span className="text-sm font-normal text-gray-400 ml-1">min</span>
            </div>
          </div>
          <div>
            <div className="text-xs text-gray-400 uppercase tracking-wide">Trip time</div>
            <div className="text-xl font-semibold text-gray-900">
              {Math.ceil(quote.eta.dropoff_seconds / 60)}
              <span className="text-sm font-normal text-gray-400 ml-1">min</span>
            </div>
          </div>
          <div>
            <div className="text-xs text-gray-400 uppercase tracking-wide">Price</div>
            <div className="text-xl font-semibold text-gray-900">
              {quote.price.amount_eur.toFixed(2)}
              <span className="text-sm font-normal text-gray-400 ml-1">EUR</span>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="flex gap-3">
          <button
            onClick={onChangeRoute}
            className="flex-1 px-4 py-3 rounded-xl border border-gray-200 text-gray-700 font-medium text-sm hover:bg-gray-50 transition-colors"
          >
            Change route
          </button>
          <button
            onClick={onRequest}
            disabled={loading}
            className="flex-[2] px-4 py-3 rounded-xl bg-black text-white font-medium text-sm hover:bg-gray-800 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
          >
            {loading ? (
              <>
                <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                Requesting...
              </>
            ) : (
              'Request Ride'
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
