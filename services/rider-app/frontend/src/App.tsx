import { useState, useCallback } from 'react';
import { useRideState } from './hooks/useRideState.ts';
import { useWebSocket } from './api/websocket.ts';
import RideMap from './components/RideMap.tsx';
import LocationPicker from './components/LocationPicker.tsx';
import QuoteCard from './components/QuoteCard.tsx';
import RideProgress from './components/RideProgress.tsx';
import DemandToggle from './components/DemandToggle.tsx';
import type { DemandHexagon, DemandUpdate, LatLng } from './types.ts';

export default function App() {
  const {
    ride, startPicking, setPickup, setDropoff,
    doRequestRide, doCancel, handleRideUpdate, reset,
  } = useRideState();

  const [demandHexagons, setDemandHexagons] = useState<DemandHexagon[]>([]);
  const [showDemand, setShowDemand] = useState(false);

  const onDemandUpdate = useCallback((update: DemandUpdate) => {
    setDemandHexagons(update.hexagons);
  }, []);

  const { subscribeRide } = useWebSocket(handleRideUpdate, onDemandUpdate);

  // Start picking on first render
  if (ride.state === 'IDLE') {
    startPicking();
  }

  const handleMapClick = useCallback((latlng: LatLng) => {
    if (ride.state === 'PICKING_PICKUP') {
      setPickup(latlng);
    } else if (ride.state === 'PICKING_DROPOFF') {
      setDropoff(latlng);
    }
  }, [ride.state, setPickup, setDropoff]);

  const handleRequestRide = useCallback(() => {
    doRequestRide(subscribeRide);
  }, [doRequestRide, subscribeRide]);

  const routeGeometry = ride.quote?.route?.geometry ?? null;

  return (
    <div className="relative w-full h-full">
      <RideMap
        state={ride.state}
        pickup={ride.pickup}
        dropoff={ride.dropoff}
        routeGeometry={routeGeometry}
        driver={ride.driver}
        bearing={ride.bearing}
        demandHexagons={demandHexagons}
        showDemand={showDemand}
        onMapClick={handleMapClick}
      />

      <LocationPicker
        state={ride.state}
        pickup={ride.pickup}
        loading={ride.loading}
        error={ride.error}
      />

      {ride.state === 'QUOTED' && ride.quote && (
        <QuoteCard
          quote={ride.quote}
          onRequest={handleRequestRide}
          onChangeRoute={reset}
          loading={ride.loading}
        />
      )}

      <RideProgress
        state={ride.state}
        driver={ride.driver}
        progress={ride.progress}
        etaSeconds={ride.etaSeconds}
        summary={ride.summary}
        quote={ride.quote}
        onCancel={doCancel}
        onNewRide={reset}
      />

      <DemandToggle
        enabled={showDemand}
        onToggle={() => setShowDemand(s => !s)}
      />
    </div>
  );
}
