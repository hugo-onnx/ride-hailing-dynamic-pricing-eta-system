import { useState, useCallback, useRef } from 'react';
import type { RideState, LatLng, Quote, RideUpdate, DriverInfo } from '../types.ts';
import { fetchQuote, requestRide, cancelRide } from '../api/client.ts';

interface RideData {
  state: RideState;
  pickup: LatLng | null;
  dropoff: LatLng | null;
  quote: Quote | null;
  rideId: string | null;
  driver: DriverInfo | null;
  bearing: number;
  progress: number;
  etaSeconds: number;
  summary: { distance_km: number; duration_min: number; price_eur: number } | null;
  loading: boolean;
  error: string | null;
}

const INITIAL: RideData = {
  state: 'IDLE',
  pickup: null,
  dropoff: null,
  quote: null,
  rideId: null,
  driver: null,
  bearing: 0,
  progress: 0,
  etaSeconds: 0,
  summary: null,
  loading: false,
  error: null,
};

export function useRideState() {
  const [ride, setRide] = useState<RideData>(INITIAL);
  const rideRef = useRef(ride);
  rideRef.current = ride;

  const startPicking = useCallback(() => {
    setRide({ ...INITIAL, state: 'PICKING_PICKUP' });
  }, []);

  const setPickup = useCallback((latlng: LatLng) => {
    setRide(r => ({ ...r, pickup: latlng, state: 'PICKING_DROPOFF' }));
  }, []);

  const setDropoff = useCallback(async (latlng: LatLng) => {
    const pickup = rideRef.current.pickup;
    if (!pickup) return;

    setRide(r => ({ ...r, dropoff: latlng, loading: true, error: null }));

    try {
      const quote = await fetchQuote(pickup, latlng);
      setRide(r => ({ ...r, quote, state: 'QUOTED', loading: false }));
    } catch (e) {
      setRide(r => ({
        ...r,
        error: e instanceof Error ? e.message : 'Failed to get quote',
        state: 'PICKING_DROPOFF',
        loading: false,
      }));
    }
  }, []);

  const doRequestRide = useCallback(async (subscribeRide: (id: string) => void) => {
    const { pickup, dropoff } = rideRef.current;
    if (!pickup || !dropoff) return;

    setRide(r => ({ ...r, state: 'REQUESTING', loading: true }));

    try {
      const result = await requestRide(pickup, dropoff);
      setRide(r => ({ ...r, rideId: result.ride_id, loading: false }));
      subscribeRide(result.ride_id);
    } catch (e) {
      setRide(r => ({
        ...r,
        error: e instanceof Error ? e.message : 'Failed to request ride',
        state: 'QUOTED',
        loading: false,
      }));
    }
  }, []);

  const doCancel = useCallback(async () => {
    const { rideId } = rideRef.current;
    if (rideId) {
      await cancelRide(rideId);
    }
    setRide(r => ({ ...r, state: 'CANCELLED' }));
  }, []);

  const handleRideUpdate = useCallback((update: RideUpdate) => {
    setRide(r => {
      if (update.ride_id !== r.rideId) return r;
      const next = { ...r };
      next.state = update.state as RideState;
      if (update.driver) next.driver = update.driver;
      if (update.bearing !== undefined) next.bearing = update.bearing;
      if (update.progress !== undefined) next.progress = update.progress;
      if (update.eta_seconds !== undefined) next.etaSeconds = update.eta_seconds;
      if (update.summary) next.summary = update.summary;
      return next;
    });
  }, []);

  const reset = useCallback(() => {
    setRide({ ...INITIAL, state: 'PICKING_PICKUP' });
  }, []);

  return {
    ride,
    startPicking,
    setPickup,
    setDropoff,
    doRequestRide,
    doCancel,
    handleRideUpdate,
    reset,
  };
}
