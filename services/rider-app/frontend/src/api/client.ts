import type { Quote, LatLng } from '../types.ts';

const BASE = '';

export async function fetchQuote(origin: LatLng, dest: LatLng): Promise<Quote> {
  const params = new URLSearchParams({
    origin_lat: String(origin.lat),
    origin_lng: String(origin.lng),
    dest_lat: String(dest.lat),
    dest_lng: String(dest.lng),
  });
  const res = await fetch(`${BASE}/api/quote?${params}`);
  if (!res.ok) throw new Error(`Quote failed: ${res.status}`);
  return res.json();
}

export async function requestRide(
  origin: LatLng,
  dest: LatLng,
): Promise<{ ride_id: string; state: string }> {
  const params = new URLSearchParams({
    origin_lat: String(origin.lat),
    origin_lng: String(origin.lng),
    dest_lat: String(dest.lat),
    dest_lng: String(dest.lng),
  });
  const res = await fetch(`${BASE}/api/ride/request?${params}`, { method: 'POST' });
  if (!res.ok) throw new Error(`Ride request failed: ${res.status}`);
  return res.json();
}

export async function cancelRide(rideId: string): Promise<void> {
  await fetch(`${BASE}/api/ride/${rideId}/cancel`, { method: 'POST' });
}
