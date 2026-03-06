import { useEffect, useRef, useCallback, useState } from 'react';
import type { RideUpdate, DemandUpdate } from '../types.ts';

type WSMessage = RideUpdate | DemandUpdate | { type: string };

export function useWebSocket(
  onRideUpdate: (update: RideUpdate) => void,
  onDemandUpdate: (update: DemandUpdate) => void,
) {
  const wsRef = useRef<WebSocket | null>(null);
  const [connected, setConnected] = useState(false);
  const onRideRef = useRef(onRideUpdate);
  const onDemandRef = useRef(onDemandUpdate);
  onRideRef.current = onRideUpdate;
  onDemandRef.current = onDemandUpdate;

  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${protocol}//${window.location.host}/ws`;
    let ws: WebSocket;
    let reconnectTimer: ReturnType<typeof setTimeout>;

    function connect() {
      ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        reconnectTimer = setTimeout(connect, 3000);
      };
      ws.onerror = () => ws.close();
      ws.onmessage = (event) => {
        try {
          const msg: WSMessage = JSON.parse(event.data);
          if (msg.type === 'ride_update') {
            onRideRef.current(msg as RideUpdate);
          } else if (msg.type === 'demand_update') {
            onDemandRef.current(msg as DemandUpdate);
          }
        } catch {
          // ignore parse errors
        }
      };
    }

    connect();

    // Keepalive
    const pingInterval = setInterval(() => {
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        wsRef.current.send(JSON.stringify({ type: 'ping' }));
      }
    }, 25000);

    return () => {
      clearInterval(pingInterval);
      clearTimeout(reconnectTimer);
      ws?.close();
    };
  }, []);

  const subscribeRide = useCallback((rideId: string) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'subscribe_ride', ride_id: rideId }));
    }
  }, []);

  return { connected, subscribeRide };
}
