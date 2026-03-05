import { useState, useEffect, useRef, useCallback } from 'react'
import type { Hexagon, Stats, WindowMinutes } from '../types'

const WS_RECONNECT_DELAY = 3000
const WS_PING_INTERVAL = 25000

interface UseWebSocketReturn {
  data: Hexagon[] | null
  stats: Stats | null
  isConnected: boolean
  error: string | null
  lastUpdate: Date | null
  requestUpdate: (window?: WindowMinutes) => void
  reconnect: () => void
}

export function useWebSocket(url: string): UseWebSocketReturn {
  const [data, setData] = useState<Hexagon[] | null>(null)
  const [stats, setStats] = useState<Stats | null>(null)
  const [isConnected, setIsConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null)

  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pingIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return
    }

    try {
      const ws = new WebSocket(url)
      wsRef.current = ws

      ws.onopen = () => {
        console.log('[WS] Connected')
        setIsConnected(true)
        setError(null)

        // Start ping interval
        pingIntervalRef.current = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'ping' }))
          }
        }, WS_PING_INTERVAL)
      }

      ws.onmessage = (event: MessageEvent<string>) => {
        try {
          const message = JSON.parse(event.data) as {
            type: string
            hexagons?: Hexagon[]
            stats?: Stats
            timestamp: string
          }

          if (message.type === 'initial' || message.type === 'demand_update') {
            setData(message.hexagons ?? [])
            setStats(message.stats ?? null)
            setLastUpdate(new Date(message.timestamp))
          }
        } catch (e) {
          console.error('[WS] Parse error:', e)
        }
      }

      ws.onerror = (event: Event) => {
        console.error('[WS] Error:', event)
        setError('Connection error')
      }

      ws.onclose = (event: CloseEvent) => {
        console.log('[WS] Disconnected:', event.code, event.reason)
        setIsConnected(false)

        // Clear ping interval
        if (pingIntervalRef.current) {
          clearInterval(pingIntervalRef.current)
        }

        // Attempt reconnect
        if (!event.wasClean) {
          reconnectTimeoutRef.current = setTimeout(() => {
            console.log('[WS] Attempting reconnect...')
            connect()
          }, WS_RECONNECT_DELAY)
        }
      }
    } catch (e) {
      console.error('[WS] Connection failed:', e)
      setError('Failed to connect')

      // Retry connection
      reconnectTimeoutRef.current = setTimeout(connect, WS_RECONNECT_DELAY)
    }
  }, [url])

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current)
    }
    if (pingIntervalRef.current) {
      clearInterval(pingIntervalRef.current)
    }
    if (wsRef.current) {
      wsRef.current.close(1000, 'Client disconnect')
      wsRef.current = null
    }
  }, [])

  const requestUpdate = useCallback((window: WindowMinutes = 5) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'request_update', window }))
    }
  }, [])

  useEffect(() => {
    connect()
    return () => disconnect()
  }, [connect, disconnect])

  return {
    data,
    stats,
    isConnected,
    error,
    lastUpdate,
    requestUpdate,
    reconnect: connect,
  }
}
