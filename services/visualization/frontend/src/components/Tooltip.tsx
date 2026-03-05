import { useState, useRef, useCallback } from 'react'
import { createPortal } from 'react-dom'
import type { TooltipPosition } from '../types'

/**
 * Portal-based tooltip — renders at document.body so it's never clipped
 * by overflow:hidden / overflow:auto ancestors.
 *
 * Usage: <Tooltip text="Explanation"><YourElement /></Tooltip>
 * position: 'top' (default) | 'right' | 'left'
 */
interface TooltipProps {
  text: string
  children: React.ReactNode
  position?: TooltipPosition
}

export default function Tooltip({ text, children, position = 'top' }: TooltipProps) {
  const [visible, setVisible] = useState(false)
  const [coords, setCoords] = useState({ x: 0, y: 0 })
  const triggerRef = useRef<HTMLSpanElement>(null)

  const show = useCallback(() => {
    const rect = triggerRef.current?.getBoundingClientRect()
    if (!rect) return
    const TIP_W = 208 // w-52 = 13rem = 208px
    const MARGIN = 8

    if (position === 'right') {
      setCoords({ x: rect.right + MARGIN, y: rect.top + rect.height / 2 })
    } else if (position === 'left') {
      setCoords({ x: rect.left - MARGIN, y: rect.top + rect.height / 2 })
    } else {
      // Center above trigger, then clamp to stay inside viewport
      const raw = rect.left + rect.width / 2
      const clamped = Math.min(
        Math.max(raw, TIP_W / 2 + MARGIN),
        window.innerWidth - TIP_W / 2 - MARGIN,
      )
      setCoords({ x: clamped, y: rect.top - MARGIN })
    }
    setVisible(true)
  }, [position])

  const hide = useCallback(() => setVisible(false), [])

  const tooltipStyle: React.CSSProperties =
    position === 'right'
      ? { left: coords.x, top: coords.y, transform: 'translateY(-50%)' }
      : position === 'left'
        ? { left: coords.x, top: coords.y, transform: 'translate(-100%, -50%)' }
        : { left: coords.x, top: coords.y, transform: 'translate(-50%, -100%)' }

  const arrow =
    position === 'right' ? (
      <span className="absolute top-1/2 -translate-y-1/2 -left-1.5 border-4 border-solid border-y-transparent border-l-transparent border-r-night-800" />
    ) : position === 'left' ? (
      <span className="absolute top-1/2 -translate-y-1/2 -right-1.5 border-4 border-solid border-y-transparent border-r-transparent border-l-night-800" />
    ) : (
      <span className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-solid border-x-transparent border-b-transparent border-t-night-800" />
    )

  return (
    <>
      <span
        ref={triggerRef}
        className="inline-flex"
        onMouseEnter={show}
        onMouseLeave={hide}
        onFocus={show}
        onBlur={hide}
      >
        {children}
      </span>

      {visible &&
        createPortal(
          <div
            className="pointer-events-none fixed z-[9999] w-52 rounded-lg bg-night-800 border border-night-700/60 px-3 py-2 text-[11px] leading-relaxed text-night-300 shadow-xl"
            style={tooltipStyle}
          >
            {text}
            {arrow}
          </div>,
          document.body,
        )}
    </>
  )
}
