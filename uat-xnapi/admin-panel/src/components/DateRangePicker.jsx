import { useEffect, useRef } from 'react'
import flatpickr from 'flatpickr'
import 'flatpickr/dist/flatpickr.min.css'

const fmt = (d) => {
  const y   = d.getFullYear()
  const m   = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

export default function DateRangePicker({ value, onChange, onClear, placeholder = 'Pick date range…' }) {
  const inputRef   = useRef(null)
  const fpRef      = useRef(null)
  const onChangeRef = useRef(onChange)   // keep latest callback without recreating flatpickr
  const onClearRef  = useRef(onClear)

  // Always update the refs so flatpickr always calls the latest handlers
  useEffect(() => { onChangeRef.current = onChange }, [onChange])
  useEffect(() => { onClearRef.current  = onClear  }, [onClear])

  // Init flatpickr once on mount only
  useEffect(() => {
    fpRef.current = flatpickr(inputRef.current, {
      mode: 'range',
      dateFormat: 'Y-m-d',
      defaultDate: (value || []).filter(Boolean),
      disableMobile: true,
      allowInput: false,
      onChange: (selectedDates) => {
        if (selectedDates.length === 2) {
          onChangeRef.current([fmt(selectedDates[0]), fmt(selectedDates[1])])
        } else if (selectedDates.length === 0) {
          onChangeRef.current(['', ''])
        }
        // length === 1 means user picked first date — wait for second, do nothing
      },
    })
    return () => fpRef.current?.destroy()
  }, []) // mount only — safe because we use refs for callbacks

  // Sync external clear (e.g. "Clear all" button)
  useEffect(() => {
    if (!fpRef.current) return
    const dates = (value || []).filter(Boolean)
    if (dates.length === 0) {
      fpRef.current.clear()
    } else if (dates.length === 2) {
      fpRef.current.setDate(dates, false) // false = don't trigger onChange
    }
  }, [value])

  const hasValue = (value || []).some(Boolean)

  return (
    <div className="relative">
      <div className="relative flex items-center">
        <svg className="w-4 h-4 text-gray-400 absolute left-3 pointer-events-none z-10"
          fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
            d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
        </svg>
        <input
          ref={inputRef}
          readOnly
          placeholder={placeholder}
          className="input pl-9 pr-8 cursor-pointer w-64 bg-white"
        />
        {hasValue && (
          <button
            type="button"
            onClick={() => {
              fpRef.current?.clear()
              onClearRef.current?.()
            }}
            className="absolute right-2.5 text-gray-400 hover:text-gray-600 transition-colors"
            title="Clear dates"
          >
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}
      </div>

      <style>{`
        .flatpickr-day.selected,.flatpickr-day.startRange,.flatpickr-day.endRange,
        .flatpickr-day.selected:hover,.flatpickr-day.startRange:hover,.flatpickr-day.endRange:hover {
          background: #1e7a38 !important; border-color: #1e7a38 !important;
        }
        .flatpickr-day.inRange {
          background: #e8f5ec !important; border-color: #e8f5ec !important;
          box-shadow: -5px 0 0 #e8f5ec, 5px 0 0 #e8f5ec;
        }
        .flatpickr-day:hover { background: #f0f9f2 !important; }
        .flatpickr-day.today { border-color: #1e7a38 !important; }
        .flatpickr-months .flatpickr-month,
        .flatpickr-current-month .flatpickr-monthDropdown-months,
        .flatpickr-weekdays { background: #0f2d1a !important; color: white !important; }
        .flatpickr-weekday { color: rgba(255,255,255,0.7) !important; background: #0f2d1a !important; }
        .flatpickr-prev-month svg,.flatpickr-next-month svg { fill: white !important; }
        .flatpickr-prev-month:hover svg,.flatpickr-next-month:hover svg { fill: #86efac !important; }
        .flatpickr-current-month input.cur-year,
        .flatpickr-current-month .flatpickr-monthDropdown-months { color: white !important; }
        .flatpickr-calendar {
          border-radius: 12px !important;
          box-shadow: 0 10px 40px rgba(0,0,0,0.12) !important;
          border: 1px solid #e5e7eb !important;
          font-family: Inter, system-ui, sans-serif !important;
        }
      `}</style>
    </div>
  )
}
