import { useState } from 'react'
import DatePicker from 'react-datepicker'
import 'react-datepicker/dist/react-datepicker.css'
import { format, parseISO } from 'date-fns'

const toDate = (str) => {
  if (!str) return null
  try { return parseISO(str) } catch { return null }
}

const toStr = (d) => d ? format(d, 'yyyy-MM-dd') : ''

export default function DateRangePicker({ value, onChange, onClear, placeholder = 'Pick date range…' }) {
  const [open, setOpen] = useState(false)

  const startDate = toDate(value?.[0])
  const endDate   = toDate(value?.[1])
  const hasValue  = !!(startDate || endDate)

  const handleChange = ([start, end]) => {
    onChange([toStr(start), toStr(end)])
    // Close after both dates picked
    if (start && end) setOpen(false)
  }

  const displayValue = hasValue
    ? [value?.[0], value?.[1]].filter(Boolean).join(' → ')
    : ''

  return (
    <div className="relative">
      {/* Custom input trigger */}
      <div className="relative flex items-center">
        <svg className="w-4 h-4 text-gray-400 absolute left-3 pointer-events-none z-10"
          fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
            d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
        </svg>
        <input
          readOnly
          value={displayValue}
          placeholder={placeholder}
          onClick={() => setOpen(!open)}
          className="input pl-9 pr-8 cursor-pointer w-64 bg-white"
        />
        {hasValue && (
          <button
            type="button"
            onClick={(e) => { e.stopPropagation(); onClear?.(); setOpen(false) }}
            className="absolute right-2.5 text-gray-400 hover:text-gray-600 transition-colors z-10"
          >
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}
      </div>

      {/* DatePicker in portal mode */}
      {open && (
        <div className="absolute z-50 mt-1">
          <DatePicker
            selected={startDate}
            onChange={handleChange}
            startDate={startDate}
            endDate={endDate}
            selectsRange
            inline
            calendarClassName="xh-calendar"
            onClickOutside={() => setOpen(false)}
          />
        </div>
      )}

      <style>{`
        .xh-calendar { font-family: Inter, system-ui, sans-serif; border-radius: 12px; border: 1px solid #e5e7eb; box-shadow: 0 10px 40px rgba(0,0,0,0.12); }
        .xh-calendar .react-datepicker__header { background: #0f2d1a; border-radius: 12px 12px 0 0; border-bottom: none; padding: 12px; }
        .xh-calendar .react-datepicker__current-month { color: white; font-weight: 600; }
        .xh-calendar .react-datepicker__day-name { color: rgba(255,255,255,0.6); font-size: 11px; }
        .xh-calendar .react-datepicker__navigation-icon::before { border-color: white; }
        .xh-calendar .react-datepicker__navigation:hover .react-datepicker__navigation-icon::before { border-color: #86efac; }
        .xh-calendar .react-datepicker__day--selected,
        .xh-calendar .react-datepicker__day--range-start,
        .xh-calendar .react-datepicker__day--range-end { background: #1e7a38 !important; color: white !important; border-radius: 50% !important; }
        .xh-calendar .react-datepicker__day--in-range { background: #e8f5ec; color: #1e7a38; border-radius: 0; }
        .xh-calendar .react-datepicker__day--range-start { border-radius: 50% 0 0 50% !important; }
        .xh-calendar .react-datepicker__day--range-end { border-radius: 0 50% 50% 0 !important; }
        .xh-calendar .react-datepicker__day:hover { background: #f0f9f2; border-radius: 50%; }
        .xh-calendar .react-datepicker__day--today { font-weight: 700; color: #1e7a38; }
        .xh-calendar .react-datepicker__day--keyboard-selected { background: #e8f5ec; color: #1e7a38; }
        .xh-calendar .react-datepicker__month { margin: 8px; }
      `}</style>
    </div>
  )
}
