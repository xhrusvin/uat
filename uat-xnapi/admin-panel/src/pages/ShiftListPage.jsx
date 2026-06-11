import { useEffect, useState, useRef } from 'react'
import { useShiftsStore } from '../store/shiftsStore'
import { shiftsService } from '../services/shiftsService'
import DateRangePicker from '../components/DateRangePicker'

function StatusBadge({ status }) {
  const s = (status || '').toLowerCase()
  const cls = s === 'open'      ? 'badge-enabled'
    : s === 'filled'  ? 'badge-default'
    : s === 'cancelled' || s === 'canceled' ? 'badge-disabled'
    : 'badge-default'
  return <span className={cls}>{status || '—'}</span>
}

function ShiftRow({ shift }) {
  const [expanded, setExpanded] = useState(false)

  return (
    <>
      <tr
        className="hover:bg-gray-50 cursor-pointer transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <td className="px-5 py-3.5 text-gray-700 font-medium text-sm">
          {shift.date || shift.shift_date || '—'}
        </td>
        <td className="px-5 py-3.5 text-gray-600 text-sm">
          {shift.title || shift.name || shift.shift_title || '—'}
        </td>
        <td className="px-5 py-3.5 text-gray-500 text-sm">
          {shift.location?.name || shift.location || shift.area || '—'}
        </td>
        <td className="px-5 py-3.5 text-gray-500 text-sm">
          {shift.start_time || shift.time || '—'}
          {shift.end_time ? ` – ${shift.end_time}` : ''}
        </td>
        <td className="px-5 py-3.5 text-sm">
          <StatusBadge status={shift.status} />
        </td>
        <td className="px-5 py-3.5 text-gray-400 text-sm">
          {shift.workers_needed || shift.slots || '—'}
        </td>
        <td className="px-5 py-3.5 text-right text-gray-400">
          <svg className={`w-4 h-4 inline transition-transform ${expanded ? 'rotate-90' : ''}`}
            fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
        </td>
      </tr>
      {expanded && (
        <tr className="bg-gray-50">
          <td colSpan={7} className="px-6 py-4">
            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">
                Raw Shift Data
              </h4>
              <pre className="text-xs text-gray-700 overflow-auto max-h-48 bg-gray-50 rounded p-3 font-mono">
                {JSON.stringify(shift, null, 2)}
              </pre>
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

function ShiftPagination({ page, perPage, total }) {
  const totalPages = Math.ceil(total / perPage)
  if (totalPages <= 1) return null

  const pages = []
  for (let i = Math.max(1, page - 2); i <= Math.min(totalPages, page + 2); i++) pages.push(i)

  return (
    <div className="flex items-center justify-between px-5 py-3 border-t border-gray-200">
      <p className="text-sm text-gray-500">
        Showing <span className="font-medium">{(page - 1) * perPage + 1}</span>–
        <span className="font-medium">{Math.min(page * perPage, total)}</span>
        {' of '}<span className="font-medium">{total}</span>
      </p>
      <div className="flex items-center gap-1">
        <button onClick={() => shiftsService.setPage(page - 1)} disabled={page === 1}
                className="px-2 py-1 rounded text-sm text-gray-600 hover:bg-gray-100 disabled:opacity-40">‹</button>
        {pages[0] > 1 && <button onClick={() => shiftsService.setPage(1)} className="px-3 py-1 rounded text-sm text-gray-600 hover:bg-gray-100">1</button>}
        {pages[0] > 2 && <span className="text-gray-400 px-1">…</span>}
        {pages.map((p) => (
          <button key={p} onClick={() => shiftsService.setPage(p)}
                  className={`px-3 py-1 rounded text-sm font-medium ${p === page ? 'text-white' : 'text-gray-600 hover:bg-gray-100'}`}
                  style={p === page ? { backgroundColor: '#1e7a38' } : {}}>
            {p}
          </button>
        ))}
        {pages[pages.length - 1] < totalPages - 1 && <span className="text-gray-400 px-1">…</span>}
        {pages[pages.length - 1] < totalPages && (
          <button onClick={() => shiftsService.setPage(totalPages)} className="px-3 py-1 rounded text-sm text-gray-600 hover:bg-gray-100">{totalPages}</button>
        )}
        <button onClick={() => shiftsService.setPage(page + 1)} disabled={page === totalPages}
                className="px-2 py-1 rounded text-sm text-gray-600 hover:bg-gray-100 disabled:opacity-40">›</button>
      </div>
    </div>
  )
}

export default function ShiftListPage() {
  const shifts    = useShiftsStore((s) => s.shifts)
  const total     = useShiftsStore((s) => s.total)
  const page      = useShiftsStore((s) => s.page)
  const perPage   = useShiftsStore((s) => s.perPage)
  const startDate = useShiftsStore((s) => s.startDate)
  const endDate   = useShiftsStore((s) => s.endDate)
  const search    = useShiftsStore((s) => s.search)
  const sortOrder = useShiftsStore((s) => s.sortOrder)
  const loading   = useShiftsStore((s) => s.loading)
  const error     = useShiftsStore((s) => s.error)

  const [searchInput, setSearchInput] = useState(search)
  const debounceRef = useRef(null)

  // Default to current month on first load
  useEffect(() => {
    const now   = new Date()
    const start = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`
    const last  = new Date(now.getFullYear(), now.getMonth() + 1, 0)
    const end   = `${last.getFullYear()}-${String(last.getMonth() + 1).padStart(2, '0')}-${String(last.getDate()).padStart(2, '0')}`
    useShiftsStore.getState().setDates(start, end)
    shiftsService.fetch({ startDate: start, endDate: end })
  }, [])

  const handleSearch = (val) => {
    setSearchInput(val)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => shiftsService.setSearch(val), 500)
  }

  const handleDateChange = ([from, to]) => {
    if (from && to) shiftsService.setDates(from, to)
  }

  const handleDateClear = () => shiftsService.setDates('', '')

  return (
    <div className="p-8">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-2 text-sm text-gray-400 mb-1">
          <span>XN API Calls</span>
          <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
          <span>Shift List</span>
        </div>
        <h1 className="text-2xl font-bold text-gray-900">Shift List</h1>
        <p className="text-sm text-gray-500 mt-0.5">
          {loading ? 'Loading…' : `${total} shift${total !== 1 ? 's' : ''} from XpressHealth Shift API`}
        </p>
      </div>

      {/* Filters */}
      <div className="card mb-5 p-4">
        <div className="flex flex-wrap gap-3 items-center">

          {/* Search */}
          <div className="relative flex-1 min-w-48">
            <svg className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2"
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input type="text" className="input pl-9" placeholder="Search shifts…"
              value={searchInput} onChange={(e) => handleSearch(e.target.value)} />
          </div>

          {/* Date range */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-500 whitespace-nowrap">Date</span>
            <DateRangePicker
              value={[startDate, endDate]}
              onChange={handleDateChange}
              onClear={handleDateClear}
              placeholder="Pick date range…"
            />
          </div>

          {/* Sort order */}
          <select value={sortOrder} onChange={(e) => shiftsService.setSortOrder(e.target.value)}
                  className="input w-32 py-1.5">
            <option value="desc">Newest first</option>
            <option value="asc">Oldest first</option>
          </select>

          {/* Per page */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-500 whitespace-nowrap">Show</span>
            <select value={perPage} onChange={(e) => shiftsService.setPerPage(Number(e.target.value))}
                    className="input w-20 py-1.5">
              {[10, 20, 50, 100].map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>

          {/* Refresh */}
          <button onClick={() => shiftsService.refresh()} disabled={loading}
                  className="btn-secondary flex items-center gap-2 py-2">
            <svg className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`}
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Refresh
          </button>
        </div>

        {(startDate || endDate) && (
          <div className="flex gap-2 mt-3 pt-3 border-t border-gray-100">
            <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs bg-purple-50 text-purple-700 font-medium">
              {startDate || '…'} → {endDate || '…'}
            </span>
          </div>
        )}
      </div>

      {/* Error */}
      {error && (
        <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg flex items-center justify-between text-sm text-red-700">
          <span>{error}</span>
          <button onClick={() => shiftsService.refresh()} className="ml-4 font-medium underline">Retry</button>
        </div>
      )}

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50">
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Date</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Title</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Location</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Time</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Slots</th>
                <th className="px-5 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {loading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i}>{Array.from({ length: 7 }).map((_, j) => (
                    <td key={j} className="px-5 py-3.5">
                      <div className="h-4 bg-gray-100 rounded animate-pulse" />
                    </td>
                  ))}</tr>
                ))
              ) : !error && shifts.length === 0 ? (
                <tr><td colSpan={7} className="px-5 py-16 text-center">
                  <svg className="w-12 h-12 mx-auto mb-3 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                      d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
                  </svg>
                  <p className="text-sm text-gray-400">No shifts found for the selected period</p>
                </td></tr>
              ) : (
                shifts.map((shift, idx) => (
                  <ShiftRow key={shift._id || shift.id || idx} shift={shift} />
                ))
              )}
            </tbody>
          </table>
        </div>
        <ShiftPagination page={page} perPage={perPage} total={total} />
      </div>
    </div>
  )
}
