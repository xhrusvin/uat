import { useState, useEffect, useRef } from 'react'
import { useShiftsStore } from '../store/shiftsStore'
import { shiftsService } from '../services/shiftsService'
import DateRangePicker from '../components/DateRangePicker'

// ── Sync banner ───────────────────────────────────────────────────────────────
function SyncBanner({ sync, onDismiss }) {
  if (!sync) return null
  return (
    <div className="mb-5 px-4 py-3 bg-green-50 border border-green-200 rounded-lg flex items-center justify-between text-sm">
      <div className="flex items-center gap-3">
        <svg className="w-4 h-4 text-green-600 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
        </svg>
        <span className="text-green-800 font-medium">Synced to database:</span>
        <span className="text-green-700">
          <span className="font-semibold">{sync.fetched}</span> fetched —&nbsp;
          <span className="font-semibold text-green-600">{sync.inserted}</span> new,&nbsp;
          <span className="font-semibold text-blue-600">{sync.updated}</span> updated,&nbsp;
          <span className="font-semibold text-gray-500">{sync.skipped}</span> skipped
        </span>
      </div>
      <button onClick={onDismiss} className="text-green-400 hover:text-green-600 ml-4">
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  )
}

// ── Status badge ──────────────────────────────────────────────────────────────
const STATUS_COLORS = {
  'Upcoming':            'bg-blue-100 text-blue-700',
  'To Be Filled':        'bg-yellow-100 text-yellow-700',
  'Completed':           'bg-green-100 text-green-700',
  'Cancelled By Client': 'bg-red-100 text-red-600',
  'Cancelled By Staff':  'bg-red-100 text-red-600',
  'In Progress':         'bg-purple-100 text-purple-700',
}

function StatusBadge({ status }) {
  const cls = STATUS_COLORS[status] || 'bg-gray-100 text-gray-600'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>
      {status || '—'}
    </span>
  )
}

// ── Pagination ────────────────────────────────────────────────────────────────
function Pagination({ page, perPage, total }) {
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
        {pages[0] > 1 && <><button onClick={() => shiftsService.setPage(1)} className="px-3 py-1 rounded text-sm text-gray-600 hover:bg-gray-100">1</button>{pages[0] > 2 && <span className="text-gray-400 px-1">…</span>}</>}
        {pages.map(p => (
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

// ── Page ──────────────────────────────────────────────────────────────────────
export default function ShiftListPage() {
  const shifts     = useShiftsStore((s) => s.shifts)
  const total      = useShiftsStore((s) => s.total)
  const page       = useShiftsStore((s) => s.page)
  const perPage    = useShiftsStore((s) => s.perPage)
  const startDate  = useShiftsStore((s) => s.startDate)
  const endDate    = useShiftsStore((s) => s.endDate)
  const search     = useShiftsStore((s) => s.search)
  const sortOrder  = useShiftsStore((s) => s.sortOrder)
  const loading    = useShiftsStore((s) => s.loading)
  const error      = useShiftsStore((s) => s.error)
  const syncResult = useShiftsStore((s) => s.syncResult)

  // Local form state (user edits before clicking Fetch)
  const [localSearch,    setLocalSearch]    = useState(search)
  const [localPage,      setLocalPage]      = useState(1)
  const [localPerPage,   setLocalPerPage]   = useState(perPage)
  const [localSortOrder, setLocalSortOrder] = useState(sortOrder)
  const [localDateValue, setLocalDateValue] = useState([startDate, endDate])
  const [showSync,       setShowSync]       = useState(false)

  useEffect(() => { if (syncResult) setShowSync(true) }, [syncResult])

  // Default to current month on first mount
  useEffect(() => {
    const now   = new Date()
    const start = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`
    const last  = new Date(now.getFullYear(), now.getMonth() + 1, 0)
    const end   = `${last.getFullYear()}-${String(last.getMonth() + 1).padStart(2, '0')}-${String(last.getDate()).padStart(2, '0')}`
    setLocalDateValue([start, end])
  }, [])

  const handleFetch = () => {
    // Apply local form values to the service
    useShiftsStore.getState().setSearch(localSearch)
    useShiftsStore.getState().setDates(localDateValue[0] || '', localDateValue[1] || '')
    useShiftsStore.getState().setSortOrder(localSortOrder)
    useShiftsStore.getState().setPerPage(localPerPage)

    shiftsService.fetch({
      search:    localSearch,
      page:      localPage,
      perPage:   localPerPage,
      sortOrder: localSortOrder,
      startDate: localDateValue[0] || '',
      endDate:   localDateValue[1] || '',
    })
  }

  const handleDateChange = ([from, to]) => {
    if (from && to) setLocalDateValue([from, to])
  }

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API Calls</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>Shift List</span>
      </div>
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Shift List</h1>
      <p className="text-sm text-gray-500 mb-6">
        Fetch shifts from the XpressHealth Shift API and sync to database.
      </p>

      {/* ── Request config card ─────────────────────────────────────────────── */}
      <div className="card p-5 mb-6">
        <div className="flex items-center gap-2 mb-4">
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold bg-blue-100 text-blue-700 font-mono">POST</span>
          <code className="text-xs text-gray-600 bg-gray-100 px-3 py-1.5 rounded-lg font-mono">
            {`${import.meta.env.VITE_API_URL || ''}/shifts/list`}
          </code>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-4">
          {/* Search */}
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Search</label>
            <input type="text" className="input text-sm" placeholder="Search shifts…"
              value={localSearch} onChange={e => setLocalSearch(e.target.value)} />
          </div>

          {/* Page */}
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Page</label>
            <input type="number" className="input text-sm" min={1}
              value={localPage} onChange={e => setLocalPage(Number(e.target.value))} />
          </div>

          {/* Per Page */}
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Per Page</label>
            <select className="input text-sm" value={localPerPage}
                    onChange={e => setLocalPerPage(Number(e.target.value))}>
              {[10, 20, 50, 100, 200, 500].map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>

          {/* Sort By (fixed = date) */}
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Sort By</label>
            <input type="text" className="input text-sm bg-gray-50 text-gray-500" value="date" readOnly />
          </div>

          {/* Sort Order */}
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Sort Order</label>
            <select className="input text-sm" value={localSortOrder}
                    onChange={e => setLocalSortOrder(e.target.value)}>
              <option value="desc">Newest first</option>
              <option value="asc">Oldest first</option>
            </select>
          </div>

          {/* Date range */}
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Date Range</label>
            <DateRangePicker
              value={localDateValue}
              onChange={handleDateChange}
              onClear={() => setLocalDateValue(['', ''])}
              placeholder="Pick date range…"
            />
          </div>
        </div>

        <button onClick={handleFetch} disabled={loading}
                className="flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-medium text-white
                           transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                style={{ backgroundColor: '#1e7a38' }}>
          {loading
            ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
            : <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>}
          {loading ? 'Fetching & syncing…' : 'Fetch & Sync'}
        </button>
      </div>

      {/* ── Sync banner ─────────────────────────────────────────────────────── */}
      {showSync && syncResult && (
        <SyncBanner sync={syncResult} onDismiss={() => setShowSync(false)} />
      )}

      {/* ── Error ───────────────────────────────────────────────────────────── */}
      {error && (
        <div className="mb-5 px-4 py-4 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-start gap-2">
              <svg className="w-4 h-4 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
              </svg>
              <div>
                <p className="font-medium mb-0.5">Shift API Error</p>
                <p className="text-red-600">{error}</p>
              </div>
            </div>
            <button onClick={handleFetch} className="flex-shrink-0 font-medium underline hover:no-underline">Retry</button>
          </div>
        </div>
      )}

      {/* ── Results table ───────────────────────────────────────────────────── */}
      {shifts.length > 0 && (
        <div className="card overflow-hidden">
          {/* Table header info */}
          <div className="px-5 py-3.5 border-b border-gray-200 flex items-center gap-3">
            <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold font-mono bg-green-100 text-green-700">200</span>
            <span className="text-sm text-gray-600">Shift list</span>
            <span className="text-xs text-gray-400 ml-auto">Total: {total}</span>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 bg-gray-50">
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Code</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Date</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Location</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Timing</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Role</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Assigned</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Rate</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  Array.from({ length: 8 }).map((_, i) => (
                    <tr key={i}>{Array.from({ length: 8 }).map((_, j) => (
                      <td key={j} className="px-5 py-3.5">
                        <div className="h-4 bg-gray-100 rounded animate-pulse" />
                      </td>
                    ))}</tr>
                  ))
                ) : (
                  shifts.map((shift, idx) => (
                    <tr key={shift.shift_id || idx} className="hover:bg-gray-50 transition-colors">
                      <td className="px-5 py-3.5">
                        <span className="font-mono text-xs bg-gray-100 px-1.5 py-0.5 rounded text-gray-700">
                          {shift.shift_code || '—'}
                        </span>
                      </td>
                      <td className="px-5 py-3.5 text-gray-700 font-medium">{shift.date || '—'}</td>
                      <td className="px-5 py-3.5">
                        <p className="text-gray-700">{shift.location || '—'}</p>
                        {shift.client_county && <p className="text-xs text-gray-400">{shift.client_county}</p>}
                      </td>
                      <td className="px-5 py-3.5 text-gray-500 text-xs">{shift.shift_timing || '—'}</td>
                      <td className="px-5 py-3.5 text-gray-600">{shift.user_type || '—'}</td>
                      <td className="px-5 py-3.5">
                        {shift.assigned_staff
                          ? <div>
                              <p className="text-gray-700">{shift.assigned_staff}</p>
                              {shift.staff_email && <p className="text-xs text-gray-400">{shift.staff_email}</p>}
                            </div>
                          : <span className="text-gray-400 text-xs italic">Unassigned</span>}
                      </td>
                      <td className="px-5 py-3.5 text-gray-600">
                        {shift.pay_rate ? `€${shift.pay_rate}/hr` : '—'}
                      </td>
                      <td className="px-5 py-3.5">
                        <StatusBadge status={shift.status_name} />
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
          <Pagination page={page} perPage={perPage} total={total} />
        </div>
      )}

      {/* ── Empty state ─────────────────────────────────────────────────────── */}
      {!shifts.length && !loading && !error && (
        <div className="card p-12 text-center">
          <svg className="w-12 h-12 mx-auto mb-4 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          <p className="text-sm text-gray-400">
            Configure the request above then click <strong>Fetch & Sync</strong>
          </p>
        </div>
      )}
    </div>
  )
}
