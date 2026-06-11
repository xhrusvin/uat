import { useEffect, useState, useRef } from 'react'
import { useShiftsDbStore } from '../store/shiftsDbStore'
import { shiftsDbService } from '../services/shiftsDbService'
import ShiftDrawer from '../components/ShiftDrawer'
import DateRangePicker from '../components/DateRangePicker'

const STATUS_COLORS = {
  'Upcoming':       'bg-blue-100 text-blue-700',
  'To be assigned': 'bg-yellow-100 text-yellow-700',
  'Completed':      'bg-green-100 text-green-700',
  'Cancelled':      'bg-red-100 text-red-600',
  'In Progress':    'bg-purple-100 text-purple-700',
}

function StatusBadge({ status }) {
  const cls = STATUS_COLORS[status] || 'bg-gray-100 text-gray-600'
  return <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>{status || '—'}</span>
}

function Pagination({ page, perPage, total }) {
  const totalPages = Math.ceil(total / perPage)
  if (totalPages <= 1) return null
  const pages = []
  for (let i = Math.max(1, page - 2); i <= Math.min(totalPages, page + 2); i++) pages.push(i)
  return (
    <div className="flex items-center justify-between px-5 py-3 border-t border-gray-200">
      <p className="text-sm text-gray-500">
        Showing <span className="font-medium">{(page-1)*perPage+1}</span>–
        <span className="font-medium">{Math.min(page*perPage, total)}</span> of <span className="font-medium">{total}</span>
      </p>
      <div className="flex items-center gap-1">
        <button onClick={() => shiftsDbService.setPage(page-1)} disabled={page===1}
                className="px-2 py-1 rounded text-sm text-gray-600 hover:bg-gray-100 disabled:opacity-40">‹</button>
        {pages[0]>1 && <><button onClick={() => shiftsDbService.setPage(1)} className="px-3 py-1 rounded text-sm text-gray-600 hover:bg-gray-100">1</button>{pages[0]>2&&<span className="text-gray-400 px-1">…</span>}</>}
        {pages.map(p => (
          <button key={p} onClick={() => shiftsDbService.setPage(p)}
                  className={`px-3 py-1 rounded text-sm font-medium ${p===page?'text-white':'text-gray-600 hover:bg-gray-100'}`}
                  style={p===page?{backgroundColor:'#1e7a38'}:{}}>
            {p}
          </button>
        ))}
        {pages[pages.length-1]<totalPages-1&&<span className="text-gray-400 px-1">…</span>}
        {pages[pages.length-1]<totalPages&&<button onClick={()=>shiftsDbService.setPage(totalPages)} className="px-3 py-1 rounded text-sm text-gray-600 hover:bg-gray-100">{totalPages}</button>}
        <button onClick={() => shiftsDbService.setPage(page+1)} disabled={page===totalPages}
                className="px-2 py-1 rounded text-sm text-gray-600 hover:bg-gray-100 disabled:opacity-40">›</button>
      </div>
    </div>
  )
}

const STATUSES = ['Upcoming', 'To be assigned', 'Completed', 'Cancelled', 'In Progress']

export default function ShiftsPage() {
  const shifts    = useShiftsDbStore((s) => s.shifts)
  const total     = useShiftsDbStore((s) => s.total)
  const page      = useShiftsDbStore((s) => s.page)
  const perPage   = useShiftsDbStore((s) => s.perPage)
  const search    = useShiftsDbStore((s) => s.search)
  const status    = useShiftsDbStore((s) => s.status)
  const dateFrom  = useShiftsDbStore((s) => s.dateFrom)
  const dateTo    = useShiftsDbStore((s) => s.dateTo)
  const loading   = useShiftsDbStore((s) => s.loading)
  const error     = useShiftsDbStore((s) => s.error)

  const [searchInput, setSearchInput] = useState(search)
  const [selectedId, setSelectedId]   = useState(null)
  const debounceRef                   = useRef(null)

  useEffect(() => { shiftsDbService.init() }, [])

  const handleSearch = (val) => {
    setSearchInput(val)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => shiftsDbService.setSearch(val), 500)
  }

  const hasFilters = search || status || dateFrom || dateTo

  return (
    <div className="p-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Shifts</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {loading ? 'Loading…' : `${total} shift${total!==1?'s':''} from database`}
          </p>
        </div>
      </div>

      {/* Filters */}
      <div className="card mb-5 p-4">
        <div className="flex flex-wrap gap-3 items-center">

          {/* Search */}
          <div className="relative flex-1 min-w-48">
            <svg className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2"
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
            </svg>
            <input type="text" className="input pl-9" placeholder="Search code, location, staff…"
              value={searchInput} onChange={(e) => handleSearch(e.target.value)} />
          </div>

          {/* Status */}
          <select value={status} onChange={(e) => shiftsDbService.setStatus(e.target.value)}
                  className="input w-44 py-1.5">
            <option value="">All statuses</option>
            {STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
          </select>

          {/* Date range */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-500 whitespace-nowrap">Date</span>
            <DateRangePicker
              value={[dateFrom, dateTo]}
              onChange={([f, t]) => { if (f && t) shiftsDbService.setDates(f, t) }}
              onClear={() => shiftsDbService.setDates('', '')}
              placeholder="Pick date range…"
            />
          </div>

          {hasFilters && (
            <button onClick={() => { setSearchInput(''); shiftsDbService.clearFilters() }}
                    className="btn-secondary flex items-center gap-1.5 text-sm">
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
              </svg>
              Clear
            </button>
          )}

          <div className="flex-1" />

          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-500 whitespace-nowrap">Show</span>
            <select value={perPage} onChange={(e) => shiftsDbService.setPerPage(Number(e.target.value))}
                    className="input w-20 py-1.5">
              {[10,20,50,100].map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>

          <button onClick={() => shiftsDbService.refresh()} disabled={loading}
                  className="btn-secondary flex items-center gap-2 py-2">
            <svg className={`w-4 h-4 ${loading?'animate-spin':''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
            </svg>
            Refresh
          </button>
        </div>

        {hasFilters && (
          <div className="flex flex-wrap gap-2 mt-3 pt-3 border-t border-gray-100">
            {search    && <span className="inline-flex items-center px-2 py-1 rounded-full text-xs bg-blue-50 text-blue-700 font-medium">Search: "{search}"</span>}
            {status    && <span className="inline-flex items-center px-2 py-1 rounded-full text-xs bg-yellow-50 text-yellow-700 font-medium">{status}</span>}
            {(dateFrom||dateTo) && <span className="inline-flex items-center px-2 py-1 rounded-full text-xs bg-purple-50 text-purple-700 font-medium">{dateFrom||'…'} → {dateTo||'…'}</span>}
          </div>
        )}
      </div>

      {/* Error */}
      {error && (
        <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg flex items-center justify-between text-sm text-red-700">
          <span>{error}</span>
          <button onClick={() => shiftsDbService.refresh()} className="ml-4 font-medium underline">Retry</button>
        </div>
      )}

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50">
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Code</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Date</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Client</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Location</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Timing</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Assigned</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Rate</th>
                <th className="px-5 py-3"/>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {loading ? (
                Array.from({length:8}).map((_,i)=>(
                  <tr key={i}>{Array.from({length:9}).map((_,j)=>(
                    <td key={j} className="px-5 py-3.5"><div className="h-4 bg-gray-100 rounded animate-pulse"/></td>
                  ))}</tr>
                ))
              ) : !error && shifts.length === 0 ? (
                <tr><td colSpan={9} className="px-5 py-16 text-center">
                  <svg className="w-12 h-12 mx-auto mb-3 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                      d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/>
                  </svg>
                  <p className="text-sm text-gray-400">No shifts found{hasFilters?' — try adjusting filters':''}</p>
                </td></tr>
              ) : (
                shifts.map((s) => {
                  const dateStr = s.date
                    ? (typeof s.date==='string' && s.date.includes('T')
                        ? new Date(s.date).toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric'})
                        : s.date)
                    : '—'
                  return (
                    <tr key={s._id} className="hover:bg-gray-50 cursor-pointer transition-colors"
                        onClick={() => setSelectedId(s._id)}>
                      <td className="px-5 py-3.5">
                        <span className="font-mono text-xs bg-gray-100 px-1.5 py-0.5 rounded text-gray-700">
                          {s.name || s.shift_xn_id || '—'}
                        </span>
                      </td>
                      <td className="px-5 py-3.5 text-gray-700 font-medium text-sm">{dateStr}</td>
                      <td className="px-5 py-3.5">
                        <p className="text-gray-700 text-sm">{s.client_name || '—'}</p>
                        {s.client_id && <p className="text-xs text-gray-400 font-mono">{s.client_id.slice(-6)}</p>}
                      </td>
                      <td className="px-5 py-3.5 text-gray-600 text-sm">{s.location || '—'}</td>
                      <td className="px-5 py-3.5 text-gray-500 text-xs">{s.shift_timing || `${s.start_time||''}–${s.end_time||''}`}</td>
                      <td className="px-5 py-3.5">
                        {s.assigned_staff
                          ? <span className="text-gray-700 text-sm">{s.assigned_staff}</span>
                          : <span className="text-gray-400 text-xs italic">Unassigned</span>}
                      </td>
                      <td className="px-5 py-3.5"><StatusBadge status={s.status}/></td>
                      <td className="px-5 py-3.5 text-gray-600 text-sm">
                        {s.rate ? `€${s.rate}/hr` : '—'}
                      </td>
                      <td className="px-5 py-3.5 text-right">
                        <button onClick={(e)=>{e.stopPropagation();setSelectedId(s._id)}}
                                className="text-gray-400 hover:text-green-600 transition-colors">
                          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
                          </svg>
                        </button>
                      </td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>
        <Pagination page={page} perPage={perPage} total={total}/>
      </div>

      {selectedId && <ShiftDrawer shiftId={selectedId} onClose={() => setSelectedId(null)}/>}
    </div>
  )
}
