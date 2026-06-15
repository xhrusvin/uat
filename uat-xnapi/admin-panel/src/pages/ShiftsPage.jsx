import { useEffect, useState, useRef } from 'react'
import { useShiftsDbStore } from '../store/shiftsDbStore'
import { shiftsDbService } from '../services/shiftsDbService'
import { criteriaApi } from '../services/api'
import ShiftDetailPage from './ShiftDetailPage'
import ShiftDrawer from '../components/ShiftDrawer'
import DateRangePicker from '../components/DateRangePicker'

// ── Status config ─────────────────────────────────────────────────────────────
const AUTOMATION_STATUS = {
  'Upcoming':       { dot: '#22c55e', label: 'Live',        sub: 'By Favorites' },
  'To be assigned': { dot: '#f59e0b', label: 'Not Started', sub: null },
  'Completed':      { dot: '#6366f1', label: 'Ended',       sub: 'By Client History' },
  'Cancelled':      { dot: '#6366f1', label: 'Ended',       sub: 'By Client History' },
  'In Progress':    { dot: '#f59e0b', label: 'Paused',      sub: 'By Rating' },
}

const SHIFT_TYPE_ICON = {
  'Night':   { bg: '#7c3aed', icon: '🌙' },
  'Day':     { bg: '#f59e0b', icon: '☀️' },
  'Morning': { bg: '#3b82f6', icon: '🌅' },
  'Evening': { bg: '#ec4899', icon: '🌆' },
}

function getShiftType(shift) {
  const timing = shift.shift_timing || ''
  if (timing.toLowerCase().includes('night')) return 'Night'
  if (timing.toLowerCase().includes('day'))   return 'Day'
  if (shift.start_time) {
    const h = parseInt(shift.start_time.split(':')[0] || '0')
    if (h >= 20 || h < 6)  return 'Night'
    if (h >= 6  && h < 12) return 'Morning'
    if (h >= 12 && h < 17) return 'Day'
    return 'Evening'
  }
  return 'Day'
}

function formatTimeRange(shift) {
  if (shift.start_time && shift.end_time) return `${shift.start_time} – ${shift.end_time}`
  const timing = shift.shift_timing || ''
  const m = timing.match(/\(([^)]+)\)/)
  return m ? m[1] : '—'
}

function formatDate(shift) {
  const d = shift.date
  if (!d) return '—'
  if (typeof d === 'string' && d.includes('T')) {
    return new Date(d).toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', year: 'numeric' })
  }
  return d
}

function AutomationStatus({ status }) {
  const cfg = AUTOMATION_STATUS[status] || { dot: '#9ca3af', label: status || 'Unknown', sub: null }
  return (
    <div>
      <div className="flex items-center gap-1.5">
        <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: cfg.dot }} />
        <span className="text-sm font-medium text-gray-800">{cfg.label}</span>
      </div>
      {cfg.sub && (
        <div className="flex items-center gap-1 mt-0.5 ml-3.5">
          <svg className="w-3 h-3 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 3v4M3 5h4M6 17v4m-2-2h4m5-16l2.286 6.857L21 12l-5.714 2.143L13 21l-2.286-6.857L5 12l5.714-2.143L13 3z" />
          </svg>
          <span className="text-xs text-gray-400">{cfg.sub}</span>
        </div>
      )}
    </div>
  )
}

function ActionButton({ shift, onClick }) {
  const status = shift.status
  if (status === 'Upcoming') return (
    <button onClick={onClick} className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-gray-600 hover:bg-gray-100 rounded-lg border border-gray-200 transition-colors">
      <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
      </svg>
      View
    </button>
  )
  if (status === 'To be assigned') return (
    <button onClick={onClick} className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-white rounded-lg transition-colors" style={{ backgroundColor: '#1e3a8a' }}>
      <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      Start
    </button>
  )
  if (status === 'In Progress') return (
    <button onClick={onClick} className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-gray-600 hover:bg-gray-100 rounded-lg border border-gray-200 transition-colors">
      <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
      </svg>
      View
    </button>
  )
  // Completed / Cancelled → Restart
  return (
    <button onClick={onClick} className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-gray-600 hover:bg-gray-100 rounded-lg border border-gray-200 transition-colors">
      <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
      </svg>
      Restart
    </button>
  )
}

function ShiftRow({ shift, checked, onCheck, onView, onDetail }) {
  const type    = getShiftType(shift)
  const typeInfo = SHIFT_TYPE_ICON[type] || SHIFT_TYPE_ICON['Day']
  const timeStr  = formatTimeRange(shift)
  const dateStr  = formatDate(shift)
  const rate     = shift.rate ? `€${shift.rate}/hr` : '€25/hr'

  return (
    <tr className="border-b border-gray-100 hover:bg-gray-50 transition-colors group">
      {/* Checkbox */}
      <td className="pl-4 pr-2 py-3.5 w-10">
        <input type="checkbox" checked={checked} onChange={onCheck}
               className="w-4 h-4 rounded border-gray-300 text-blue-600 cursor-pointer" />
      </td>

      {/* Client */}
      <td className="px-4 py-3.5">
        <div className="font-medium text-gray-900 text-sm">{shift.client_name || 'Wooster Care Home'}</div>
        <div className="flex items-center gap-1 mt-0.5">
          <span className="text-sm">🪙</span>
          <span className="text-xs font-semibold text-amber-600">{rate}</span>
        </div>
      </td>

      {/* Date */}
      <td className="px-4 py-3.5 text-sm text-gray-700 whitespace-nowrap">{dateStr}</td>

      {/* Time with type badge */}
      <td className="px-4 py-3.5">
        <div className="flex items-center gap-2">
          <span className="w-6 h-6 rounded-full flex items-center justify-center text-xs flex-shrink-0"
                style={{ backgroundColor: typeInfo.bg + '22' }}>
            {typeInfo.icon}
          </span>
          <div>
            <span className="text-xs font-medium text-gray-600">{type}</span>
            <div className="text-xs text-gray-500">{timeStr}</div>
          </div>
        </div>
      </td>

      {/* User Type */}
      <td className="px-4 py-3.5 text-sm text-gray-700">{shift.user_type || 'Nurse'}</td>

      {/* Client Tags — hardcoded from design */}
      <td className="px-4 py-3.5">
        <div className="flex flex-wrap gap-1">
          <span className="px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-700">Soft recruit</span>
          <span className="px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-700">Previous staff reffered</span>
          <div className="w-full flex flex-wrap gap-1 mt-0.5">
            <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600">Paper timesheet</span>
            <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600">Email confirmation</span>
            <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-200 text-gray-500">+2 more</span>
          </div>
        </div>
      </td>

      {/* Automation Status */}
      <td className="px-4 py-3.5">
        <AutomationStatus status={shift.status} />
      </td>

      {/* Available Staff */}
      <td className="px-4 py-3.5">
        {shift.status === 'In Progress' ? (
          <div className="flex items-center gap-1.5">
            <span className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center text-white text-xs font-bold">1</span>
            <span className="text-xs text-gray-600">Staff Available</span>
          </div>
        ) : (
          <span className="text-gray-400 text-sm">–</span>
        )}
      </td>

      {/* Actions */}
      <td className="px-4 py-3.5 text-right">
        <div className="flex items-center gap-2 justify-end">
          <ActionButton shift={shift} onClick={() => {
            const status = shift.status
            if (status === 'To be assigned' || status === 'Completed' || status === 'Cancelled' || status === 'In Progress') {
              onDetail(shift._id)
            } else {
              onView(shift._id)
            }
          }} />
          <button onClick={() => onView(shift._id)}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-gray-600
                             hover:bg-gray-100 rounded-lg border border-gray-200 transition-colors">
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
            </svg>
            View
          </button>
        </div>
      </td>
    </tr>
  )
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
                  style={p===page?{backgroundColor:'#1e3a8a'}:{}}>
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
  const status          = useShiftsDbStore((s) => s.status)
  const userType          = useShiftsDbStore((s) => s.userType)
  const automationStatus  = useShiftsDbStore((s) => s.automationStatus)
  const dateFrom  = useShiftsDbStore((s) => s.dateFrom)
  const dateTo    = useShiftsDbStore((s) => s.dateTo)
  const loading   = useShiftsDbStore((s) => s.loading)
  const error     = useShiftsDbStore((s) => s.error)

  const [searchInput, setSearchInput] = useState(search)
  const [selectedId, setSelectedId]   = useState(null)
  const [checked, setChecked]         = useState(new Set())
  const [allChecked, setAllChecked]   = useState(false)
  const debounceRef                   = useRef(null)
  const [filterCriteria, setFilterCriteria] = useState('')
  const [filterValue, setFilterValue]       = useState('')
  const [criteriaList, setCriteriaList]     = useState([])
  const [detailShiftId, setDetailShiftId]   = useState(null)

  useEffect(() => {
    shiftsDbService.init()
    // Load filter criteria from DB
    criteriaApi.list({ active_only: true })
      .then(({ data }) => setCriteriaList(data.data || []))
      .catch(() => {})
  }, [])

  const handleSearch = (val) => {
    setSearchInput(val)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => shiftsDbService.setSearch(val), 500)
  }

  const handleFilterCriteriaChange = (criteria) => {
    setFilterCriteria(criteria)
    setFilterValue('')
    shiftsDbService.setUserType('')
    shiftsDbService.setAutomationStatus('')
    // Also clear search so new criteria takes effect cleanly
    setSearchInput('')
    shiftsDbService.setSearch('')
  }

  const handleFilterValueChange = (val) => {
    setFilterValue(val)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      // Find the DB field for the selected criteria
      const c = criteriaList.find(c => c.label === filterCriteria)
      const field = c?.field || ''
      if (field === 'user_type')         shiftsDbService.setUserType(val)
      else if (field === 'automation_status') shiftsDbService.setAutomationStatus(val)
      else                               shiftsDbService.setSearch(val)
    }, 400)
  }

  const toggleAll = () => {
    if (allChecked) { setChecked(new Set()); setAllChecked(false) }
    else { setChecked(new Set(shifts.map(s => s._id))); setAllChecked(true) }
  }

  const toggleOne = (id) => {
    const next = new Set(checked)
    next.has(id) ? next.delete(id) : next.add(id)
    setChecked(next)
    setAllChecked(next.size === shifts.length)
  }

  const hasFilters = search || status || userType || automationStatus || dateFrom || dateTo

  if (detailShiftId) return (
    <ShiftDetailPage shiftId={detailShiftId} onBack={() => setDetailShiftId(null)} />
  )

  return (
    <div className="flex flex-col h-full bg-gray-50">
      {/* ── Top tab bar ─────────────────────────────────────────────────── */}
      <div className="bg-white border-b border-gray-200 px-6 pt-4">
        <div className="flex items-center gap-2 mb-4">
          <button className="px-4 py-1.5 rounded-lg text-sm font-semibold text-white"
                  style={{ backgroundColor: '#1e3a8a' }}>
            Shifts
          </button>
          <button className="px-4 py-1.5 rounded-lg text-sm font-medium text-gray-600 hover:bg-gray-100">
            Automation
          </button>
        </div>

        {/* ── Filter row ────────────────────────────────────────────────── */}
        <div className="flex items-center gap-3 pb-3">
          {/* Count tab */}
          <div className="flex items-center gap-1.5 border-b-2 border-blue-900 pb-2 -mb-3">
            <span className="text-sm font-medium text-gray-700">All</span>
            <span className="text-xs font-bold text-white px-1.5 py-0.5 rounded"
                  style={{ backgroundColor: '#1e3a8a' }}>{total}</span>
          </div>

          <div className="flex-1" />

          {/* Date range */}
          <div className="flex items-center gap-1.5 text-sm text-gray-600 bg-white border border-gray-200 rounded-lg px-3 py-1.5">
            <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
            </svg>
            <DateRangePicker
              value={[dateFrom, dateTo]}
              onChange={([f, t]) => { if (f && t) shiftsDbService.setDates(f, t) }}
              onClear={() => shiftsDbService.setDates('', '')}
              placeholder="Jun 28 – Jul 28"
            />
          </div>

          {/* Bulk Start */}
          <button className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium text-white"
                  style={{ backgroundColor: '#6366f1' }}>
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            Bulk Start
          </button>
        </div>
      </div>

      {/* ── Toolbar ─────────────────────────────────────────────────────── */}
      <div className="bg-white border-b border-gray-200 px-6 py-2 flex items-center gap-3">
        {/* Search */}
        <div className="relative">
          <svg className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2"
            fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            type="text"
            className="pl-9 pr-4 py-1.5 text-sm border border-gray-200 rounded-lg w-52
                       focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="Search code, location, county, staff…"
            value={searchInput}
            onChange={(e) => handleSearch(e.target.value)}
          />
        </div>

        {/* Refresh */}
        <button onClick={() => shiftsDbService.refresh()} disabled={loading}
                className="p-1.5 rounded-lg text-gray-500 hover:bg-gray-100 border border-gray-200 transition-colors">
          <svg className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`}
            fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
        </button>

        {/* Filter */}
        <div className="relative">
          <button className="p-1.5 rounded-lg text-gray-500 hover:bg-gray-100 border border-gray-200 transition-colors">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
            </svg>
          </button>
        </div>

        {/* Criteria dropdown — loaded from DB */}
        <select value={filterCriteria} onChange={(e) => handleFilterCriteriaChange(e.target.value)}
                className="text-sm border border-gray-200 rounded-lg px-2 py-1.5 text-gray-600
                           focus:outline-none focus:ring-2 focus:ring-blue-500">
          <option value="">Filter by…</option>
          {criteriaList.map(c => (
            <option key={c._id} value={c.label}>{c.label}</option>
          ))}
        </select>

        {/* Value input — shown when criteria selected */}
        {filterCriteria && (
          <input
            type="text"
            value={filterValue}
            onChange={(e) => handleFilterValueChange(e.target.value)}
            placeholder={{'User Type':'e.g. Nurse, HCA…','Automation Status':'e.g. Upcoming…','County':'e.g. Dublin…','Client':'e.g. Newcastle Hospital…'}[filterCriteria]}
            className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 text-gray-600 w-48
                       focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        )}

        {hasFilters && (
          <button onClick={() => { setSearchInput(''); setFilterCriteria(''); setFilterValue(''); shiftsDbService.clearFilters() }}
                  className="text-xs text-red-500 hover:text-red-700 flex items-center gap-1">
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
            Clear filters
          </button>
        )}

        <div className="flex-1" />

        <div className="flex items-center gap-2 text-xs text-gray-400">
          <span>Show</span>
          <select value={perPage} onChange={(e) => shiftsDbService.setPerPage(Number(e.target.value))}
                  className="text-sm border border-gray-200 rounded px-1.5 py-1 text-gray-600">
            {[10, 20, 50, 100].map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </div>
      </div>

      {/* ── Table ───────────────────────────────────────────────────────── */}
      {error && (
        <div className="mx-6 mt-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg flex items-center justify-between text-sm text-red-700">
          <span>{error}</span>
          <button onClick={() => shiftsDbService.refresh()} className="font-medium underline">Retry</button>
        </div>
      )}

      <div className="flex-1 overflow-auto">
        <table className="w-full text-sm bg-white">
          <thead className="sticky top-0 z-10">
            <tr className="border-b border-gray-200 bg-white">
              <th className="pl-4 pr-2 py-3 w-10">
                <input type="checkbox" checked={allChecked} onChange={toggleAll}
                       className="w-4 h-4 rounded border-gray-300" />
              </th>
              {[
                ['Client', true],
                ['Date', true],
                ['Time', true],
                ['User Type', true],
                ['Client Tags', true],
                ['Automation Status', true],
                ['Available Staff', true],
                ['Actions', false],
              ].map(([label, sortable]) => (
                <th key={label}
                    className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wide whitespace-nowrap">
                  <div className="flex items-center gap-1">
                    {label}
                    {sortable && (
                      <svg className="w-3 h-3 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
                      </svg>
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {loading ? (
              Array.from({ length: 10 }).map((_, i) => (
                <tr key={i} className="border-b border-gray-100">
                  {Array.from({ length: 9 }).map((_, j) => (
                    <td key={j} className="px-4 py-4">
                      <div className="h-4 bg-gray-100 rounded animate-pulse" />
                    </td>
                  ))}
                </tr>
              ))
            ) : !error && shifts.length === 0 ? (
              <tr>
                <td colSpan={9} className="px-4 py-20 text-center">
                  <svg className="w-12 h-12 mx-auto mb-3 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                      d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
                  </svg>
                  <p className="text-sm text-gray-400">No shifts found{hasFilters ? ' — try adjusting filters' : ''}</p>
                </td>
              </tr>
            ) : (
              shifts.map((shift) => (
                <ShiftRow
                  key={shift._id}
                  shift={shift}
                  checked={checked.has(shift._id)}
                  onCheck={() => toggleOne(shift._id)}
                  onView={setSelectedId}
                  onDetail={setDetailShiftId}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="bg-white border-t border-gray-200">
        <Pagination page={page} perPage={perPage} total={total} />
      </div>

      {selectedId && <ShiftDrawer shiftId={selectedId} onClose={() => setSelectedId(null)} />}
    </div>
  )
}
