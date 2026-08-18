import { useEffect, useState } from 'react'
import { useShiftsDbStore } from '../store/shiftsDbStore'
import { shiftsDbService } from '../services/shiftsDbService'

// ── Helpers ───────────────────────────────────────────────────────────────────
function getInitials(name = '') {
  return name.split(' ').map(p => p[0]).join('').toUpperCase().slice(0, 2) || '??'
}

const AVATAR_COLORS = [
  '#6366f1','#ec4899','#f59e0b','#10b981','#3b82f6',
  '#8b5cf6','#ef4444','#14b8a6','#f97316','#84cc16',
]
function avatarColor(name = '') {
  let hash = 0
  for (const c of name) hash = (hash * 31 + c.charCodeAt(0)) & 0xffff
  return AVATAR_COLORS[hash % AVATAR_COLORS.length]
}

function getShiftType(shift) {
  const t = (shift.shift_timing || '').toLowerCase()
  if (t.includes('night')) return { label: 'Night', icon: '🌙', color: '#7c3aed' }
  if (t.includes('morning')) return { label: 'Morning', icon: '🌅', color: '#3b82f6' }
  return { label: 'Day', icon: '☀️', color: '#f59e0b' }
}

function formatTimeRange(shift) {
  if (shift.start_time && shift.end_time) return `${shift.start_time} – ${shift.end_time}`
  const m = (shift.shift_timing || '').match(/\(([^)]+)\)/)
  return m ? m[1] : ''
}

// ── Mock staff data (hardcoded like design, real data would come from User API) ──
const MOCK_CHANNELS = ['Phone', 'Phone', 'Phone', 'Phone', 'Phone', 'WhatsApp', 'WhatsApp', 'WhatsApp']
const MOCK_NAMES = [
  'Annette Black', 'Theresa Webb', 'Floyd Miles', 'Robert Fox',
  'Ronald Richards', 'Cameron Williamson', 'Arlene McCoy', 'Jacob Jones',
]

function StaffRow({ name, channel, index, checked, onToggle }) {
  const [isChecked, setIsChecked] = useState(checked ?? true)
  const ch = MOCK_CHANNELS[index % MOCK_CHANNELS.length]
  const bg = avatarColor(name)
  const initials = getInitials(name)

  const toggle = () => { setIsChecked(!isChecked); onToggle?.(!isChecked) }

  return (
    <tr className="border-b border-gray-100 hover:bg-gray-50 transition-colors">
      {/* Checkbox */}
      <td className="pl-5 pr-3 py-3.5 w-10">
        <div onClick={toggle}
             className={`w-5 h-5 rounded flex items-center justify-center cursor-pointer border-2 transition-colors
                         ${isChecked ? 'border-[#1e3a8a] bg-[#1e3a8a]' : 'border-gray-300 bg-white'}`}>
          {isChecked && (
            <svg className="w-3 h-3 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
            </svg>
          )}
        </div>
      </td>

      {/* Staff */}
      <td className="px-4 py-3.5">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-full flex items-center justify-center text-white text-xs font-bold flex-shrink-0"
               style={{ backgroundColor: bg }}>
            {initials}
          </div>
          <div>
            <p className="text-sm font-medium text-gray-900">{name}</p>
            <div className="flex items-center gap-1 mt-0.5">
              <svg className="w-3 h-3 text-amber-400" fill="currentColor" viewBox="0 0 20 20">
                <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
              </svg>
              <span className="text-xs text-gray-500">4.5</span>
              {index === 0 && (
                <span className="ml-1 text-xs font-medium text-green-600 bg-green-50 px-1.5 py-0.5 rounded-full">● Requested</span>
              )}
            </div>
          </div>
        </div>
      </td>

      {/* Channel */}
      <td className="px-4 py-3.5">
        <div className="flex items-center gap-2 text-sm text-gray-600">
          {ch === 'Phone' ? (
            <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z" />
            </svg>
          ) : (
            <svg className="w-4 h-4 text-green-500" fill="currentColor" viewBox="0 0 24 24">
              <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413Z"/>
            </svg>
          )}
          {ch}
        </div>
      </td>

      {/* Work History */}
      <td className="px-4 py-3.5">
        <span className="text-sm text-gray-600">8 Shifts · 2 days ago</span>
      </td>

      {/* Distance */}
      <td className="px-4 py-3.5">
        <div className="flex items-center gap-1.5 text-sm text-gray-600">
          <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
          </svg>
          3.4 km
        </div>
      </td>

      {/* Staff Tags */}
      <td className="px-4 py-3.5">
        <div className="flex gap-1.5 flex-wrap">
          <span className="px-2 py-0.5 rounded text-xs font-medium bg-blue-50 text-blue-600 border border-blue-200">Client History</span>
          <span className="px-2 py-0.5 rounded text-xs font-medium bg-orange-50 text-orange-600 border border-orange-200">Favorite</span>
        </div>
      </td>

      {/* Last Contacted */}
      <td className="px-4 py-3.5 text-sm text-gray-500">8 days ago</td>
    </tr>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function ShiftDetailPage({ shiftId, onBack }) {
  const shift         = useShiftsDbStore((s) => s.selected)
  const drawerLoading = useShiftsDbStore((s) => s.drawerLoading)

  const [staffPage, setStaffPage] = useState(1)
  const [rowsPerPage, setRowsPerPage] = useState(8)
  const [searchStaff, setSearchStaff] = useState('')
  const TOTAL_STAFF = 24

  useEffect(() => {
    if (shiftId) shiftsDbService.fetchOne(shiftId)
    return () => useShiftsDbStore.getState().clearSelected()
  }, [shiftId])

  if (drawerLoading) return (
    <div className="flex items-center justify-center h-full">
      <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin"
           style={{ borderColor: '#1e3a8a', borderTopColor: 'transparent' }} />
    </div>
  )

  if (!shift) return null

  const shiftType = getShiftType(shift)
  const timeRange = formatTimeRange(shift)
  const rate      = shift.rate ? `€${shift.rate}/hr` : '€25/hr'
  const totalPages = Math.ceil(TOTAL_STAFF / rowsPerPage)

  return (
    <div className="flex flex-col h-full bg-gray-50">

      {/* ── Top header ────────────────────────────────────────────────────── */}
      <div className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-start justify-between">
          <div>
            {/* Back + title */}
            <div className="flex items-center gap-3 mb-2">
              <button onClick={onBack}
                      className="p-1.5 rounded-lg text-gray-500 hover:bg-gray-100 transition-colors">
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                </svg>
              </button>
              <h1 className="text-xl font-bold text-gray-900">
                {shift.client_name || shift.location || 'Shift Detail'}
              </h1>
            </div>
            {/* Shift meta pills */}
            <div className="flex items-center gap-3 ml-10 flex-wrap">
              <span className="flex items-center gap-1 text-xs font-semibold text-amber-600">
                🪙 {rate}
              </span>
              <span className="text-gray-300">·</span>
              <span className="text-sm text-gray-600">
                {shift.date
                  ? (typeof shift.date === 'string' && shift.date.includes('T')
                      ? new Date(shift.date).toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', year: 'numeric' })
                      : shift.date)
                  : '—'}
              </span>
              <span className="text-gray-300">·</span>
              <div className="flex items-center gap-1.5">
                <span className="w-5 h-5 rounded-full flex items-center justify-center text-xs"
                      style={{ backgroundColor: shiftType.color + '22' }}>
                  {shiftType.icon}
                </span>
                <span className="text-sm text-gray-600">{shiftType.label} {timeRange}</span>
              </div>
              <span className="text-gray-300">·</span>
              <span className="text-sm text-gray-600">{shift.user_type || 'Nurse'}</span>
            </div>
          </div>

          {/* Action buttons */}
          <div className="flex items-center gap-3 flex-shrink-0">
            <button onClick={onBack}
                    className="px-4 py-2 rounded-lg text-sm font-medium text-gray-700 border border-gray-300 hover:bg-gray-50 transition-colors">
              Cancel
            </button>
            <button className="px-5 py-2 rounded-lg text-sm font-medium text-white transition-colors"
                    style={{ backgroundColor: '#1e3a8a' }}>
              Continue to Sequence
            </button>
          </div>
        </div>
      </div>

      {/* ── Pool summary bar ──────────────────────────────────────────────── */}
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between">
        <div>
          <p className="text-sm font-semibold text-gray-900">{TOTAL_STAFF} Staff in Pool</p>
          <p className="text-xs text-gray-500 mt-0.5">phone 10 · WhatsApp 3 · email 1</p>
        </div>

        {/* Progress steps */}
        <div className="flex items-center gap-3">
          {[
            { label: 'Build Pool', active: true, done: false },
            { label: 'Pick Sequence', active: false, done: false },
            { label: 'Start', active: false, done: false },
          ].map((step, i) => (
            <div key={step.label} className="flex items-center gap-2">
              {i > 0 && (
                <svg className="w-4 h-4 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
              )}
              <div className="flex items-center gap-2">
                <div className={`w-4 h-4 rounded-full border-2 flex items-center justify-center
                                 ${step.active ? 'border-[#1e3a8a] bg-[#1e3a8a]' : 'border-gray-300'}`}>
                  {step.active && <div className="w-1.5 h-1.5 rounded-full bg-white" />}
                </div>
                <span className={`text-sm ${step.active ? 'font-semibold text-gray-900' : 'text-gray-400'}`}>
                  {step.label}
                </span>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ── Pool composition ─────────────────────────────────────────────── */}
      <div className="bg-blue-50 border-b border-blue-100 px-6 py-2.5 flex items-center justify-between text-sm">
        <div className="flex items-center gap-6 text-gray-600">
          <div className="flex items-center gap-2">
            <svg className="w-4 h-4 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" />
            </svg>
            <span>Pool composition:</span>
            <strong className="text-gray-900">18 from bulk pool</strong>
            <span>·</span>
            <strong className="text-gray-900">0 added by you</strong>
          </div>
          <div className="flex items-center gap-1.5 text-gray-500">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636" />
            </svg>
            <span>25 excluded by system</span>
          </div>
        </div>
        <button className="text-sm font-medium text-blue-600 hover:text-blue-800">View Excluded</button>
      </div>

      {/* ── Staff search ─────────────────────────────────────────────────── */}
      <div className="bg-white border-b border-gray-200 px-6 py-3">
        <div className="relative max-w-2xl">
          <input
            type="text"
            value={searchStaff}
            onChange={e => setSearchStaff(e.target.value)}
            placeholder="Search by name or phone to add staff directly to pool..."
            className="w-full pl-4 pr-10 py-2.5 text-sm border border-gray-300 rounded-lg
                       focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
          <svg className="w-4 h-4 text-gray-400 absolute right-3 top-1/2 -translate-y-1/2"
            fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </div>
      </div>

      {/* ── Filter chips ─────────────────────────────────────────────────── */}
      <div className="bg-white border-b border-gray-200 px-6 py-2.5 flex items-center gap-2">
        {/* Distance chip */}
        <button className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium
                           bg-gray-100 text-gray-700 hover:bg-gray-200 transition-colors border border-gray-200">
          25 km
          <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </button>

        {/* Active filters */}
        {[shift.user_type || 'Nurse', shift.location || shift.client_county || 'Dublin'].filter(Boolean).map(tag => (
          <div key={tag} className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium
                                    bg-blue-50 text-blue-700 border border-blue-200">
            <button className="text-blue-400 hover:text-blue-700">×</button>
            {tag}
          </div>
        ))}

        <button className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium
                           text-gray-600 bg-white border border-dashed border-gray-300 hover:border-gray-400 transition-colors">
          + Previously worked
        </button>

        <button className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 ml-1">
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
          </svg>
        </button>
      </div>

      {/* ── Staff table ───────────────────────────────────────────────────── */}
      <div className="flex-1 overflow-auto bg-white">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-white z-10">
            <tr className="border-b border-gray-200">
              <th className="pl-5 pr-3 py-3 w-10">
                <div className="w-5 h-5 rounded border-2 border-[#1e3a8a] bg-[#1e3a8a] flex items-center justify-center">
                  <svg className="w-3 h-3 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                  </svg>
                </div>
              </th>
              {['Staff', 'Channel', 'Work History', 'Distance', 'Staff Tags', 'Last Contacted'].map(h => (
                <th key={h} className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wide">
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {MOCK_NAMES.map((name, i) => (
              <StaffRow key={name} name={name} index={i} checked={true} />
            ))}
          </tbody>
        </table>
      </div>

      {/* ── Pagination ───────────────────────────────────────────────────── */}
      <div className="bg-white border-t border-gray-200 px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-1">
          <button onClick={() => setStaffPage(p => Math.max(1, p - 1))} disabled={staffPage === 1}
                  className="p-1.5 rounded text-gray-500 hover:bg-gray-100 disabled:opacity-40">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
            </svg>
          </button>
          {[1, 2, 3].map(p => (
            <button key={p} onClick={() => setStaffPage(p)}
                    className={`w-8 h-8 rounded text-sm font-medium transition-colors
                                ${p === staffPage ? 'text-white' : 'text-gray-600 hover:bg-gray-100'}`}
                    style={p === staffPage ? { backgroundColor: '#1e3a8a' } : {}}>
              {p}
            </button>
          ))}
          <span className="px-1 text-gray-400 text-sm">…</span>
          <button onClick={() => setStaffPage(10)}
                  className={`w-8 h-8 rounded text-sm font-medium transition-colors
                              ${staffPage === 10 ? 'text-white' : 'text-gray-600 hover:bg-gray-100'}`}
                  style={staffPage === 10 ? { backgroundColor: '#1e3a8a' } : {}}>
            10
          </button>
          <button onClick={() => setStaffPage(p => Math.min(totalPages, p + 1))} disabled={staffPage === totalPages}
                  className="p-1.5 rounded text-gray-500 hover:bg-gray-100 disabled:opacity-40">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
          </button>
        </div>

        <div className="flex items-center gap-2 text-sm text-gray-600">
          <span>Rows per page</span>
          <select value={rowsPerPage} onChange={e => setRowsPerPage(Number(e.target.value))}
                  className="border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none">
            {[8, 16, 24].map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </div>
      </div>
    </div>
  )
}
