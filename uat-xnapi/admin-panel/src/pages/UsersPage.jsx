import { useEffect, useState, useRef } from 'react'
import { useUsersStore } from '../store/usersStore'
import { usersService } from '../services/usersService'
import Pagination from '../components/Pagination'
import UserDrawer from '../components/UserDrawer'
import DateRangePicker from '../components/DateRangePicker'

function StatusBadge({ status }) {
  const cls = status?.toLowerCase() === 'enabled'  ? 'badge-enabled'
    : status?.toLowerCase() === 'disabled' ? 'badge-disabled'
    : 'badge-default'
  return <span className={cls}>{status || '—'}</span>
}

function Avatar({ user }) {
  const initials = user.first_name?.[0]?.toUpperCase() || user.email?.[0]?.toUpperCase() || '?'
  return (
    <div className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold flex-shrink-0"
         style={{ backgroundColor: '#e8f5ec', color: '#1e7a38' }}>
      {initials}
    </div>
  )
}

export default function UsersPage() {
  // Read-only state subscription — rendering only, no actions
  const users       = useUsersStore((s) => s.users)
  const total       = useUsersStore((s) => s.total)
  const page        = useUsersStore((s) => s.page)
  const perPage     = useUsersStore((s) => s.perPage)
  const search      = useUsersStore((s) => s.search)
  const dateFrom    = useUsersStore((s) => s.dateFrom)
  const dateTo      = useUsersStore((s) => s.dateTo)
  const listLoading = useUsersStore((s) => s.listLoading)
  const error       = useUsersStore((s) => s.error)

  const [searchInput, setSearchInput] = useState(search)
  const [dateValue, setDateValue]     = useState([dateFrom, dateTo])
  const [selectedId, setSelectedId]   = useState(null)
  const debounceRef                   = useRef(null)

  // init() is module-guarded — runs the fetch only the very first time
  useEffect(() => {
    usersService.init()
  }, [])

  const handleSearchChange = (value) => {
    setSearchInput(value)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => usersService.setSearch(value), 500)
  }

  const handleDateChange = ([from, to]) => {
    setDateValue([from, to])
    usersService.setDateRange(from, to)
  }

  const handleDateClear = () => {
    setDateValue(['', ''])
    usersService.setDateRange('', '')
  }

  const handleClearAll = () => {
    setSearchInput('')
    setDateValue(['', ''])
    usersService.clearFilters()
  }

  const hasFilters = search || dateFrom || dateTo

  return (
    <div className="p-8">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Users</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {listLoading ? 'Loading…' : `${total} user${total !== 1 ? 's' : ''} — sorted oldest first`}
          </p>
        </div>
      </div>

      {/* Filters */}
      <div className="card mb-5 p-4">
        <div className="flex flex-wrap gap-3 items-center">
          <div className="relative flex-1 min-w-48">
            <svg className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2"
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input
              type="text"
              className="input pl-9"
              placeholder="Search name, email, phone…"
              value={searchInput}
              onChange={(e) => handleSearchChange(e.target.value)}
            />
          </div>

          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-500 whitespace-nowrap">Joined</span>
            <DateRangePicker
              value={dateValue}
              onChange={handleDateChange}
              onClear={handleDateClear}
              placeholder="Pick date range…"
            />
          </div>

          {hasFilters && (
            <button onClick={handleClearAll} className="btn-secondary flex items-center gap-1.5 text-sm">
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
              Clear all
            </button>
          )}

          <div className="flex-1" />

          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-500 whitespace-nowrap">Show</span>
            <select value={perPage} onChange={(e) => usersService.setPerPage(Number(e.target.value))}
                    className="input w-20 py-1.5">
              {[10, 20, 50, 100].map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>

          <button onClick={() => usersService.refresh()} disabled={listLoading}
                  className="btn-secondary flex items-center gap-2 py-2">
            <svg className={`w-4 h-4 ${listLoading ? 'animate-spin' : ''}`}
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Refresh
          </button>
        </div>

        {hasFilters && (
          <div className="flex flex-wrap gap-2 mt-3 pt-3 border-t border-gray-100">
            {search && (
              <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs bg-blue-50 text-blue-700 font-medium">
                Search: "{search}"
              </span>
            )}
            {(dateFrom || dateTo) && (
              <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs bg-purple-50 text-purple-700 font-medium">
                Joined: {dateFrom || '…'} → {dateTo || '…'}
              </span>
            )}
          </div>
        )}
      </div>

      {/* Error */}
      {error && (
        <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg flex items-center
                        justify-between text-sm text-red-700">
          <span>{error}</span>
          <button onClick={() => usersService.refresh()}
                  className="ml-4 font-medium underline hover:no-underline">Retry</button>
        </div>
      )}

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50">
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">User</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Email</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Phone</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Designation</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">
                  <div className="flex items-center gap-1">
                    Joined
                    <svg className="w-3 h-3 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" />
                    </svg>
                  </div>
                </th>
                <th className="px-5 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {listLoading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i}>
                    {Array.from({ length: 7 }).map((_, j) => (
                      <td key={j} className="px-5 py-3.5">
                        <div className="h-4 bg-gray-100 rounded animate-pulse" />
                      </td>
                    ))}
                  </tr>
                ))
              ) : !error && users.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-5 py-16 text-center text-gray-400">
                    <p className="text-sm">No users found{hasFilters ? ' — try adjusting your filters' : ''}</p>
                  </td>
                </tr>
              ) : (
                users.map((u) => (
                  <tr key={u.id} className="hover:bg-gray-50 transition-colors cursor-pointer"
                      onClick={() => setSelectedId(u.id)}>
                    <td className="px-5 py-3.5">
                      <div className="flex items-center gap-3">
                        <Avatar user={u} />
                        <span className="font-medium text-gray-900">{u.full_name || '—'}</span>
                      </div>
                    </td>
                    <td className="px-5 py-3.5 text-gray-600">{u.email}</td>
                    <td className="px-5 py-3.5 text-gray-500">{u.phone || '—'}</td>
                    <td className="px-5 py-3.5 text-gray-500 text-xs">{u.designation || '—'}</td>
                    <td className="px-5 py-3.5"><StatusBadge status={u.status} /></td>
                    <td className="px-5 py-3.5 text-gray-400 text-xs">
                      {u.created_at ? new Date(u.created_at).toLocaleDateString('en-GB', {
                        day: '2-digit', month: 'short', year: 'numeric'
                      }) : '—'}
                    </td>
                    <td className="px-5 py-3.5 text-right">
                      <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                      </svg>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <Pagination page={page} perPage={perPage} total={total} onPage={(p) => usersService.setPage(p)} />
      </div>

      {selectedId && (
        <UserDrawer userId={selectedId} onClose={() => setSelectedId(null)} />
      )}
    </div>
  )
}
