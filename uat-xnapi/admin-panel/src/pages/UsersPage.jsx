import { useEffect, useState, useRef } from 'react'
import { useUsersStore } from '../store/usersStore'
import { usersService } from '../services/usersService'
import { usersApi } from '../services/api'
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
  const i = user.first_name?.[0]?.toUpperCase() || user.email?.[0]?.toUpperCase() || '?'
  return (
    <div className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold flex-shrink-0"
         style={{ backgroundColor: '#e8f5ec', color: '#1e7a38' }}>{i}</div>
  )
}

function DeleteModal({ user, onConfirm, onCancel, loading }) {
  if (!user) return null
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-md mx-4">
        <div className="px-6 py-5 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">Delete User</h2>
        </div>
        <div className="px-6 py-5">
          <div className="flex items-center gap-3 mb-4">
            <div className="w-10 h-10 rounded-full flex items-center justify-center text-sm font-bold flex-shrink-0"
                 style={{ backgroundColor: '#fee2e2', color: '#dc2626' }}>
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/>
              </svg>
            </div>
            <div>
              <p className="text-sm font-medium text-gray-900">Are you sure you want to delete this user?</p>
              <p className="text-xs text-gray-500 mt-0.5">This action cannot be undone.</p>
            </div>
          </div>
          <div className="bg-gray-50 rounded-lg p-3 text-sm">
            <p className="font-medium text-gray-800">{user.full_name || '—'}</p>
            <p className="text-gray-500 text-xs mt-0.5">{user.email}</p>
            {user.designation && <p className="text-gray-400 text-xs">{user.designation}</p>}
          </div>
        </div>
        <div className="px-6 py-4 border-t border-gray-100 flex justify-end gap-2">
          <button onClick={onCancel} disabled={loading}
            className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-600">
            Cancel
          </button>
          <button onClick={onConfirm} disabled={loading}
            className="flex items-center gap-2 px-4 py-2 text-sm rounded-lg bg-red-600 hover:bg-red-700 text-white disabled:opacity-50">
            {loading
              ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"/>
              : <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/>
                </svg>
            }
            {loading ? 'Deleting…' : 'Delete User'}
          </button>
        </div>
      </div>
    </div>
  )
}

export default function UsersPage() {
  const users       = useUsersStore((s) => s.users)
  const total       = useUsersStore((s) => s.total)
  const page        = useUsersStore((s) => s.page)
  const perPage     = useUsersStore((s) => s.perPage)
  const search      = useUsersStore((s) => s.search)
  const dateFrom    = useUsersStore((s) => s.dateFrom)
  const dateTo      = useUsersStore((s) => s.dateTo)
  const listLoading = useUsersStore((s) => s.listLoading)
  const error       = useUsersStore((s) => s.error)

  const [searchInput, setSearchInput]   = useState(search)
  const [selectedId, setSelectedId]     = useState(null)
  const [deleteUser, setDeleteUser]     = useState(null)
  const [deleting, setDeleting]         = useState(false)
  const [deleteError, setDeleteError]   = useState(null)
  const debounceRef                     = useRef(null)

  useEffect(() => { usersService.init() }, [])

  const handleSearchChange = (val) => {
    setSearchInput(val)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => usersService.setSearch(val), 500)
  }

  const handleDateChange = ([from, to]) => {
    if (from && to) usersService.setDateRange(from, to)
  }

  const handleDateClear = () => usersService.setDateRange('', '')

  const handleClearAll = () => {
    setSearchInput('')
    usersService.clearFilters()
  }

  const handleDeleteConfirm = async () => {
    if (!deleteUser) return
    setDeleting(true)
    setDeleteError(null)
    try {
      await usersApi.delete(deleteUser.id)
      setDeleteUser(null)
      usersService.refresh()
    } catch (err) {
      setDeleteError(err?.response?.data?.detail || 'Failed to delete user')
    } finally {
      setDeleting(false)
    }
  }

  const hasFilters = search || dateFrom || dateTo

  return (
    <div className="p-8">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Users</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {listLoading ? 'Loading…' : `${total} user${total !== 1 ? 's' : ''} — oldest first`}
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
                d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input type="text" className="input pl-9" placeholder="Search name, email, phone…"
              value={searchInput} onChange={(e) => handleSearchChange(e.target.value)} />
          </div>

          {/* Date range */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-500 whitespace-nowrap">Joined</span>
            <DateRangePicker
              value={[dateFrom, dateTo]}
              onChange={handleDateChange}
              onClear={handleDateClear}
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
        <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg flex items-center justify-between text-sm text-red-700">
          <span>{error}</span>
          <button onClick={() => usersService.refresh()} className="ml-4 font-medium underline">Retry</button>
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
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Tags</th>
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
                  <tr key={i}>{Array.from({ length: 8 }).map((_, j) => (
                    <td key={j} className="px-5 py-3.5">
                      <div className="h-4 bg-gray-100 rounded animate-pulse" />
                    </td>
                  ))}</tr>
                ))
              ) : !error && users.length === 0 ? (
                <tr><td colSpan={8} className="px-5 py-16 text-center text-sm text-gray-400">
                  No users found{hasFilters ? ' — try adjusting your filters' : ''}
                </td></tr>
              ) : (
                users.map((u) => (
                  <tr key={u.id} className="hover:bg-gray-50">
                    <td className="px-5 py-3.5 cursor-pointer" onClick={() => setSelectedId(u.id)}>
                      <div className="flex items-center gap-3">
                        <Avatar user={u} />
                        <span className="font-medium text-gray-900">{u.full_name || '—'}</span>
                      </div>
                    </td>
                    <td className="px-5 py-3.5 text-gray-600 cursor-pointer" onClick={() => setSelectedId(u.id)}>{u.email}</td>
                    <td className="px-5 py-3.5 cursor-pointer" onClick={() => setSelectedId(u.id)}>
                      <div className="flex flex-wrap gap-1">
                        {(u.tags || []).map((t, i) => (
                          <span key={t.id || i} className="px-1.5 py-0.5 rounded text-xs font-medium bg-orange-100 text-orange-700 whitespace-nowrap">
                            {t.name || t}
                          </span>
                        ))}
                      </div>
                    </td>
                    <td className="px-5 py-3.5 text-gray-500 cursor-pointer" onClick={() => setSelectedId(u.id)}>{u.phone || '—'}</td>
                    <td className="px-5 py-3.5 text-gray-500 text-xs cursor-pointer" onClick={() => setSelectedId(u.id)}>{u.designation || '—'}</td>
                    <td className="px-5 py-3.5 cursor-pointer" onClick={() => setSelectedId(u.id)}><StatusBadge status={u.status} /></td>
                    <td className="px-5 py-3.5 text-gray-400 text-xs cursor-pointer" onClick={() => setSelectedId(u.id)}>
                      {u.created_at ? new Date(u.created_at).toLocaleDateString('en-GB', {
                        day: '2-digit', month: 'short', year: 'numeric'
                      }) : '—'}
                    </td>
                    <td className="px-5 py-3.5 text-right">
                      <div className="flex items-center justify-end gap-2">
                        <button
                          onClick={(e) => { e.stopPropagation(); setDeleteUser(u); setDeleteError(null) }}
                          className="p-1.5 rounded-lg text-gray-400 hover:text-red-600 hover:bg-red-50 transition-colors"
                          title="Delete user">
                          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/>
                          </svg>
                        </button>
                        <svg className="w-4 h-4 text-gray-400 cursor-pointer" onClick={() => setSelectedId(u.id)} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                        </svg>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <Pagination page={page} perPage={perPage} total={total} onPage={(p) => usersService.setPage(p)} />
      </div>

      {selectedId && <UserDrawer userId={selectedId} onClose={() => setSelectedId(null)} />}

      {deleteError && (
        <div className="fixed bottom-4 right-4 z-50 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700 shadow-lg">
          {deleteError}
        </div>
      )}

      <DeleteModal
        user={deleteUser}
        loading={deleting}
        onConfirm={handleDeleteConfirm}
        onCancel={() => { setDeleteUser(null); setDeleteError(null) }}
      />
    </div>
  )
}
