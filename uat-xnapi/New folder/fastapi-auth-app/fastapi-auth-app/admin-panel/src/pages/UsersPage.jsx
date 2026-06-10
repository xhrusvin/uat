import { useEffect, useState, useCallback } from 'react'
import { useUsersStore } from '../store/usersStore'
import Pagination from '../components/Pagination'
import UserDrawer from '../components/UserDrawer'

function StatusBadge({ status }) {
  const cls = status?.toLowerCase() === 'enabled' ? 'badge-enabled'
    : status?.toLowerCase() === 'disabled' ? 'badge-disabled'
    : 'badge-default'
  return <span className={cls}>{status || '—'}</span>
}

function Avatar({ user }) {
  const initials = user.first_name?.[0]?.toUpperCase() || user.email?.[0]?.toUpperCase() || '?'
  return (
    <div className="w-8 h-8 rounded-full bg-brand-100 text-brand-700 flex items-center
                    justify-center text-xs font-bold flex-shrink-0">
      {initials}
    </div>
  )
}

export default function UsersPage() {
  const {
    users, total, page, perPage, search,
    loading, error, fetchUsers, setSearch, setPage, setPerPage,
  } = useUsersStore()

  const [searchInput, setSearchInput] = useState(search)
  const [selectedId, setSelectedId] = useState(null)

  useEffect(() => { fetchUsers() }, [])

  // Debounced search
  useEffect(() => {
    const timer = setTimeout(() => {
      if (searchInput !== search) setSearch(searchInput)
    }, 400)
    return () => clearTimeout(timer)
  }, [searchInput])

  const totalPages = Math.ceil(total / perPage)

  return (
    <div className="p-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Users</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {total} total user{total !== 1 ? 's' : ''}
          </p>
        </div>
      </div>

      {/* Filters bar */}
      <div className="card mb-5 px-4 py-3 flex flex-wrap items-center gap-3">
        {/* Search */}
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
            onChange={(e) => setSearchInput(e.target.value)}
          />
        </div>

        {/* Per page */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">Show</span>
          <select
            value={perPage}
            onChange={(e) => setPerPage(Number(e.target.value))}
            className="input w-20 py-1.5"
          >
            {[10, 20, 50, 100].map((n) => (
              <option key={n} value={n}>{n}</option>
            ))}
          </select>
        </div>

        {/* Refresh */}
        <button
          onClick={fetchUsers}
          disabled={loading}
          className="btn-secondary flex items-center gap-2 py-1.5"
        >
          <svg className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`}
            fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Refresh
        </button>
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        {error ? (
          <div className="px-5 py-8 text-center text-sm text-red-500">{error}</div>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 bg-gray-50">
                    <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">User</th>
                    <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Email</th>
                    <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Phone</th>
                    <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Designation</th>
                    <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
                    <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Joined</th>
                    <th className="px-5 py-3" />
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {loading && users.length === 0 ? (
                    Array.from({ length: 5 }).map((_, i) => (
                      <tr key={i}>
                        {Array.from({ length: 6 }).map((_, j) => (
                          <td key={j} className="px-5 py-3.5">
                            <div className="h-4 bg-gray-100 rounded animate-pulse" />
                          </td>
                        ))}
                      </tr>
                    ))
                  ) : users.length === 0 ? (
                    <tr>
                      <td colSpan={7} className="px-5 py-12 text-center text-gray-400">
                        <svg className="w-10 h-10 mx-auto mb-3 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                            d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" />
                        </svg>
                        No users found
                        {search && <span> for "<strong>{search}</strong>"</span>}
                      </td>
                    </tr>
                  ) : (
                    users.map((u) => (
                      <tr
                        key={u.id}
                        className="hover:bg-gray-50 transition-colors cursor-pointer"
                        onClick={() => setSelectedId(u.id)}
                      >
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
                          {u.created_at ? new Date(u.created_at).toLocaleDateString() : '—'}
                        </td>
                        <td className="px-5 py-3.5 text-right">
                          <button
                            onClick={(e) => { e.stopPropagation(); setSelectedId(u.id) }}
                            className="text-gray-400 hover:text-brand-500 transition-colors"
                          >
                            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                                d="M9 5l7 7-7 7" />
                            </svg>
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            <Pagination
              page={page}
              perPage={perPage}
              total={total}
              onPage={setPage}
            />
          </>
        )}
      </div>

      {/* User drawer */}
      {selectedId && (
        <UserDrawer userId={selectedId} onClose={() => setSelectedId(null)} />
      )}
    </div>
  )
}
