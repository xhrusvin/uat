import { useState } from 'react'
import { commonApi } from '../services/api'

function SyncBanner({ sync, onDismiss }) {
  if (!sync) return null
  return (
    <div className="mb-5 px-4 py-3 bg-green-50 border border-green-200 rounded-lg flex items-center justify-between text-sm">
      <div className="flex items-center gap-2">
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

export default function AdminUserListCallPage() {
  const [loading, setLoading]       = useState(false)
  const [users, setUsers]           = useState(null)   // null = not yet fetched; [] = fetched, empty
  const [error, setError]           = useState(null)
  const [syncResult, setSyncResult] = useState(null)
  const [showSync, setShowSync]     = useState(false)
  const [meta, setMeta]             = useState(null)   // { success, status_code, message, total }

  const handleCall = async () => {
    setLoading(true)
    setError(null)
    setUsers(null)
    setSyncResult(null)
    setShowSync(false)
    setMeta(null)

    try {
      const { data } = await commonApi.administrationUserList()

      // Normalise — upstream might return success:false with no status_code field
      const success    = data?.success !== false
      const statusCode = data?.status_code ?? (success ? 200 : 500)
      const message    = data?.message || (success ? 'OK' : 'Request failed')
      const items      = Array.isArray(data?.data) ? data.data : []
      const total      = data?.total ?? items.length
      const sync       = data?.sync ?? null

      setMeta({ success, status_code: statusCode, message, total })
      setUsers(items)

      if (!success) {
        setError(message)
      } else {
        if (sync) {
          setSyncResult(sync)
          setShowSync(true)
        }
      }
    } catch (err) {
      const msg = err.response?.data?.detail || err.message || 'Request failed'
      setError(msg)
      setUsers([])
      setMeta({ success: false, status_code: err.response?.status ?? 500, message: msg, total: 0 })
    } finally {
      setLoading(false)
    }
  }

  const fetched = users !== null   // true once a call has been made

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API Calls</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>Administration User List</span>
      </div>
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Administration User List</h1>
      <p className="text-sm text-gray-500 mb-6">
        Fetch administration users from User API and sync to{' '}
        <code className="bg-gray-100 px-1 rounded text-xs font-mono">session_users</code> collection.
      </p>

      {/* Request card */}
      <div className="card p-5 mb-6">
        <div className="flex items-center gap-2 mb-2">
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold bg-green-100 text-green-700 font-mono">
            GET
          </span>
          <code className="text-xs text-gray-600 bg-gray-100 px-3 py-1.5 rounded-lg font-mono">
            {`${import.meta.env.VITE_API_URL || ''}/common/administration-user-list`}
          </code>
        </div>
        <p className="text-xs text-gray-400 mb-4">
          Proxies to{' '}
          <span className="font-mono bg-gray-100 px-1 rounded">
            {'{{user_url}}/ai/common/administration-user-list'}
          </span>{' '}
          and upserts results into the{' '}
          <span className="font-mono bg-gray-100 px-1 rounded">session_users</span> collection.
        </p>

        <button
          onClick={handleCall}
          disabled={loading}
          className="flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-medium text-white transition-colors disabled:opacity-50"
          style={{ backgroundColor: '#1e7a38' }}
        >
          {loading
            ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
            : <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>}
          {loading ? 'Fetching & syncing…' : 'Fetch & Sync'}
        </button>
      </div>

      {/* Sync success banner */}
      {showSync && syncResult && (
        <SyncBanner sync={syncResult} onDismiss={() => setShowSync(false)} />
      )}

      {/* Error banner */}
      {error && (
        <div className="mb-5 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Results table — shown once a fetch has been made */}
      {fetched && (
        <div className="card overflow-hidden">
          {/* Table header bar */}
          <div className="px-5 py-3.5 border-b border-gray-200 flex items-center gap-3">
            {meta && (
              <span className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold font-mono
                ${meta.success ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                {meta.status_code}
              </span>
            )}
            <span className="text-sm text-gray-600">{meta?.message}</span>
            <span className="text-xs text-gray-400 ml-auto">
              Total: {meta?.total ?? (users?.length ?? 0)}
            </span>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 bg-gray-50">
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Name</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Email</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Phone</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Role / Type</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  <tr>
                    <td colSpan={5} className="px-5 py-10 text-center text-sm text-gray-400">
                      <div className="flex items-center justify-center gap-2">
                        <div className="w-4 h-4 border-2 border-gray-300 border-t-transparent rounded-full animate-spin" />
                        Loading…
                      </div>
                    </td>
                  </tr>
                ) : !users || users.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-5 py-10 text-center text-sm text-gray-400">
                      No users returned
                    </td>
                  </tr>
                ) : (
                  users.map((u, i) => {
                    const name = u.full_name
                      || [u.first_name, u.last_name].filter(Boolean).join(' ')
                      || u.name
                      || '—'
                    const isActive = u.is_active !== false && u.is_active !== 0
                    return (
                      <tr key={u._id || u.id || u.xn_user_id || i} className="hover:bg-gray-50">
                        <td className="px-5 py-3 font-medium text-gray-900">{name}</td>
                        <td className="px-5 py-3 text-gray-600">{u.email || '—'}</td>
                        <td className="px-5 py-3 text-gray-500">{u.phone || u.mobile || '—'}</td>
                        <td className="px-5 py-3 text-gray-500 text-xs">
                          {u.role || u.user_type || u.user_type_name || '—'}
                        </td>
                        <td className="px-5 py-3">
                          <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium
                            ${isActive ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                            {isActive ? 'Active' : 'Inactive'}
                          </span>
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Empty state — before any fetch */}
      {!fetched && !loading && (
        <div className="card p-12 text-center">
          <svg className="w-12 h-12 mx-auto mb-4 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
          </svg>
          <p className="text-sm text-gray-400">
            Click <strong>Fetch &amp; Sync</strong> to pull administration users from the upstream API
          </p>
        </div>
      )}
    </div>
  )
}
