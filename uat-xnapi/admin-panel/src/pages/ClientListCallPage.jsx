import { useState } from 'react'
import { clientsApi } from '../services/api'

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

export default function ClientListCallPage() {
  const [loading, setLoading]   = useState(false)
  const [result, setResult]     = useState(null)
  const [error, setError]       = useState(null)
  const [syncResult, setSyncResult] = useState(null)
  const [showSync, setShowSync] = useState(false)

  // Request params
  const [page, setPage]         = useState(1)
  const [perPage, setPerPage]   = useState(3000)
  const [search, setSearch]     = useState('')
  const [sortBy, setSortBy]     = useState('created_at')
  const [sortOrder, setSortOrder] = useState('desc')

  const handleCall = async () => {
    setLoading(true)
    setError(null)
    setResult(null)
    setSyncResult(null)
    setShowSync(false)

    const payload = {
      search, page, per_page: perPage,
      sort_by: sortBy, sort_order: sortOrder,
    }

    try {
      const { data } = await clientsApi.sync(payload)
      setResult(data)
      if (data.success === false) {
        setError(data.message || 'API returned an error')
      } else if (data.sync) {
        setSyncResult(data.sync)
        setShowSync(true)
      }
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Request failed')
    } finally {
      setLoading(false)
    }
  }

  const clients = result?.data || []

  return (
    <div className="p-8">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API Calls</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>Client List</span>
      </div>
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Client List</h1>
      <p className="text-sm text-gray-500 mb-6">Fetch clients from User API and sync to database.</p>

      {/* Request config */}
      <div className="card p-5 mb-6">
        <div className="flex items-center gap-2 mb-4">
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold bg-blue-100 text-blue-700 font-mono">POST</span>
          <code className="text-xs text-gray-600 bg-gray-100 px-3 py-1.5 rounded-lg font-mono">
            {`${import.meta.env.VITE_API_URL || ''}/clients/sync`}
          </code>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-4">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Search</label>
            <input type="text" className="input text-sm" placeholder="Search term…"
              value={search} onChange={e => setSearch(e.target.value)} />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Page</label>
            <input type="number" className="input text-sm" min={1}
              value={page} onChange={e => setPage(Number(e.target.value))} />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Per Page</label>
            <select className="input text-sm" value={perPage} onChange={e => setPerPage(Number(e.target.value))}>
              {[20, 50, 100, 500, 1000, 3000].map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Sort By</label>
            <select className="input text-sm" value={sortBy} onChange={e => setSortBy(e.target.value)}>
              <option value="created_at">Created At</option>
              <option value="name">Name</option>
              <option value="updated_at">Updated At</option>
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Sort Order</label>
            <select className="input text-sm" value={sortOrder} onChange={e => setSortOrder(e.target.value)}>
              <option value="desc">Newest first</option>
              <option value="asc">Oldest first</option>
            </select>
          </div>
        </div>

        <button onClick={handleCall} disabled={loading}
                className="flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-medium text-white
                           transition-colors disabled:opacity-50"
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

      {showSync && syncResult && <SyncBanner sync={syncResult} onDismiss={() => setShowSync(false)} />}

      {error && (
        <div className="mb-5 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
          {error}
        </div>
      )}

      {result && (
        <div className="card overflow-hidden">
          <div className="px-5 py-3.5 border-b border-gray-200 flex items-center gap-3">
            <span className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold font-mono
                              ${result.success ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
              {result.status_code}
            </span>
            <span className="text-sm text-gray-600">{result.message}</span>
            <span className="text-xs text-gray-400 ml-auto">Total: {result.total ?? clients.length}</span>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 bg-gray-50">
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Name</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Email</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Phone</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Client Type</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">County</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {clients.length === 0 ? (
                  <tr><td colSpan={6} className="px-5 py-10 text-center text-sm text-gray-400">No clients returned</td></tr>
                ) : (
                  clients.map((c, i) => (
                    <tr key={c._id || c.id || i} className="hover:bg-gray-50">
                      <td className="px-5 py-3 font-medium text-gray-900">{c.name || '—'}</td>
                      <td className="px-5 py-3 text-gray-600">{c.email || '—'}</td>
                      <td className="px-5 py-3 text-gray-500">{c.phone || c.mobile || '—'}</td>
                      <td className="px-5 py-3 text-gray-500 text-xs">{c.client_type || c.client_type_name || '—'}</td>
                      <td className="px-5 py-3 text-gray-500 text-xs">{c.county || c.county_name || '—'}</td>
                      <td className="px-5 py-3">
                        <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium
                                          ${c.is_active !== false ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                          {c.is_active !== false ? 'Active' : 'Inactive'}
                        </span>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {!result && !loading && !error && (
        <div className="card p-12 text-center">
          <svg className="w-12 h-12 mx-auto mb-4 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" />
          </svg>
          <p className="text-sm text-gray-400">Configure filters above then click <strong>Fetch & Sync</strong></p>
        </div>
      )}
    </div>
  )
}
