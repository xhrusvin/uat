import { useState, useEffect } from 'react'
import { commonApi } from '../services/api'

export default function ClientTypePage() {
  const [items, setItems]     = useState([])
  const [total, setTotal]     = useState(0)
  const [loading, setLoading] = useState(true)
  const [syncing, setSyncing] = useState(false)
  const [error, setError]     = useState(null)
  const [syncMsg, setSyncMsg] = useState(null)
  const [search, setSearch]   = useState('')

  const loadFromDb = async () => {
    setLoading(true)
    setError(null)
    try {
      const { data } = await commonApi.clientTypesFromDb()
      setItems(data.data || [])
      setTotal(data.total || 0)
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to load client types')
    } finally {
      setLoading(false)
    }
  }

  const syncFromApi = async () => {
    setSyncing(true)
    setSyncMsg(null)
    setError(null)
    try {
      const { data } = await commonApi.clientTypeList()
      if (data.success === false) {
        setError(data.message || 'Sync failed')
      } else {
        const s = data.sync
        setSyncMsg(s
          ? `Synced: ${s.fetched} fetched — ${s.inserted} new, ${s.updated} updated`
          : 'Sync complete')
        await loadFromDb()
      }
    } catch (err) {
      setError(err.response?.data?.detail || 'Sync failed')
    } finally {
      setSyncing(false)
    }
  }

  useEffect(() => { loadFromDb() }, [])

  const filtered = items.filter(item => {
    if (!search) return true
    const s = search.toLowerCase()
    return Object.values(item).some(v =>
      v !== null && v !== undefined && String(v).toLowerCase().includes(s)
    )
  })

  // Detect columns from first item (excluding meta fields)
  const SKIP = ['_id', 'synced_at', 'created_at']
  const columns = items.length > 0
    ? Object.keys(items[0]).filter(k => !SKIP.includes(k))
    : []

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Master</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>Client Type</span>
      </div>

      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Client Types</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {loading ? 'Loading…' : `${total} client type${total !== 1 ? 's' : ''} in database`}
          </p>
        </div>

        <button
          onClick={syncFromApi}
          disabled={syncing || loading}
          className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white
                     transition-colors disabled:opacity-50"
          style={{ backgroundColor: '#1e7a38' }}
          title="Fetch latest from User API and update database"
        >
          {syncing ? (
            <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
          ) : (
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
          )}
          {syncing ? 'Syncing…' : 'Sync from API'}
        </button>
      </div>

      {/* Sync success banner */}
      {syncMsg && (
        <div className="mb-5 px-4 py-3 bg-green-50 border border-green-200 rounded-lg
                        flex items-center justify-between text-sm text-green-800">
          <div className="flex items-center gap-2">
            <svg className="w-4 h-4 text-green-600 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
            {syncMsg}
          </div>
          <button onClick={() => setSyncMsg(null)} className="text-green-400 hover:text-green-600">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="mb-5 px-4 py-3 bg-red-50 border border-red-200 rounded-lg
                        flex items-center justify-between text-sm text-red-700">
          <span>{error}</span>
          <button onClick={loadFromDb} className="ml-4 font-medium underline">Retry</button>
        </div>
      )}

      {/* Search + table */}
      <div className="card overflow-hidden">

        {/* Search bar */}
        <div className="px-4 py-3 border-b border-gray-200 flex items-center gap-3">
          <div className="relative flex-1 max-w-xs">
            <svg className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2"
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input
              type="text"
              className="input pl-9 py-1.5 text-sm"
              placeholder="Search client types…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
          {search && (
            <span className="text-xs text-gray-500">
              {filtered.length} of {total} result{filtered.length !== 1 ? 's' : ''}
            </span>
          )}
          <div className="flex-1" />
          <button
            onClick={loadFromDb}
            disabled={loading}
            className="btn-secondary flex items-center gap-1.5 py-1.5 text-sm"
          >
            <svg className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`}
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Refresh
          </button>
        </div>

        {/* Table */}
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50">
                {loading ? (
                  ['', '', '', ''].map((_, i) => (
                    <th key={i} className="px-5 py-3">
                      <div className="h-3 bg-gray-200 rounded animate-pulse w-20" />
                    </th>
                  ))
                ) : columns.length > 0 ? (
                  columns.map(col => (
                    <th key={col} className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">
                      {col.replace(/_/g, ' ')}
                    </th>
                  ))
                ) : null}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {loading ? (
                Array.from({ length: 6 }).map((_, i) => (
                  <tr key={i}>
                    {Array.from({ length: 4 }).map((_, j) => (
                      <td key={j} className="px-5 py-3.5">
                        <div className="h-4 bg-gray-100 rounded animate-pulse" />
                      </td>
                    ))}
                  </tr>
                ))
              ) : filtered.length === 0 ? (
                <tr>
                  <td colSpan={columns.length || 4} className="px-5 py-16 text-center">
                    <svg className="w-12 h-12 mx-auto mb-3 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                        d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
                    </svg>
                    {items.length === 0 ? (
                      <div>
                        <p className="text-sm text-gray-400 mb-3">No client types in database yet.</p>
                        <button
                          onClick={syncFromApi}
                          disabled={syncing}
                          className="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white"
                          style={{ backgroundColor: '#1e7a38' }}
                        >
                          Sync from API now
                        </button>
                      </div>
                    ) : (
                      <p className="text-sm text-gray-400">No results for "{search}"</p>
                    )}
                  </td>
                </tr>
              ) : (
                filtered.map((item, idx) => (
                  <tr key={item._id || idx} className="hover:bg-gray-50 transition-colors">
                    {columns.map(col => {
                      const val = item[col]
                      return (
                        <td key={col} className="px-5 py-3.5 text-gray-700">
                          {val === null || val === undefined ? (
                            <span className="text-gray-300 text-xs italic">—</span>
                          ) : typeof val === 'boolean' ? (
                            <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium
                                             ${val ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                              {String(val)}
                            </span>
                          ) : (
                            <span>{String(val)}</span>
                          )}
                        </td>
                      )
                    })}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Footer count */}
        {!loading && filtered.length > 0 && (
          <div className="px-5 py-3 border-t border-gray-200 text-xs text-gray-400">
            {filtered.length} item{filtered.length !== 1 ? 's' : ''}
            {search && ` matching "${search}"`}
          </div>
        )}
      </div>
    </div>
  )
}
