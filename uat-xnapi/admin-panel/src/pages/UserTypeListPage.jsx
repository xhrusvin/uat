import { useState } from 'react'
import { userTypeListApi } from '../services/api'

export default function UserTypeListPage() {
  const [loading, setLoading]       = useState(false)
  const [error, setError]           = useState(null)
  const [result, setResult]         = useState(null)
  const [subLoading, setSubLoading] = useState({})  // xn_id → bool
  const [subResults, setSubResults] = useState({})  // xn_id → result

  const sync = async () => {
    setLoading(true); setError(null); setResult(null); setSubResults({})
    try {
      const { data } = await userTypeListApi.sync()
      setResult(data)
    } catch (err) {
      setError(err?.response?.data?.detail || 'Failed to sync user types')
    } finally { setLoading(false) }
  }

  const syncSubTypes = async (xn_id, name) => {
    setSubLoading(p => ({ ...p, [xn_id]: true }))
    try {
      const { data } = await userTypeListApi.syncSubTypes(xn_id)
      setSubResults(p => ({ ...p, [xn_id]: data }))
    } catch (err) {
      setSubResults(p => ({ ...p, [xn_id]: { error: err?.response?.data?.detail || 'Failed' } }))
    } finally {
      setSubLoading(p => ({ ...p, [xn_id]: false }))
    }
  }

  const actionColor = (action) => {
    if (action === 'inserted')        return 'bg-green-100 text-green-700'
    if (action === 'updated')         return 'bg-blue-100 text-blue-700'
    if (action === 'matched_by_name') return 'bg-yellow-100 text-yellow-700'
    return 'bg-gray-100 text-gray-500'
  }

  return (
    <div className="p-8 max-w-3xl">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API Calls</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>User Type List</span>
      </div>

      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">User Type List</h1>
          <p className="text-sm text-gray-500 mt-1">Sync user types from upstream. Then fetch sub types per type.</p>
        </div>
        <button onClick={sync} disabled={loading}
          className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white disabled:opacity-50"
          style={{ backgroundColor: '#1e7a38' }}>
          {loading
            ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"/>
            : <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
              </svg>
          }
          {loading ? 'Syncing…' : 'Sync User Types'}
        </button>
      </div>

      {error && <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">{error}</div>}

      {result && (
        <>
          <div className="grid grid-cols-3 gap-4 mb-5">
            {[
              { label: 'Total from upstream', value: result.total,    color: 'bg-gray-50' },
              { label: 'Updated',             value: result.updated,  color: 'bg-blue-50' },
              { label: 'Inserted',            value: result.inserted, color: 'bg-green-50' },
            ].map(s => (
              <div key={s.label} className={`card p-4 text-center ${s.color}`}>
                <div className="text-2xl font-bold text-gray-900">{s.value}</div>
                <div className="text-xs text-gray-500 mt-1">{s.label}</div>
              </div>
            ))}
          </div>

          <div className="card overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b border-gray-100">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Name</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">XN ID</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Action</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Sub Types</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {(result.results || []).map((r, i) => {
                  const sr = subResults[r.xn_id]
                  return (
                    <tr key={i} className="hover:bg-gray-50/50">
                      <td className="px-4 py-3 font-medium text-gray-900">{r.name}</td>
                      <td className="px-4 py-3 text-xs font-mono text-gray-500">{r.xn_id}</td>
                      <td className="px-4 py-3">
                        <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${actionColor(r.action)}`}>
                          {r.action}
                        </span>
                      </td>
                      <td className="px-4 py-3">
                        {sr?.error ? (
                          <span className="text-xs text-red-500">{sr.error}</span>
                        ) : sr ? (
                          <span className="text-xs text-gray-500">
                            {sr.total} sub type{sr.total !== 1 ? 's' : ''} — {sr.inserted} new, {sr.updated} updated
                          </span>
                        ) : (
                          <button
                            onClick={() => syncSubTypes(r.xn_id, r.name)}
                            disabled={subLoading[r.xn_id]}
                            className="flex items-center gap-1 px-2 py-1 rounded text-xs font-medium bg-gray-100 hover:bg-blue-50 hover:text-blue-700 text-gray-600 disabled:opacity-50">
                            {subLoading[r.xn_id]
                              ? <div className="w-3 h-3 border-2 border-current border-t-transparent rounded-full animate-spin"/>
                              : <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
                                </svg>
                            }
                            {subLoading[r.xn_id] ? 'Fetching…' : 'Get Sub Types'}
                          </button>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </>
      )}

      {!result && !loading && !error && (
        <div className="card p-12 text-center text-gray-400">
          Click "Sync User Types" to fetch from upstream, then use "Get Sub Types" per row.
        </div>
      )}
    </div>
  )
}
