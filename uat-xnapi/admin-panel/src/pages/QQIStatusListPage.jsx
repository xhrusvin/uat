import { useState, useEffect } from 'react'
import { usersClient } from '../services/api'

export default function QQIStatusListPage() {
  const [items, setItems]     = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)
  const [meta, setMeta]       = useState(null)

  const load = async () => {
    setLoading(true); setError(null)
    try {
      const { data } = await usersClient.get('/common/qqi-status-list')
      const list = Array.isArray(data.data) ? data.data : []
      setItems(list)
      setMeta({ status: data.upstream_status, total: list.length })
    } catch (err) {
      setError(err?.response?.data?.detail || 'Failed to load QQI status list')
    } finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  // Detect object shape from first item
  const keys = items.length > 0 ? Object.keys(items[0]) : []

  return (
    <div className="p-8">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API Calls</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>QQI Status List</span>
      </div>

      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">QQI Status List</h1>
          <p className="text-sm text-gray-500 mt-1">
            {loading ? 'Loading…' : `${items.length} status${items.length !== 1 ? 'es' : ''} from upstream`}
            {meta && !loading && (
              <span className="ml-2 px-1.5 py-0.5 rounded text-xs bg-gray-100 text-gray-500">
                HTTP {meta.status}
              </span>
            )}
          </p>
        </div>
        <button onClick={load} disabled={loading}
          className="btn-secondary flex items-center gap-2 py-2">
          <svg className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
          </svg>
          Refresh
        </button>
      </div>

      {error && (
        <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg flex items-center justify-between text-sm text-red-700">
          <span>{error}</span>
          <button onClick={load} className="ml-4 font-medium underline">Retry</button>
        </div>
      )}

      <div className="card overflow-hidden">
        {loading ? (
          <div className="px-5 py-12 text-center text-gray-400">Loading…</div>
        ) : items.length === 0 ? (
          <div className="px-5 py-12 text-center text-gray-400">No data returned</div>
        ) : keys.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b border-gray-100">
                <tr>
                  {keys.map(k => (
                    <th key={k} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                      {k.replace(/_/g, ' ')}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {items.map((item, i) => (
                  <tr key={i} className="hover:bg-gray-50/50">
                    {keys.map(k => (
                      <td key={k} className="px-4 py-3 text-gray-700">
                        {item[k] === null || item[k] === undefined
                          ? <span className="text-gray-300">—</span>
                          : typeof item[k] === 'boolean'
                            ? <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${item[k] ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}`}>
                                {item[k] ? 'Yes' : 'No'}
                              </span>
                            : typeof item[k] === 'object'
                              ? <span className="text-xs text-gray-400 font-mono">{JSON.stringify(item[k])}</span>
                              : String(item[k])
                        }
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          // Raw data if not array of objects
          <pre className="p-5 text-xs text-gray-600 font-mono overflow-x-auto">
            {JSON.stringify(items, null, 2)}
          </pre>
        )}
      </div>
    </div>
  )
}
