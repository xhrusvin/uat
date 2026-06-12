import { useState } from 'react'
import { commonApi } from '../services/api'

function JsonViewer({ data }) {
  if (data === null || data === undefined) return <span className="text-gray-400 italic">null</span>
  if (typeof data === 'boolean') return <span className="text-purple-600">{String(data)}</span>
  if (typeof data === 'number') return <span className="text-blue-600">{data}</span>
  if (typeof data === 'string') return <span className="text-green-700">"{data}"</span>
  if (Array.isArray(data)) {
    if (data.length === 0) return <span className="text-gray-400">[]</span>
    return (
      <div className="ml-4">
        <span className="text-gray-500">[</span>
        {data.map((item, i) => (
          <div key={i} className="ml-4">
            <JsonViewer data={item} />
            {i < data.length - 1 && <span className="text-gray-400">,</span>}
          </div>
        ))}
        <span className="text-gray-500">]</span>
      </div>
    )
  }
  if (typeof data === 'object') {
    const keys = Object.keys(data)
    if (keys.length === 0) return <span className="text-gray-400">{'{}'}</span>
    return (
      <div className="ml-4">
        <span className="text-gray-500">{'{'}</span>
        {keys.map((key, i) => (
          <div key={key} className="ml-4">
            <span className="text-red-600">"{key}"</span>
            <span className="text-gray-600">: </span>
            <JsonViewer data={data[key]} />
            {i < keys.length - 1 && <span className="text-gray-400">,</span>}
          </div>
        ))}
        <span className="text-gray-500">{'}'}</span>
      </div>
    )
  }
  return <span>{String(data)}</span>
}

function TableView({ data }) {
  // Try to render as a table if it's an array of objects
  const list = Array.isArray(data) ? data
    : Array.isArray(data?.data) ? data.data
    : null

  if (!list || list.length === 0 || typeof list[0] !== 'object') return null

  const keys = Object.keys(list[0])

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200 bg-gray-50">
            {keys.map(k => (
              <th key={k} className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase tracking-wide">
                {k}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {list.map((row, i) => (
            <tr key={i} className="hover:bg-gray-50">
              {keys.map(k => (
                <td key={k} className="px-4 py-2.5 text-gray-700 text-sm">
                  {row[k] === null || row[k] === undefined
                    ? <span className="text-gray-300 italic text-xs">—</span>
                    : typeof row[k] === 'boolean'
                    ? <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${row[k] ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>{String(row[k])}</span>
                    : String(row[k])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function ClientTypeListPage() {
  const [result, setResult]   = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState(null)
  const [view, setView]       = useState('table') // 'table' | 'json'

  const handleCall = async () => {
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      const { data } = await commonApi.clientTypeList()
      setResult(data)
      if (data.success === false) setError(data.message || 'API returned an error')
    } catch (err) {
      setError(err.response?.data?.message || err.response?.data?.detail || err.message || 'Request failed')
    } finally {
      setLoading(false)
    }
  }

  const tableData = result?.data
  const canShowTable = Array.isArray(tableData) || Array.isArray(tableData?.data)

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API Calls</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>Client Type List</span>
      </div>

      <h1 className="text-2xl font-bold text-gray-900 mb-1">Client Type List</h1>
      <p className="text-sm text-gray-500 mb-6">
        Fetches client types from the XpressHealth User API.
      </p>

      {/* Request info card */}
      <div className="card p-5 mb-6">
        <div className="flex items-start justify-between gap-6 flex-wrap">
          <div className="space-y-3 flex-1 min-w-0">
            <div className="flex items-center gap-3">
              <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold bg-green-100 text-green-700 font-mono">
                GET
              </span>
              <code className="text-sm text-gray-700 bg-gray-100 px-3 py-1 rounded-lg font-mono break-all">
                {`${import.meta.env.VITE_API_URL || ''}/common/client-type-list`}
              </code>
            </div>
            <div className="flex flex-wrap gap-2 text-xs">
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1.5 bg-gray-50 border border-gray-200 rounded-lg font-mono text-gray-600">
                <span className="text-gray-400">Api-Key:</span>
                <span className="text-gray-800">••••••••••</span>
              </span>
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1.5 bg-gray-50 border border-gray-200 rounded-lg font-mono text-gray-600">
                <span className="text-gray-400">X-App-Country:</span>
                <span className="text-gray-800">{import.meta.env.VITE_APP_COUNTRY || 'ie'}</span>
              </span>
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1.5 bg-blue-50 border border-blue-200 rounded-lg text-blue-600">
                No auth required
              </span>
            </div>
          </div>

          <button
            onClick={handleCall}
            disabled={loading}
            className="flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-medium
                       text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex-shrink-0"
            style={{ backgroundColor: '#1e7a38' }}
          >
            {loading ? (
              <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
            ) : (
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            )}
            {loading ? 'Calling…' : 'Call API'}
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="mb-5 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700 flex items-start gap-2">
          <svg className="w-4 h-4 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
          </svg>
          <span>{error}</span>
        </div>
      )}

      {/* Response */}
      {result && (
        <div className="card overflow-hidden">
          {/* Response header */}
          <div className="px-5 py-3.5 border-b border-gray-200 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <span className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold font-mono
                                ${result.success ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                {result.status_code}
              </span>
              <span className="text-sm text-gray-600">
                {result.message || (result.success ? 'OK' : 'Error')}
              </span>
              {result.upstream_url && (
                <code className="text-xs text-gray-400 font-mono hidden lg:block truncate max-w-xs">
                  {result.upstream_url}
                </code>
              )}
            </div>

            {/* View toggle */}
            {canShowTable && (
              <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1">
                <button
                  onClick={() => setView('table')}
                  className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors
                              ${view === 'table' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
                >
                  Table
                </button>
                <button
                  onClick={() => setView('json')}
                  className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors
                              ${view === 'json' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
                >
                  JSON
                </button>
              </div>
            )}
          </div>

          {/* Response body */}
          {view === 'table' && canShowTable ? (
            <TableView data={tableData} />
          ) : (
            <div className="p-5 bg-gray-50 overflow-auto max-h-[500px]">
              <pre className="text-xs font-mono text-gray-700 leading-relaxed">
                <JsonViewer data={result.data} />
              </pre>
            </div>
          )}
        </div>
      )}

      {/* Empty state */}
      {!result && !loading && !error && (
        <div className="card p-12 text-center">
          <svg className="w-12 h-12 mx-auto mb-4 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          <p className="text-sm text-gray-400">Click <strong>Call API</strong> to fetch client types</p>
        </div>
      )}
    </div>
  )
}
