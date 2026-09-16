import { useState } from 'react'
import { commonApi } from '../services/api'

// ── Status badge ──────────────────────────────────────────────────────────────
function StatusBadge({ value }) {
  const active = value === 1
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium
                      ${active ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}`}>
      <span className={`w-1.5 h-1.5 rounded-full ${active ? 'bg-green-500' : 'bg-gray-400'}`} />
      {active ? 'Active' : 'Inactive'}
    </span>
  )
}

// ── Sync stats bar ────────────────────────────────────────────────────────────
function SyncStats({ sync, total }) {
  if (!sync) return null
  return (
    <div className="flex flex-wrap gap-3 mt-3 pt-3 border-t border-gray-100">
      <Stat label="Fetched"  value={sync.fetched  ?? total ?? '—'} color="blue" />
      <Stat label="Inserted" value={sync.inserted ?? '—'}         color="green" />
      <Stat label="Updated"  value={sync.updated  ?? '—'}         color="amber" />
      {sync.skipped != null && (
        <Stat label="Skipped" value={sync.skipped} color="gray" />
      )}
    </div>
  )
}

function Stat({ label, value, color }) {
  const colors = {
    blue:  'bg-blue-50  text-blue-700  border-blue-100',
    green: 'bg-green-50 text-green-700 border-green-100',
    amber: 'bg-amber-50 text-amber-700 border-amber-100',
    gray:  'bg-gray-50  text-gray-600  border-gray-200',
  }
  return (
    <div className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-xs font-medium ${colors[color]}`}>
      <span className="text-xs opacity-60">{label}</span>
      <span className="font-bold">{value}</span>
    </div>
  )
}

// ── Table view ────────────────────────────────────────────────────────────────
function TableView({ data }) {
  const [filter, setFilter] = useState('')
  const [statusFilter, setStatusFilter] = useState('all') // 'all' | 'active' | 'inactive'

  const list = Array.isArray(data) ? data : []

  const filtered = list.filter(row => {
    const matchStatus =
      statusFilter === 'all'
        ? true
        : statusFilter === 'active'
        ? row.status === 1
        : row.status !== 1

    const q = filter.toLowerCase()
    const matchText =
      !q ||
      (row.name  || '').toLowerCase().includes(q) ||
      (row.email || '').toLowerCase().includes(q) ||
      (row.id    || '').toLowerCase().includes(q)

    return matchStatus && matchText
  })

  if (list.length === 0) return null

  const activeCount   = list.filter(r => r.status === 1).length
  const inactiveCount = list.length - activeCount

  return (
    <div>
      {/* Filters */}
      <div className="px-5 py-3 border-b border-gray-100 flex flex-wrap items-center gap-3">
        {/* Search */}
        <div className="relative flex-1 min-w-[180px] max-w-sm">
          <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400"
            fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            type="text"
            value={filter}
            onChange={e => setFilter(e.target.value)}
            placeholder="Search name, email, ID…"
            className="w-full pl-9 pr-3 py-1.5 text-xs border border-gray-200 rounded-lg
                       focus:outline-none focus:ring-2 focus:ring-green-200 focus:border-green-400"
          />
        </div>

        {/* Status toggle */}
        <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1">
          {[
            { val: 'all',      label: `All (${list.length})` },
            { val: 'active',   label: `Active (${activeCount})` },
            { val: 'inactive', label: `Inactive (${inactiveCount})` },
          ].map(({ val, label }) => (
            <button
              key={val}
              onClick={() => setStatusFilter(val)}
              className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors
                          ${statusFilter === val
                            ? 'bg-white text-gray-900 shadow-sm'
                            : 'text-gray-500 hover:text-gray-700'}`}
            >
              {label}
            </button>
          ))}
        </div>

        <span className="text-xs text-gray-400 ml-auto">
          {filtered.length} of {list.length}
        </span>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              <th className="text-left px-5 py-2.5 text-xs font-medium text-gray-500 uppercase tracking-wide">#</th>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase tracking-wide">Name</th>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase tracking-wide">Email</th>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase tracking-wide">ID</th>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-5 py-8 text-center text-sm text-gray-400 italic">
                  No records match your filter.
                </td>
              </tr>
            ) : (
              filtered.map((row, i) => (
                <tr key={row.id || i} className="hover:bg-gray-50 transition-colors">
                  <td className="px-5 py-2.5 text-xs text-gray-400 tabular-nums">{i + 1}</td>
                  <td className="px-4 py-2.5 font-medium text-gray-800 text-sm">{row.name || '—'}</td>
                  <td className="px-4 py-2.5 text-gray-600 text-sm">
                    <a href={`mailto:${row.email}`}
                       className="hover:text-green-600 hover:underline transition-colors">
                      {row.email || '—'}
                    </a>
                  </td>
                  <td className="px-4 py-2.5">
                    <code className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded font-mono break-all">
                      {row.id || '—'}
                    </code>
                  </td>
                  <td className="px-4 py-2.5">
                    <StatusBadge value={row.status} />
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── JSON viewer ───────────────────────────────────────────────────────────────
function JsonViewer({ data }) {
  if (data === null || data === undefined) return <span className="text-gray-400 italic">null</span>
  if (typeof data === 'boolean') return <span className="text-purple-600">{String(data)}</span>
  if (typeof data === 'number')  return <span className="text-blue-600">{data}</span>
  if (typeof data === 'string')  return <span className="text-green-700">"{data}"</span>
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

// ── Page ──────────────────────────────────────────────────────────────────────
export default function AdminUserListCallPage() {
  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)
  const [view,    setView]    = useState('table') // 'table' | 'json'

  const handleCall = async () => {
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      const { data } = await commonApi.administrationUserList()
      setResult(data)
      if (data.success === false) setError(data.message || 'API returned an error')
    } catch (err) {
      setError(
        err.response?.data?.message ||
        err.response?.data?.detail  ||
        err.message ||
        'Request failed'
      )
    } finally {
      setLoading(false)
    }
  }

  const tableData    = result?.data
  const canShowTable = Array.isArray(tableData) && tableData.length > 0

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API Calls</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>Administration Users</span>
      </div>

      <h1 className="text-2xl font-bold text-gray-900 mb-1">Administration User List</h1>
      <p className="text-sm text-gray-500 mb-6">
        Fetches all administration users from the XpressHealth User API and syncs them to
        the <code className="text-xs bg-gray-100 px-1.5 py-0.5 rounded font-mono">session_users</code> collection.
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
                {`${import.meta.env.VITE_API_URL || ''}/common/administration-user-list`}
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
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1.5 bg-purple-50 border border-purple-200 rounded-lg text-purple-600">
                Streams NDJSON → syncs to DB
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
            {loading ? 'Fetching…' : 'Call API'}
          </button>
        </div>

        {/* Sync stats shown after a successful call */}
        {result?.sync && <SyncStats sync={result.sync} total={result.total} />}
      </div>

      {/* Error */}
      {error && (
        <div className="mb-5 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700 flex items-start gap-2">
          <svg className="w-4 h-4 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd"
              d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z"
              clipRule="evenodd" />
          </svg>
          <span>{error}</span>
        </div>
      )}

      {/* Response */}
      {result && (
        <div className="card overflow-hidden">
          {/* Response header */}
          <div className="px-5 py-3.5 border-b border-gray-200 flex items-center justify-between flex-wrap gap-3">
            <div className="flex items-center gap-3 flex-wrap">
              <span className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold font-mono
                                ${result.success ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                {result.status_code ?? (result.success ? 200 : 'ERR')}
              </span>
              <span className="text-sm text-gray-600">
                {result.message || (result.success ? 'OK' : 'Error')}
              </span>
              {result.total != null && (
                <span className="text-xs text-gray-400">
                  {result.total} user{result.total !== 1 ? 's' : ''}
                </span>
              )}
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
                              ${view === 'table'
                                ? 'bg-white text-gray-900 shadow-sm'
                                : 'text-gray-500 hover:text-gray-700'}`}
                >
                  Table
                </button>
                <button
                  onClick={() => setView('json')}
                  className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors
                              ${view === 'json'
                                ? 'bg-white text-gray-900 shadow-sm'
                                : 'text-gray-500 hover:text-gray-700'}`}
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
            <div className="p-5 bg-gray-50 overflow-auto max-h-[600px]">
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
              d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
          </svg>
          <p className="text-sm text-gray-400">
            Click <strong>Call API</strong> to fetch and sync administration users
          </p>
          <p className="text-xs text-gray-300 mt-1">
            Results are streamed as NDJSON and upserted into <code className="font-mono">session_users</code>
          </p>
        </div>
      )}
    </div>
  )
}
