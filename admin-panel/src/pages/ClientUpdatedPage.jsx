import { useState, useEffect } from 'react'
import { webhookApi } from '../services/api'

function timeAgo(ts) {
  if (!ts) return '—'
  const diff = Math.floor((Date.now() - new Date(ts)) / 1000)
  if (diff < 60)    return `${diff}s ago`
  if (diff < 3600)  return `${Math.floor(diff/60)}m ago`
  if (diff < 86400) return `${Math.floor(diff/3600)}h ago`
  return new Date(ts).toLocaleDateString('en-GB', { day:'2-digit', month:'short', year:'numeric' })
}

function ApiStatusBadge({ text }) {
  const n = parseInt(text)
  if (n >= 200 && n < 300) return <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">{text}</span>
  if (n >= 400) return <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-600">{text}</span>
  return <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-500">{text || '—'}</span>
}

function Row({ label, value }) {
  return (
    <div className="flex items-start gap-2 py-1.5 border-b border-gray-50 last:border-0">
      <span className="text-xs text-gray-400 w-36 flex-shrink-0 pt-0.5">{label}</span>
      <span className="text-sm text-gray-800 font-medium break-all">{value ?? '—'}</span>
    </div>
  )
}

function DetailModal({ row, onClose }) {
  if (!row) return null
  let parsed = null
  try { parsed = JSON.parse(row.sync_api_response) } catch {}
  const d    = parsed?.data || {}
  const sync = parsed?.sync || {}
  const c    = row.client   || {}

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-2xl max-h-[90vh] flex flex-col">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 flex-shrink-0">
          <div>
            <h2 className="text-base font-semibold text-gray-900">{c.name || d.name || 'Client'}</h2>
            <p className="text-xs text-gray-400 mt-0.5">{c.client_type} · {c.county}</p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
            </svg>
          </button>
        </div>
        <div className="overflow-y-auto flex-1 px-6 py-4 space-y-4">
          <div>
            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Webhook Record</p>
            <Row label="Client ID"        value={row.client_id} />
            <Row label="Received"         value={row.uploaded_at ? new Date(row.uploaded_at).toLocaleString('en-GB') : '—'} />
            <Row label="Country"          value={row.country} />
            <Row label="Sync API Status"  value={row.sync_api_status} />
          </div>
          {c.name && (
            <div>
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Client Details</p>
              <Row label="Name"        value={c.name} />
              <Row label="Address"     value={c.address} />
              <Row label="County"      value={c.county} />
              <Row label="Client Type" value={c.client_type} />
            </div>
          )}
          {sync.action && (
            <div>
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Sync Result</p>
              <Row label="Action"  value={sync.action} />
              <Row label="Message" value={sync.message} />
            </div>
          )}
          {row.sync_api_response && (
            <div>
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Raw Response</p>
              <pre className="text-xs bg-gray-50 rounded-lg p-3 overflow-auto max-h-48 text-gray-600 font-mono">
                {row.sync_api_response}
              </pre>
            </div>
          )}
        </div>
        <div className="px-6 py-4 border-t border-gray-100 flex-shrink-0">
          <button onClick={onClose} className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-600">Close</button>
        </div>
      </div>
    </div>
  )
}

export default function ClientUpdatedPage() {
  const [rows, setRows]         = useState([])
  const [total, setTotal]       = useState(0)
  const [page, setPage]         = useState(1)
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState(null)
  const [selected, setSelected] = useState(null)
  const perPage = 20

  const load = async (p = page) => {
    setLoading(true); setError(null)
    try {
      const { data } = await webhookApi.clientUpdated({ page: p, per_page: perPage })
      setRows(data.data || []); setTotal(data.total || 0); setPage(p)
    } catch { setError('Failed to load') }
    finally { setLoading(false) }
  }

  useEffect(() => { load(1) }, [])

  const totalPages = Math.ceil(total / perPage)

  return (
    <div className="p-8">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Webhook Monitor</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Client Updated</span>
      </div>

      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Client Updated</h1>
          <p className="text-sm text-gray-500 mt-1">{loading ? 'Loading…' : `${total} webhook event${total !== 1 ? 's' : ''}`}</p>
        </div>
        <button onClick={() => load(page)} disabled={loading}
          className="btn-secondary flex items-center gap-2 py-2">
          <svg className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
          </svg>
          Refresh
        </button>
      </div>

      {error && <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">{error}</div>}

      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                {['Client', 'Client ID', 'County', 'Type', 'API Status', 'Received', ''].map(h => (
                  <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {loading ? (
                Array.from({length: 5}).map((_, i) => (
                  <tr key={i}>{Array.from({length: 7}).map((_, j) => (
                    <td key={j} className="px-4 py-3"><div className="h-4 bg-gray-100 rounded animate-pulse"/></td>
                  ))}</tr>
                ))
              ) : rows.length === 0 ? (
                <tr><td colSpan={7} className="px-4 py-12 text-center text-gray-400">No client update events found</td></tr>
              ) : rows.map(row => (
                <tr key={row.id} className="hover:bg-gray-50/50 cursor-pointer" onClick={() => setSelected(row)}>
                  <td className="px-4 py-3 font-medium text-gray-900">{row.client?.name || '—'}</td>
                  <td className="px-4 py-3 text-xs font-mono text-gray-500">{row.client_id?.slice(-8)}…</td>
                  <td className="px-4 py-3 text-gray-500">{row.client?.county || '—'}</td>
                  <td className="px-4 py-3 text-gray-500">{row.client?.client_type || '—'}</td>
                  <td className="px-4 py-3"><ApiStatusBadge text={row.sync_api_status}/></td>
                  <td className="px-4 py-3 text-gray-400 text-xs">{timeAgo(row.uploaded_at)}</td>
                  <td className="px-4 py-3 text-right">
                    <svg className="w-4 h-4 text-gray-400 inline" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
                    </svg>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
            <span>Page {page} of {totalPages} · {total} total</span>
            <div className="flex gap-2">
              <button onClick={() => load(page-1)} disabled={page<=1} className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">←</button>
              <button onClick={() => load(page+1)} disabled={page>=totalPages} className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">→</button>
            </div>
          </div>
        )}
      </div>

      {selected && <DetailModal row={selected} onClose={() => setSelected(null)} />}
    </div>
  )
}
