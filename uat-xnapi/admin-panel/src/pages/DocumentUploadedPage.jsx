import { useState, useEffect } from 'react'
import { usersClient } from '../services/api'

function timeAgo(ts) {
  if (!ts) return '—'
  const diff = Math.floor((Date.now() - new Date(ts)) / 1000)
  if (diff < 60)   return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff/60)}m ago`
  if (diff < 86400) return `${Math.floor(diff/3600)}h ago`
  return new Date(ts).toLocaleDateString('en-GB', { day:'2-digit', month:'short', year:'numeric' })
}

function StatusBadge({ status }) {
  const map = {
    uploaded:  'bg-green-100 text-green-700',
    pending:   'bg-yellow-100 text-yellow-700',
    failed:    'bg-red-100 text-red-600',
    processed: 'bg-blue-100 text-blue-700',
  }
  const cls = map[status?.toLowerCase()] || 'bg-gray-100 text-gray-500'
  return <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>{status || '—'}</span>
}

export default function DocumentUploadedPage() {
  const [rows, setRows]       = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)
  const [page, setPage]       = useState(1)
  const [total, setTotal]     = useState(0)
  const perPage = 20

  const fetch = async (p = 1) => {
    setLoading(true)
    setError(null)
    try {
      const { data } = await usersClient.post('/webhook/document-uploaded', {
        page: p, per_page: perPage
      })
      setRows(data.data || [])
      setTotal(data.total || 0)
      setPage(p)
    } catch {
      setError('Failed to load documents')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { fetch(1) }, [])

  const totalPages = Math.ceil(total / perPage)

  return (
    <div className="p-8 max-w-6xl">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Webhook Monitor</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Document Uploaded</span>
      </div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Document Uploaded</h1>
          <p className="text-sm text-gray-500 mt-1">{total} records in uploaded_documents</p>
        </div>
        <button onClick={() => fetch(page)}
          className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white"
          style={{ backgroundColor: '#1e7a38' }}>
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
          </svg>
          Refresh
        </button>
      </div>

      {error && <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">{error}</div>}

      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              {['Staff Name','Email','User ID','Document ID','Uploaded','Status'].map(h => (
                <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {loading ? (
              <tr><td colSpan={6} className="px-4 py-12 text-center text-gray-400 text-sm">Loading…</td></tr>
            ) : rows.length === 0 ? (
              <tr><td colSpan={6} className="px-4 py-12 text-center text-gray-400 text-sm">No documents found</td></tr>
            ) : rows.map((r, i) => (
              <tr key={r.id || i} className="hover:bg-gray-50/50">
                <td className="px-4 py-3 font-medium text-gray-900">{r.name || <span className="text-gray-400">—</span>}</td>
                <td className="px-4 py-3 text-gray-500 text-xs">{r.email || '—'}</td>
                <td className="px-4 py-3 text-gray-400 text-xs font-mono">{r.user_id}</td>
                <td className="px-4 py-3 text-gray-400 text-xs font-mono">{r.document_id}</td>
                <td className="px-4 py-3 text-gray-500 text-xs">{timeAgo(r.uploaded_at)}</td>
                <td className="px-4 py-3"><StatusBadge status={r.status}/></td>
              </tr>
            ))}
          </tbody>
        </table>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
            <span>Page {page} of {totalPages}</span>
            <div className="flex gap-2">
              <button onClick={() => fetch(page - 1)} disabled={page <= 1}
                className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">←</button>
              <button onClick={() => fetch(page + 1)} disabled={page >= totalPages}
                className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">→</button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
