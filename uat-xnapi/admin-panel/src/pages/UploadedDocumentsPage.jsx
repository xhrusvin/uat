import { useState, useEffect } from 'react'
import { usersApi, recruitmentsApi } from '../services/api'

function timeAgo(ts) {
  if (!ts) return '—'
  const diff = Math.floor((Date.now() - new Date(ts)) / 1000)
  if (diff < 60)   return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff/60)}m ago`
  if (diff < 86400) return `${Math.floor(diff/3600)}h ago`
  return new Date(ts).toLocaleDateString('en-GB', { day:'2-digit', month:'short', year:'numeric' })
}

function formatDate(ts) {
  if (!ts) return null
  const d = new Date(ts.toString().replace(' ', 'T'))
  if (isNaN(d)) return ts
  return d.toLocaleString('en-GB', {
    day:'2-digit', month:'short', year:'numeric', hour:'2-digit', minute:'2-digit'
  })
}

function StatusBadge({ status }) {
  const map = {
    uploaded:     'bg-green-100 text-green-700',
    pending:      'bg-yellow-100 text-yellow-700',
    failed:       'bg-red-100 text-red-600',
    processed:    'bg-blue-100 text-blue-700',
    ai_accepted:  'bg-green-100 text-green-700',
    ai_rejected:  'bg-red-100 text-red-600',
    ai_review:    'bg-yellow-100 text-yellow-700',
    accepted:     'bg-green-100 text-green-700',
    rejected:     'bg-red-100 text-red-600',
  }
  const key = status?.toLowerCase()
  const cls = map[key] || 'bg-gray-100 text-gray-500'
  const label = status ? status.replace(/_/g, ' ') : '—'
  return <span className={`px-2 py-0.5 rounded-full text-xs font-medium capitalize ${cls}`}>{label}</span>
}

// ── Document details modal ────────────────────────────────────────────────────
function DocumentDetailsModal({ row, onClose }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      setLoading(true)
      setError(null)
      try {
        const res = await recruitmentsApi.userDocumentList(row.user_id, row.document_id)
        if (cancelled) return
        const body = res.data
        if (body?.success === false) {
          setError(body.message || 'Failed to load document details')
        } else {
          setData(body?.data || null)
        }
      } catch {
        if (!cancelled) setError('Failed to load document details')
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => { cancelled = true }
  }, [row.user_id, row.document_id])

  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])

  const documents = data?.documents || []

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/40"
         onClick={onClose}>
      <div className="bg-white rounded-xl shadow-xl w-full max-w-2xl max-h-[85vh] flex flex-col"
           onClick={(e) => e.stopPropagation()}>

        {/* Header */}
        <div className="flex items-start justify-between px-6 py-4 border-b border-gray-100">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Document Details</h2>
            <p className="text-xs text-gray-400 mt-0.5 font-mono">
              Staff ID: {row.user_id || '—'}
            </p>
          </div>
          <button onClick={onClose}
            className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-6 py-5">
          {loading && (
            <div className="py-12 text-center text-gray-400 text-sm">Loading document details…</div>
          )}

          {!loading && error && (
            <div className="px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
              {error}
            </div>
          )}

          {!loading && !error && (
            <>
              {/* Staff info */}
              <div className="grid grid-cols-2 gap-4 pb-5 mb-5 border-b border-gray-100">
                <div>
                  <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-1">Name</p>
                  <p className="text-sm font-medium text-gray-900">
                    {data?.name || <span className="text-gray-400">—</span>}
                  </p>
                </div>
                <div>
                  <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-1">Email</p>
                  <p className="text-sm text-gray-700 break-all">
                    {data?.email || <span className="text-gray-400">—</span>}
                  </p>
                </div>
              </div>

              {/* Documents */}
              <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-3">
                Documents ({documents.length})
              </p>

              {documents.length === 0 ? (
                <div className="py-8 text-center text-gray-400 text-sm">No documents found</div>
              ) : (
                <div className="space-y-3">
                  {documents.map((d, i) => (
                    <div key={d.document_id || i}
                         className="border border-gray-200 rounded-lg p-4 hover:border-gray-300 transition-colors">
                      <div className="flex items-start justify-between gap-3 mb-3">
                        <div>
                          <p className="text-sm font-semibold text-gray-900">
                            {d.document_type_name || <span className="text-gray-400">Untitled document</span>}
                          </p>
                          {d.sub_type_name && (
                            <p className="text-xs text-gray-500 mt-0.5">{d.sub_type_name}</p>
                          )}
                        </div>
                        <StatusBadge status={d.status} />
                      </div>

                      <div className="grid grid-cols-2 gap-3 text-xs mb-3">
                        <div>
                          <span className="text-gray-400">Expiry date: </span>
                          <span className="text-gray-700">{formatDate(d.expiry_date) || '—'}</span>
                        </div>
                        <div>
                          <span className="text-gray-400">Last updated: </span>
                          <span className="text-gray-700">{formatDate(d.updated_at) || '—'}</span>
                        </div>
                      </div>

                      {d.url ? (
                        <a href={d.url} target="_blank" rel="noopener noreferrer"
                           className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium text-white"
                           style={{ backgroundColor: '#1e7a38' }}>
                          <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/>
                          </svg>
                          View Document
                        </a>
                      ) : (
                        <span className="text-xs text-gray-400">No document URL available</span>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-3 border-t border-gray-100 flex justify-end">
          <button onClick={onClose}
            className="px-4 py-2 rounded-lg text-sm font-medium text-gray-600 hover:bg-gray-100">
            Close
          </button>
        </div>
      </div>
    </div>
  )
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function UploadedDocumentsPage() {
  const [rows, setRows]       = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)
  const [page, setPage]       = useState(1)
  const [total, setTotal]     = useState(0)
  const [selected, setSelected] = useState(null)
  const perPage = 20

  const fetch = async (p = 1) => {
    setLoading(true)
    setError(null)
    try {
      const { data } = await usersApi.documentUploaded({
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
        <span>Documents</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Uploaded Documents</span>
      </div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Uploaded Documents</h1>
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
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wide">Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {loading ? (
              <tr><td colSpan={7} className="px-4 py-12 text-center text-gray-400 text-sm">Loading…</td></tr>
            ) : rows.length === 0 ? (
              <tr><td colSpan={7} className="px-4 py-12 text-center text-gray-400 text-sm">No documents found</td></tr>
            ) : rows.map((r, i) => (
              <tr key={r.id || i} className="hover:bg-gray-50/50">
                <td className="px-4 py-3 font-medium text-gray-900">{r.name || <span className="text-gray-400">—</span>}</td>
                <td className="px-4 py-3 text-gray-500 text-xs">{r.email || '—'}</td>
                <td className="px-4 py-3 text-gray-400 text-xs font-mono">{r.user_id}</td>
                <td className="px-4 py-3 text-gray-400 text-xs font-mono">{r.document_id}</td>
                <td className="px-4 py-3 text-gray-500 text-xs">{timeAgo(r.uploaded_at)}</td>
                <td className="px-4 py-3"><StatusBadge status={r.status}/></td>
                <td className="px-4 py-3 text-right">
                  <button
                    onClick={() => setSelected(r)}
                    disabled={!r.user_id}
                    title={r.user_id ? 'View document details' : 'No user id on this record'}
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium
                               border border-gray-200 text-gray-600 hover:bg-gray-50 hover:text-gray-900
                               disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
                    <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/>
                    </svg>
                    View
                  </button>
                </td>
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

      {selected && (
        <DocumentDetailsModal row={selected} onClose={() => setSelected(null)} />
      )}
    </div>
  )
}
