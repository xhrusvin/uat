import { useState, useEffect } from 'react'
import { usersApi } from '../services/api'

function timeAgo(ts) {
  if (!ts) return '—'
  const diff = Math.floor((Date.now() - new Date(ts)) / 1000)
  if (diff < 60)    return `${diff}s ago`
  if (diff < 3600)  return `${Math.floor(diff/60)}m ago`
  if (diff < 86400) return `${Math.floor(diff/3600)}h ago`
  return new Date(ts).toLocaleDateString('en-GB', { day:'2-digit', month:'short', year:'numeric' })
}

function StatusBadge({ text }) {
  const n = parseInt(text)
  if (n >= 200 && n < 300) return <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">{text}</span>
  if (n >= 400) return <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-600">{text}</span>
  return <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-500">{text || '—'}</span>
}

function SyncStatusBadge({ status }) {
  const map = { '1': 'bg-green-100 text-green-700', '0': 'bg-red-100 text-red-600', 'failed': 'bg-red-100 text-red-600' }
  return <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${map[status] || 'bg-gray-100 text-gray-500'}`}>
    {status === '1' ? 'Success' : status === '0' ? 'Failed' : status || '—'}
  </span>
}

// ── Detail Modal ───────────────────────────────────────────────────────────────
function DetailModal({ row, onClose }) {
  if (!row) return null

  let parsed = null
  try { parsed = JSON.parse(row.sync_api_response) } catch {}
  const d = parsed?.data

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-lg mx-4 overflow-hidden">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">Shift Updated Detail</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
            </svg>
          </button>
        </div>
        <div className="px-6 py-4 space-y-3 text-sm">
          <Row label="Shift ID"     value={row.shift_id} />
          <Row label="Uploaded"     value={timeAgo(row.uploaded_at)} />
          <Row label="Country"      value={row.country} />
          <Row label="Status"       value={<SyncStatusBadge status={row.status}/>} />
          <Row label="API Status"   value={<StatusBadge text={row.sync_api_status}/>} />
          {d && <>
            <div className="border-t border-gray-100 pt-3 font-medium text-gray-700">Shift Data</div>
            <Row label="Shift Code"  value={d.shift_code} />
            <Row label="Date"        value={d.date} />
            <Row label="Time"        value={d.start_time && d.end_time ? `${d.start_time} – ${d.end_time}` : '—'} />
            <Row label="User Type"   value={d.user_type} />
            <Row label="Status"      value={d.status} />
            <Row label="Client"      value={d.client} />
            <Row label="Staff"       value={d.staff || '—'} />
            <Row label="Premium"     value={d.is_premium ? 'Yes' : 'No'} />
          </>}
          {!d && row.sync_api_response && (
            <div>
              <p className="text-xs text-gray-400 mb-1">Raw Response</p>
              <pre className="text-xs bg-gray-50 rounded p-2 overflow-auto max-h-40 text-gray-600">{row.sync_api_response}</pre>
            </div>
          )}
        </div>
        <div className="px-6 py-3 border-t border-gray-100 flex justify-end">
          <button onClick={onClose} className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50">Close</button>
        </div>
      </div>
    </div>
  )
}

function Row({ label, value }) {
  return (
    <div className="flex items-center gap-2">
      <span className="text-xs text-gray-400 w-28 flex-shrink-0">{label}</span>
      <span className="text-gray-800 font-medium">{value ?? '—'}</span>
    </div>
  )
}

// ── Page ───────────────────────────────────────────────────────────────────────
export default function ShiftUpdatedPage() {
  const [rows, setRows]       = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)
  const [page, setPage]       = useState(1)
  const [total, setTotal]     = useState(0)
  const [selected, setSelected] = useState(null)
  const perPage = 20

  const load = async (p = 1) => {
    setLoading(true)
    setError(null)
    try {
      const { data } = await usersApi.shiftUpdated({ page: p, per_page: perPage })
      setRows(data.data || [])
      setTotal(data.total || 0)
      setPage(p)
    } catch {
      setError('Failed to load records')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load(1) }, [])

  const totalPages = Math.ceil(total / perPage)

  return (
    <div className="p-8 max-w-6xl">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Webhook Monitor</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Shift Updated</span>
      </div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Shift Updated</h1>
          <p className="text-sm text-gray-500 mt-1">{total} records in shift_updated</p>
        </div>
        <button onClick={() => load(page)}
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
              {['Shift ID','Shift Code','Client','Date','API Status','Uploaded','Status',''].map(h => (
                <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {loading ? (
              <tr><td colSpan={8} className="px-4 py-12 text-center text-gray-400 text-sm">Loading…</td></tr>
            ) : rows.length === 0 ? (
              <tr><td colSpan={8} className="px-4 py-12 text-center text-gray-400 text-sm">No records found</td></tr>
            ) : rows.map((r, i) => (
              <tr key={r.id || i} className="hover:bg-gray-50/50">
                <td className="px-4 py-3 font-mono text-xs text-gray-500">{r.shift_id?.slice(-8)}…</td>
                <td className="px-4 py-3 font-medium text-gray-900">{r.shift_code || '—'}</td>
                <td className="px-4 py-3 text-gray-600 text-xs max-w-[180px] truncate">{r.client || '—'}</td>
                <td className="px-4 py-3 text-gray-500 text-xs">{r.date || '—'}</td>
                <td className="px-4 py-3"><StatusBadge text={r.sync_api_status}/></td>
                <td className="px-4 py-3 text-gray-500 text-xs">{timeAgo(r.uploaded_at)}</td>
                <td className="px-4 py-3"><SyncStatusBadge status={r.status}/></td>
                <td className="px-4 py-3">
                  <button onClick={() => setSelected(r)}
                    className="px-3 py-1 text-xs rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-600">
                    View
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
            <span>Page {page} of {totalPages}</span>
            <div className="flex gap-2">
              <button onClick={() => load(page - 1)} disabled={page <= 1}
                className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">←</button>
              <button onClick={() => load(page + 1)} disabled={page >= totalPages}
                className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">→</button>
            </div>
          </div>
        )}
      </div>

      <DetailModal row={selected} onClose={() => setSelected(null)} />
    </div>
  )
}
