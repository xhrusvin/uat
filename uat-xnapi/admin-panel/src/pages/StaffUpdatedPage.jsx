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

// ── Detail Modal ───────────────────────────────────────────────────────────────
function DetailModal({ row, onClose }) {
  if (!row) return null

  let parsed = null
  try { parsed = JSON.parse(row.staff_api_response) } catch {}
  const d    = parsed?.data    || {}
  const sync = parsed?.sync    || {}

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-2xl max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 flex-shrink-0">
          <div>
            <h2 className="text-base font-semibold text-gray-900">
              {d.first_name} {d.last_name}
            </h2>
            <p className="text-xs text-gray-400 mt-0.5">{d.email} · {d.user_type}</p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
            </svg>
          </button>
        </div>

        {/* Scrollable body */}
        <div className="overflow-y-auto flex-1 px-6 py-4 space-y-4 text-sm">
          {/* Webhook meta */}
          <section>
            <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Webhook</h3>
            <Row label="User ID"      value={row.user_id} />
            <Row label="Uploaded"     value={timeAgo(row.uploaded_at)} />
            <Row label="Country"      value={row.country} />
            <Row label="API Status"   value={<ApiStatusBadge text={row.staff_api_status}/>} />
            <Row label="Sync Action"  value={sync.action} />
          </section>

          {/* Staff data */}
          {d.first_name && (
            <section>
              <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Staff Details</h3>
              <Row label="Name"          value={`${d.first_name} ${d.last_name}`} />
              <Row label="Email"         value={d.email} />
              <Row label="Phone"         value={d.phone_number} />
              <Row label="User Type"     value={d.user_type} />
              <Row label="Status"        value={d.status} />
              <Row label="Recruit Status" value={d.recruitment_status} />
              <Row label="DOB"           value={d.dob} />
              <Row label="Address"       value={d.address} />
              <Row label="EIR Code"      value={d.eir_code} />
              <Row label="Experience"    value={d.experience_year != null ? `${d.experience_year}y ${d.experience_month || 0}m` : null} />
              <Row label="Company"       value={d.company_name} />
              <Row label="Job Title"     value={d.job_title} />
              <Row label="PPS Number"    value={d.pps_number} />
              <Row label="Uniform Size"  value={d.uniform_size} />
            </section>
          )}

          {/* Tags */}
          {d.tags && d.tags.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Tags</h3>
              <div className="flex flex-wrap gap-1.5">
                {d.tags.map(t => (
                  <span key={t.id} className="px-2 py-0.5 rounded-full text-xs font-medium bg-orange-100 text-orange-700">{t.name}</span>
                ))}
              </div>
            </section>
          )}

          {/* Fields updated */}
          {sync.fields_updated && sync.fields_updated.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
                Fields Updated ({sync.fields_updated.length})
              </h3>
              <div className="flex flex-wrap gap-1.5">
                {sync.fields_updated.map(f => (
                  <span key={f} className="px-2 py-0.5 rounded text-xs bg-blue-50 text-blue-600">{f}</span>
                ))}
              </div>
            </section>
          )}

          {/* References */}
          {d.references && d.references.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">References</h3>
              {d.references.map(r => (
                <div key={r.id} className="mb-2 p-2 bg-gray-50 rounded-lg text-xs">
                  <p className="font-medium text-gray-800">{r.name} — {r.job_role} @ {r.organization}</p>
                  <p className="text-gray-500">{r.email} · Status: {r.status}</p>
                </div>
              ))}
            </section>
          )}
        </div>

        <div className="px-6 py-3 border-t border-gray-100 flex justify-end flex-shrink-0">
          <button onClick={onClose} className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50">Close</button>
        </div>
      </div>
    </div>
  )
}

// ── Page ───────────────────────────────────────────────────────────────────────
export default function StaffUpdatedPage() {
  const [rows, setRows]         = useState([])
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState(null)
  const [page, setPage]         = useState(1)
  const [total, setTotal]       = useState(0)
  const [selected, setSelected] = useState(null)
  const perPage = 20

  const load = async (p = 1) => {
    setLoading(true)
    setError(null)
    try {
      const { data } = await usersApi.staffUpdated({ page: p, per_page: perPage })
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
        <span>Staff Updated</span>
      </div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Staff Updated</h1>
          <p className="text-sm text-gray-500 mt-1">{total} records in staff_updated</p>
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
              {['Name','Email','User Type','Status','Recruit Status','API Status','Uploaded',''].map(h => (
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
                <td className="px-4 py-3 font-medium text-gray-900">{r.name || <span className="text-gray-400 text-xs">—</span>}</td>
                <td className="px-4 py-3 text-gray-500 text-xs">{r.email || '—'}</td>
                <td className="px-4 py-3 text-gray-600 text-xs">{r.user_type || '—'}</td>
                <td className="px-4 py-3 text-xs">
                  {r.status ? <span className={`px-2 py-0.5 rounded-full font-medium ${r.status === 'Enabled' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}`}>{r.status}</span> : '—'}
                </td>
                <td className="px-4 py-3 text-gray-500 text-xs">{r.recruitment_status || '—'}</td>
                <td className="px-4 py-3"><ApiStatusBadge text={r.staff_api_status}/></td>
                <td className="px-4 py-3 text-gray-500 text-xs">{timeAgo(r.uploaded_at)}</td>
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
