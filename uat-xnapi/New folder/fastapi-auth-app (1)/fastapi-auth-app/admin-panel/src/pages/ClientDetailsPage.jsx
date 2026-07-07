import { useState } from 'react'
import { commonApi } from '../services/api'

function Field({ label, value }) {
  if (value === null || value === undefined || value === '') return null
  const display = typeof value === 'boolean' ? (value ? 'Yes' : 'No')
    : typeof value === 'object' ? JSON.stringify(value, null, 2)
    : String(value)
  return (
    <div>
      <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-0.5">
        {label.replace(/_/g, ' ')}
      </p>
      {typeof value === 'object' ? (
        <pre className="text-xs font-mono text-gray-700 bg-gray-50 rounded p-2 overflow-auto max-h-32">
          {display}
        </pre>
      ) : (
        <p className="text-sm text-gray-900 break-all">{display}</p>
      )}
    </div>
  )
}

function Section({ title, children }) {
  const hasContent = Array.isArray(children) ? children.some(Boolean) : Boolean(children)
  if (!hasContent) return null
  return (
    <div className="mb-6">
      <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3
                     pb-2 border-b border-gray-100">{title}</h4>
      <div className="grid grid-cols-2 gap-4">{children}</div>
    </div>
  )
}

function Avatar({ name }) {
  const initials = name?.split(' ').map(p => p[0]).join('').toUpperCase().slice(0, 2) || '?'
  return (
    <div className="w-14 h-14 rounded-full flex items-center justify-center text-white text-xl font-bold flex-shrink-0"
         style={{ backgroundColor: '#1e7a38' }}>
      {initials}
    </div>
  )
}

export default function ClientDetailsPage() {
  const [clientId, setClientId] = useState('')
  const [loading, setLoading]   = useState(false)
  const [result, setResult]     = useState(null)
  const [error, setError]       = useState(null)

  const handleCall = async () => {
    if (!clientId.trim()) return
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      const { data } = await commonApi.clientDetail(clientId.trim())
      setResult(data)
      if (data.success === false) setError(data.message || 'API returned an error')
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Request failed')
    } finally {
      setLoading(false)
    }
  }

  const d = result?.data || {}

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API Calls</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>Client Details</span>
      </div>
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Client Details</h1>
      <p className="text-sm text-gray-500 mb-6">
        Fetch client details from the XpressHealth User API.
      </p>

      {/* Request card */}
      <div className="card p-5 mb-6">
        <div className="flex items-center gap-2 mb-4">
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold bg-blue-100 text-blue-700 font-mono">POST</span>
          <code className="text-xs text-gray-600 bg-gray-100 px-3 py-1.5 rounded-lg font-mono">
            {`${import.meta.env.VITE_API_URL || ''}/common/client-detail`}
          </code>
          <span className="text-xs text-gray-400 bg-yellow-50 border border-yellow-200 px-2 py-1 rounded-lg">
            Internal API Key
          </span>
        </div>

        <div className="flex gap-3 items-end">
          <div className="flex-1 max-w-sm">
            <label className="block text-xs font-medium text-gray-500 mb-1.5">
              Client ID <span className="text-gray-400 font-normal font-mono">("client_id" field)</span>
            </label>
            <input
              type="text"
              className="input font-mono text-sm"
              placeholder="69e6070cfee7f1df2600e583"
              value={clientId}
              onChange={e => setClientId(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleCall()}
            />
          </div>
          <button
            onClick={handleCall}
            disabled={loading || !clientId.trim()}
            className="flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-medium text-white
                       transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            style={{ backgroundColor: '#1e7a38' }}
          >
            {loading
              ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
              : <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                    d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"/>
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
                </svg>}
            {loading ? 'Fetching…' : 'Fetch'}
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="mb-5 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Response status */}
      {result && (
        <div className="mb-4 flex items-center gap-3">
          <span className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold font-mono
                            ${result.success ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
            {result.status_code}
          </span>
          <span className="text-sm text-gray-500">{result.message}</span>
          <code className="text-xs text-gray-400 font-mono hidden lg:block">{result.upstream_url}</code>
        </div>
      )}

      {/* Client detail card */}
      {result?.success && Object.keys(d).length > 0 && (
        <div className="card p-6">
          {/* Header */}
          <div className="flex items-center gap-4 mb-6">
            <Avatar name={d.name || d.title || '?'} />
            <div>
              <h2 className="text-xl font-bold text-gray-900">{d.name || d.title || '—'}</h2>
              <p className="text-sm text-gray-500">{d.client_type || d.type || '—'}</p>
              {d.is_active !== undefined && (
                <span className={`inline-flex items-center mt-1 px-2 py-0.5 rounded-full text-xs font-medium
                                  ${d.is_active ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                  {d.is_active ? 'Active' : 'Inactive'}
                </span>
              )}
            </div>
          </div>

          <Section title="Contact">
            <Field label="Email"    value={d.email} />
            <Field label="Phone"    value={d.phone || d.mobile} />
            <Field label="Address"  value={d.address} />
            <Field label="County"   value={d.county || d.county_name} />
            <Field label="Eir Code" value={d.eir_code || d.postal_code} />
            <Field label="Website"  value={d.website} />
          </Section>

          <Section title="Client Info">
            <Field label="Client ID"      value={d._id || d.id || d.client_id} />
            <Field label="Client Type"    value={d.client_type || d.type} />
            <Field label="Client Type ID" value={d.client_type_id} />
            <Field label="County ID"      value={d.county_id} />
            <Field label="Region"         value={d.region || d.region_name} />
            <Field label="Country"        value={d.country || d.country_name} />
          </Section>

          <Section title="Admin">
            <Field label="Contact Person" value={d.contact_person || d.contact_name} />
            <Field label="Notes"          value={d.notes} />
            <Field label="Status"         value={d.status} />
            <Field label="Tags"           value={Array.isArray(d.tags) ? d.tags.map(t => t.name || t).join(', ') : d.tags} />
          </Section>

          <Section title="Timestamps">
            <Field label="Created At" value={d.created_at ? new Date(d.created_at).toLocaleString() : null} />
            <Field label="Updated At" value={d.updated_at ? new Date(d.updated_at).toLocaleString() : null} />
          </Section>

          {/* Any remaining fields not yet displayed */}
          {(() => {
            const shown = new Set(['_id','id','client_id','name','title','client_type','type','is_active',
              'email','phone','mobile','address','county','county_name','eir_code','postal_code','website',
              'client_type_id','county_id','region','region_name','country','country_name',
              'contact_person','contact_name','notes','status','tags','created_at','updated_at'])
            const extra = Object.entries(d).filter(([k]) => !shown.has(k))
            if (!extra.length) return null
            return (
              <details className="mt-4">
                <summary className="text-xs font-medium text-gray-400 cursor-pointer select-none">
                  {extra.length} more fields
                </summary>
                <div className="grid grid-cols-2 gap-4 mt-3">
                  {extra.map(([k, v]) => <Field key={k} label={k} value={v} />)}
                </div>
              </details>
            )
          })()}
        </div>
      )}

      {/* Empty state */}
      {!result && !loading && !error && (
        <div className="card p-12 text-center">
          <svg className="w-12 h-12 mx-auto mb-4 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
          </svg>
          <p className="text-sm text-gray-400">Enter a Client ID and click <strong>Fetch</strong></p>
        </div>
      )}
    </div>
  )
}
