import { useState } from 'react'
import { recruitmentsApi } from '../services/api'

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
  const hasContent = Array.isArray(children)
    ? children.some(Boolean)
    : Boolean(children)
  if (!hasContent) return null
  return (
    <div className="mb-6">
      <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3
                     pb-2 border-b border-gray-100">{title}</h4>
      <div className="grid grid-cols-2 gap-4">{children}</div>
    </div>
  )
}

function SyncBadge({ sync }) {
  if (!sync) return null
  const color = sync.action === 'inserted'
    ? 'bg-green-100 text-green-700'
    : 'bg-blue-100 text-blue-700'
  return (
    <div className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium ${color}`}>
      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
      </svg>
      User {sync.action} in database
      <span className="text-xs opacity-70 font-mono">({sync.user_id?.slice(-8)})</span>
    </div>
  )
}

export default function UserDetailsPage() {
  const [userId, setUserId]   = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult]   = useState(null)
  const [error, setError]     = useState(null)

  const handleCall = async () => {
    if (!userId.trim()) return
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      const { data } = await recruitmentsApi.detail(userId.trim())
      setResult(data)
      if (data.success === false) setError(data.message || 'API returned an error')
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Request failed')
    } finally {
      setLoading(false)
    }
  }

  const d   = result?.data || {}
  const sync = result?.sync

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API Calls</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>User Details</span>
      </div>
      <h1 className="text-2xl font-bold text-gray-900 mb-1">User Details</h1>
      <p className="text-sm text-gray-500 mb-6">
        Fetch recruitment detail by XN User ID and sync to users collection.
      </p>

      {/* Request card */}
      <div className="card p-5 mb-6">
        <div className="flex items-center gap-2 mb-4">
          <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold bg-blue-100 text-blue-700 font-mono">GET</span>
          <code className="text-xs text-gray-600 bg-gray-100 px-3 py-1.5 rounded-lg font-mono">
            {`${import.meta.env.VITE_API_URL || ''}/recruitments/detail`}
          </code>
          <span className="text-xs text-gray-400 bg-yellow-50 border border-yellow-200 px-2 py-1 rounded-lg">
            External API Key
          </span>
        </div>

        <div className="flex gap-3 items-end">
          <div className="flex-1 max-w-sm">
            <label className="block text-xs font-medium text-gray-500 mb-1.5">
              XN User ID <span className="text-gray-400 font-normal font-mono">("_id" field)</span>
            </label>
            <input
              type="text"
              className="input font-mono text-sm"
              placeholder="6955c86c55caf29bbc0b0402"
              value={userId}
              onChange={e => setUserId(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleCall()}
            />
          </div>
          <button
            onClick={handleCall}
            disabled={loading || !userId.trim()}
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
            {loading ? 'Fetching…' : 'Fetch & Sync'}
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="mb-5 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Result */}
      {result?.success && (
        <div className="space-y-5">
          {/* Sync status */}
          {sync && (
            <div className="flex items-center gap-4 flex-wrap">
              <SyncBadge sync={sync} />
              <span className="text-xs text-gray-400">
                {sync.fields_updated?.length} fields synced to users collection
              </span>
            </div>
          )}

          {/* User card */}
          <div className="card p-6">
            {/* Avatar + name */}
            <div className="flex items-center gap-4 mb-6">
              <div className="w-14 h-14 rounded-full flex items-center justify-center
                              text-white text-xl font-bold flex-shrink-0"
                   style={{ backgroundColor: '#1e7a38' }}>
                {d.first_name?.[0]?.toUpperCase() || '?'}
              </div>
              <div>
                <h2 className="text-xl font-bold text-gray-900">
                  {[d.first_name, d.last_name].filter(Boolean).join(' ') || '—'}
                </h2>
                <p className="text-sm text-gray-500">{d.user_type || '—'}</p>
                <div className="flex gap-2 mt-1.5">
                  {d.status && (
                    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">
                      {d.status}
                    </span>
                  )}
                  {d.recruitment_status && (
                    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-700">
                      {d.recruitment_status}
                    </span>
                  )}
                </div>
              </div>
            </div>

            <Section title="Contact">
              <Field label="Email"        value={d.email} />
              <Field label="Phone"        value={d.phone_number} />
              <Field label="Address"      value={d.address} />
              <Field label="Eir Code"     value={d.eir_code} />
              <Field label="County ID"    value={d.county_id} />
              <Field label="Country ID"   value={d.country_id} />
            </Section>

            <Section title="Personal">
              <Field label="Date of Birth"  value={d.dob} />
              <Field label="Gender ID"      value={d.gender_id} />
              <Field label="PPS Number"     value={d.pps_number} />
              <Field label="Uniform Size"   value={d.uniform_size} />
            </Section>

            <Section title="Work Experience">
              <Field label="Experience (Years)"  value={d.experience_year} />
              <Field label="Experience (Months)" value={d.experience_month} />
              <Field label="Company Name"        value={d.company_name} />
              <Field label="Job Title"           value={d.job_title} />
              <Field label="Company Phone"       value={`${d.company_dial_code || ''}${d.company_phone || ''}`} />
              <Field label="Last Company (Years)" value={d.last_company_experience_year} />
              <Field label="Last Company (Months)" value={d.last_company_experience_month} />
              <Field label="Company County ID"   value={d.company_county_id} />
              <Field label="Travel Mode"         value={d.travel_mode} />
              <Field label="Masters"             value={d.masters} />
            </Section>

            <Section title="Compliance & Visa">
              <Field label="Permission to Work"   value={d.permission_to_work} />
              <Field label="Work Permit Exemption" value={d.work_permit_exemption} />
              <Field label="Visa Type ID"         value={d.visa_type_id} />
              <Field label="Face Verification"    value={d.face_verification_status} />
              <Field label="TB Vaccine"           value={d.tuberculosis_vaccine} />
              <Field label="Hepatitis Antibody"   value={d.hepatitis_antibody} />
              <Field label="MMR Vaccine"          value={d.mmr_vaccine} />
              <Field label="COVID-19 Vaccine"     value={d.covid_19_vaccine} />
            </Section>

            {/* References */}
            {d.references?.length > 0 && (
              <div className="mb-6">
                <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3
                               pb-2 border-b border-gray-100">
                  References ({d.references.length})
                </h4>
                <div className="space-y-3">
                  {d.references.map((ref, i) => (
                    <div key={ref.id || i} className="bg-gray-50 rounded-lg p-4">
                      <div className="flex items-start justify-between mb-2">
                        <div>
                          <p className="text-sm font-semibold text-gray-900">{ref.name}</p>
                          <p className="text-xs text-gray-500">{ref.job_role} — {ref.organization}</p>
                        </div>
                        <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium
                                          ${ref.status === 'completed' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'}`}>
                          {ref.status}
                        </span>
                      </div>
                      <div className="grid grid-cols-2 gap-2 text-xs text-gray-500">
                        <span>📧 {ref.email}</span>
                        <span>📞 {ref.dial_code}{ref.phone}</span>
                        <span>Mail: {ref.mail_delivery_status}</span>
                        <span>Sent: {ref.mail_sent ? 'Yes' : 'No'}</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Tags + location */}
            {(d.tags?.length > 0 || d.location) && (
              <Section title="Other">
                {d.tags?.length > 0 && (
                  <div className="col-span-2">
                    <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-1.5">Tags</p>
                    <div className="flex flex-wrap gap-2">
                      {d.tags.map(t => (
                        <span key={t.id} className="inline-flex items-center px-2.5 py-1 rounded-full text-xs
                                                     font-medium bg-orange-100 text-orange-700">
                          {t.name}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
                {d.location && (
                  <Field label="Location" value={`${d.location.latitude}, ${d.location.longitude}`} />
                )}
              </Section>
            )}
          </div>

          {/* Raw sync info */}
          {sync?.fields_updated && (
            <details className="card p-4">
              <summary className="text-xs font-medium text-gray-500 cursor-pointer select-none">
                Fields synced to users collection ({sync.fields_updated.length})
              </summary>
              <div className="flex flex-wrap gap-1.5 mt-3">
                {sync.fields_updated.map(f => (
                  <span key={f} className="inline-flex items-center px-2 py-0.5 rounded bg-gray-100 text-gray-600 text-xs font-mono">
                    {f}
                  </span>
                ))}
              </div>
            </details>
          )}
        </div>
      )}

      {/* Empty state */}
      {!result && !loading && !error && (
        <div className="card p-12 text-center">
          <svg className="w-12 h-12 mx-auto mb-4 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
          </svg>
          <p className="text-sm text-gray-400">Enter an XN User ID and click <strong>Fetch & Sync</strong></p>
        </div>
      )}
    </div>
  )
}
