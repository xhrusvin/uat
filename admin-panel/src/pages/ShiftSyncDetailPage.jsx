import { useState } from 'react'
import { shiftsApi } from '../services/api'

export default function ShiftSyncDetailPage() {
  const [shiftId, setShiftId] = useState('')
  const [loading, setLoading]   = useState(false)
  const [result, setResult]     = useState(null)
  const [error, setError]       = useState(null)

  const handleSync = async (e) => {
    e.preventDefault()
    if (!shiftId.trim()) return
    setLoading(true)
    setResult(null)
    setError(null)
    try {
      const { data } = await shiftsApi.syncDetail({ shift_id: shiftId.trim() })
      setResult(data)
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to sync shift')
    } finally {
      setLoading(false)
    }
  }

  const Field = ({ label, value }) => (
    <div className="flex items-start gap-2 py-2 border-b border-gray-100 last:border-0">
      <span className="text-xs text-gray-400 uppercase tracking-wide w-28 flex-shrink-0 pt-0.5">{label}</span>
      <span className="text-sm text-gray-800 font-medium">{value ?? '—'}</span>
    </div>
  )

  return (
    <div className="p-8 max-w-xl">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>XN API</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Sync Shift Detail</span>
      </div>

      <h1 className="text-2xl font-bold text-gray-900 mb-1">Sync Shift Detail</h1>
      <p className="text-sm text-gray-500 mb-6">
        Fetch a single shift from the XpressHealth API and upsert to the shifts collection.
      </p>

      <form onSubmit={handleSync} className="flex gap-3 mb-6">
        <input
          className="input flex-1"
          placeholder="XN Shift ID e.g. 69c2679dd3565ae372023eb6"
          value={shiftId}
          onChange={e => setShiftId(e.target.value)}
          required
        />
        <button
          type="submit"
          disabled={loading}
          className="flex items-center gap-2 px-5 py-2 rounded-lg text-sm font-medium text-white disabled:opacity-50"
          style={{ backgroundColor: '#1e7a38' }}
        >
          {loading
            ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"/>
            : <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
              </svg>
          }
          {loading ? 'Syncing…' : 'Sync'}
        </button>
      </form>

      {error && (
        <div className="px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700 mb-4">
          {error}
        </div>
      )}

      {result && (
        <div className="card p-5">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-base font-semibold text-gray-900">Sync Result</h2>
            <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium
              ${result.action === 'inserted' ? 'bg-green-100 text-green-700' : 'bg-blue-100 text-blue-700'}`}>
              {result.action === 'inserted' ? '✓ Inserted' : '↻ Updated'}
            </span>
          </div>
          <div className="divide-y divide-gray-100">
            <Field label="Shift ID"    value={result.data?.shift_id} />
            <Field label="Shift Code"  value={result.data?.shift_code} />
            <Field label="Date"        value={result.data?.date} />
            <Field label="Time"        value={result.data?.start_time && result.data?.end_time ? `${result.data.start_time} – ${result.data.end_time}` : null} />
            <Field label="User Type"   value={result.data?.user_type} />
            <Field label="Status"      value={result.data?.status} />
            <Field label="Client"      value={result.data?.client} />
            <Field label="Staff"       value={result.data?.staff} />
            <Field label="Premium"     value={result.data?.is_premium ? 'Yes' : 'No'} />
          </div>
        </div>
      )}
    </div>
  )
}
