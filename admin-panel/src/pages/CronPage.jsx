import { useState, useEffect, useRef } from 'react'
import { shiftsApi } from '../services/api'

// ── helpers ────────────────────────────────────────────────────────────────────
function timeAgo(ts) {
  if (!ts) return ''
  const diff = Math.floor((Date.now() - new Date(ts)) / 1000)
  if (diff < 60)  return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff/60)}m ago`
  if (diff < 86400) return `${Math.floor(diff/3600)}h ago`
  return `${Math.floor(diff/86400)}d ago`
}

function StatusBadge({ text }) {
  const map = {
    'inserted': 'bg-green-100 text-green-700',
    'updated':  'bg-blue-100 text-blue-700',
    'synced':   'bg-blue-100 text-blue-700',
    'error':    'bg-red-100 text-red-600',
    'pending':  'bg-gray-100 text-gray-500',
  }
  const cls = map[text] || 'bg-gray-100 text-gray-500'
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${cls}`}>{text}</span>
  )
}

// ── CronPage ────────────────────────────────────────────────────────────────────
export default function CronPage() {
  const [loading, setLoading]   = useState(false)
  const [lastRun, setLastRun]   = useState(null)
  const [results, setResults]   = useState([])
  const [summary, setSummary]   = useState(null)
  const [error, setError]       = useState(null)
  const [autoRun, setAutoRun]   = useState(false)
  const [interval, setIntervalMs] = useState(300)  // seconds
  const timerRef = useRef(null)

  const runSync = async () => {
    setLoading(true)
    setError(null)
    try {
      const { data } = await shiftsApi.list({
        search:     '',
        page:       1,
        per_page:   10,
        sort_by:    'date',
        sort_order: 'desc',
      })

      const shifts  = data.data || []
      const rows = shifts.map(s => ({
        id:         s._id || s.id || '',
        code:       s.shift_code || s.name || '—',
        date:       s.date || '',
        start_time: s.start_time || '',
        end_time:   s.end_time || '',
        client:     s.client_name || s.location || '—',
        status:     s.upstream_status || s.status || '—',
        syncStatus: 'synced',
      }))

      setResults(rows)
      setSummary({
        fetched:   shifts.length,
        inserted:  data.inserted  ?? '—',
        updated:   data.updated   ?? '—',
        skipped:   data.skipped   ?? '—',
        total:     data.total     ?? '—',
      })
      setLastRun(new Date())
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Sync failed')
    } finally {
      setLoading(false)
    }
  }

  // Auto-run timer
  useEffect(() => {
    if (autoRun) {
      timerRef.current = setInterval(runSync, interval * 1000)
    } else {
      clearInterval(timerRef.current)
    }
    return () => clearInterval(timerRef.current)
  }, [autoRun, interval])

  // Run once on mount
  useEffect(() => { runSync() }, [])

  return (
    <div className="p-8 max-w-5xl">
      {/* Header */}
      <div className="flex items-start justify-between mb-6">
        <div>
          <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
            <span>Cron</span>
            <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
            </svg>
            <span>Sync Latest Shifts</span>
          </div>
          <h1 className="text-2xl font-bold text-gray-900">Sync Latest Shifts</h1>
          <p className="text-sm text-gray-500 mt-1">
            Fetches the 10 most recent shifts from the upstream API and saves to DB.
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Auto-run toggle */}
          <div className="flex items-center gap-2 bg-gray-50 border border-gray-200 rounded-lg px-3 py-2">
            <span className="text-xs text-gray-500">Auto every</span>
            <input
              type="number" min={30} max={3600}
              value={interval}
              onChange={e => setIntervalMs(Number(e.target.value))}
              className="w-14 text-center text-xs border border-gray-200 rounded px-1 py-0.5"
            />
            <span className="text-xs text-gray-500">s</span>
            <button
              onClick={() => setAutoRun(v => !v)}
              className={`px-2 py-0.5 rounded text-xs font-medium transition-colors ${
                autoRun ? 'bg-green-600 text-white' : 'bg-gray-200 text-gray-600'
              }`}
            >
              {autoRun ? 'ON' : 'OFF'}
            </button>
          </div>
          {/* Manual run */}
          <button
            onClick={runSync}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white disabled:opacity-50"
            style={{ backgroundColor: '#1e7a38' }}
          >
            {loading
              ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"/>
              : <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
                </svg>
            }
            {loading ? 'Syncing…' : 'Run Now'}
          </button>
        </div>
      </div>

      {/* Status bar */}
      <div className="flex items-center gap-4 mb-5 p-3 bg-gray-50 rounded-lg border border-gray-100 text-sm">
        <div className={`w-2 h-2 rounded-full ${loading ? 'bg-yellow-400 animate-pulse' : autoRun ? 'bg-green-500' : 'bg-gray-300'}`}/>
        <span className="text-gray-500">{loading ? 'Running…' : autoRun ? `Auto-syncing every ${interval}s` : 'Manual mode'}</span>
        {lastRun && <span className="text-gray-400 text-xs ml-auto">Last run: {timeAgo(lastRun)}</span>}
      </div>

      {/* Error */}
      {error && (
        <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">{error}</div>
      )}

      {/* Summary */}
      {summary && (
        <div className="grid grid-cols-5 gap-3 mb-5">
          {[
            { label: 'Fetched',  value: summary.fetched,  color: 'text-gray-800' },
            { label: 'Inserted', value: summary.inserted, color: 'text-green-600' },
            { label: 'Updated',  value: summary.updated,  color: 'text-blue-600' },
            { label: 'Skipped',  value: summary.skipped,  color: 'text-gray-400' },
            { label: 'Total DB', value: summary.total,    color: 'text-gray-600' },
          ].map(({ label, value, color }) => (
            <div key={label} className="card p-3 text-center">
              <div className={`text-xl font-bold ${color}`}>{value}</div>
              <div className="text-xs text-gray-400 mt-0.5">{label}</div>
            </div>
          ))}
        </div>
      )}

      {/* Table */}
      {results.length > 0 && (
        <div className="card overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                {['Shift Code','Date','Time','Client','Status','Sync'].map(h => (
                  <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {results.map((r, i) => (
                <tr key={r.id || i} className="hover:bg-gray-50/50">
                  <td className="px-4 py-3 font-medium text-gray-900">{r.code}</td>
                  <td className="px-4 py-3 text-gray-600">{r.date ? String(r.date).split('T')[0] : '—'}</td>
                  <td className="px-4 py-3 text-gray-600">
                    {r.start_time && r.end_time ? `${r.start_time} – ${r.end_time}` : r.start_time || '—'}
                  </td>
                  <td className="px-4 py-3 text-gray-600">{r.client}</td>
                  <td className="px-4 py-3"><StatusBadge text={r.status}/></td>
                  <td className="px-4 py-3"><StatusBadge text={r.syncStatus}/></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
