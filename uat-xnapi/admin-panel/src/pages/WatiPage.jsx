import { useState, useEffect, useRef } from 'react'
import { watiApi } from '../services/api'

const api = watiApi

function Tab({ label, active, onClick, badge }) {
  return (
    <button onClick={onClick}
      className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${active ? 'border-green-600 text-green-700' : 'border-transparent text-gray-500 hover:text-gray-700'}`}>
      {label}
      {badge > 0 && <span className="ml-1.5 px-1.5 py-0.5 rounded-full text-xs bg-green-100 text-green-700">{badge}</span>}
    </button>
  )
}

function StatusBadge({ s }) {
  const map = { sent: 'bg-blue-100 text-blue-700', replied: 'bg-green-100 text-green-700', failed: 'bg-red-100 text-red-600', pending: 'bg-gray-100 text-gray-500', sending: 'bg-yellow-100 text-yellow-700', done: 'bg-green-100 text-green-700', error: 'bg-red-100 text-red-600' }
  return <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${map[s] || 'bg-gray-100 text-gray-500'}`}>{s}</span>
}

function timeAgo(ts) {
  if (!ts) return '—'
  const diff = Math.floor((Date.now() - new Date(ts)) / 1000)
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff/60)}m ago`
  if (diff < 86400) return `${Math.floor(diff/3600)}h ago`
  return new Date(ts).toLocaleDateString('en-GB', { day:'2-digit', month:'short', year:'numeric' })
}

// ── Broadcast Tab ─────────────────────────────────────────────────────────────
function BroadcastTab() {
  const [message, setMessage]           = useState('')
  const [templateName, setTemplateName] = useState('')
  const [designation, setDesignation]   = useState('')
  const [county, setCounty]             = useState('')
  const [sendTo, setSendTo]             = useState('all') // all | selected
  const [users, setUsers]               = useState([])
  const [selectedIds, setSelectedIds]   = useState([])
  const [userSearch, setUserSearch]     = useState('')
  const [loadingUsers, setLoadingUsers] = useState(false)
  const [sending, setSending]           = useState(false)
  const [result, setResult]             = useState(null)
  const [error, setError]               = useState(null)
  const debounce = useRef(null)

  const loadUsers = async (search = '') => {
    setLoadingUsers(true)
    try {
      const { data } = await api.usersList({ skip: 0, limit: 100, search })
      setUsers(data.users || [])
    } catch {}
    finally { setLoadingUsers(false) }
  }

  const toggleUser = (id) => setSelectedIds(p => p.includes(id) ? p.filter(x => x !== id) : [...p, id])

  const handleSend = async () => {
    if (!message.trim()) return setError('Message is required')
    setSending(true); setError(null); setResult(null)
    try {
      const { data } = await api.broadcast({
        message,
        template_name: templateName || null,
        designation:   designation  || null,
        county:        county        || null,
        user_ids:      sendTo === 'selected' ? selectedIds : null,
      })
      setResult(data)
    } catch (err) {
      setError(err?.response?.data?.detail || 'Failed to send')
    } finally { setSending(false) }
  }

  return (
    <div className="space-y-5 max-w-2xl">
      {/* Send to */}
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-2">Send To</label>
        <div className="flex gap-3">
          {['all', 'selected'].map(opt => (
            <label key={opt} className="flex items-center gap-2 cursor-pointer">
              <input type="radio" value={opt} checked={sendTo === opt} onChange={() => setSendTo(opt)} className="w-4 h-4"/>
              <span className="text-sm capitalize">{opt === 'all' ? 'All Enabled Users' : 'Selected Users'}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Filters (for all) */}
      {sendTo === 'all' && (
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Designation (optional)</label>
            <input className="input" placeholder="e.g. Nurse" value={designation} onChange={e => setDesignation(e.target.value)}/>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">County (optional)</label>
            <input className="input" placeholder="e.g. Dublin" value={county} onChange={e => setCounty(e.target.value)}/>
          </div>
        </div>
      )}

      {/* User picker (for selected) */}
      {sendTo === 'selected' && (
        <div className="card p-4">
          <div className="flex items-center gap-2 mb-3">
            <input className="input flex-1" placeholder="Search users…" value={userSearch}
              onChange={e => { setUserSearch(e.target.value); clearTimeout(debounce.current); debounce.current = setTimeout(() => loadUsers(e.target.value), 400) }}
              onFocus={() => !users.length && loadUsers()}/>
            <span className="text-xs text-gray-400">{selectedIds.length} selected</span>
          </div>
          <div className="max-h-48 overflow-y-auto space-y-1">
            {loadingUsers ? <p className="text-sm text-gray-400 text-center py-4">Loading…</p>
              : users.map(u => (
              <label key={u.id} className="flex items-center gap-2 p-2 rounded hover:bg-gray-50 cursor-pointer">
                <input type="checkbox" checked={selectedIds.includes(u.id)} onChange={() => toggleUser(u.id)} className="w-4 h-4"/>
                <span className="text-sm text-gray-800">{u.full_name || `${u.first_name} ${u.last_name}`}</span>
                <span className="text-xs text-gray-400 ml-auto">{u.phone}</span>
              </label>
            ))}
          </div>
        </div>
      )}

      {/* Message */}
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">Message</label>
        <textarea className="input font-mono text-sm" rows={5} placeholder="Type your WhatsApp message…"
          value={message} onChange={e => setMessage(e.target.value)}/>
        <p className="text-xs text-gray-400 mt-1">{message.length} chars</p>
      </div>

      {/* Template (optional) */}
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">Template Name <span className="text-gray-400">(optional — leave blank for session message)</span></label>
        <input className="input" placeholder="e.g. shift_notification" value={templateName} onChange={e => setTemplateName(e.target.value)}/>
      </div>

      {error && <div className="px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">{error}</div>}

      {result && (
        <div className="px-4 py-3 bg-green-50 border border-green-200 rounded-lg text-sm text-green-700">
          ✓ Broadcast sent — {result.sent}/{result.total} delivered, {result.failed} failed
          <span className="ml-2 text-xs text-green-600">ID: {result.broadcast_id}</span>
        </div>
      )}

      <button onClick={handleSend} disabled={sending || !message.trim()}
        className="flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-medium text-white disabled:opacity-50"
        style={{ backgroundColor: '#1e7a38' }}>
        {sending
          ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"/>
          : <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8"/>
            </svg>
        }
        {sending ? 'Sending…' : 'Send WhatsApp Message'}
      </button>
    </div>
  )
}

// ── History Tab ───────────────────────────────────────────────────────────────
function HistoryTab() {
  const [broadcasts, setBroadcasts] = useState([])
  const [loading, setLoading]       = useState(true)
  const [expanded, setExpanded]     = useState(null)
  const [messages, setMessages]     = useState({})
  const [msgLoading, setMsgLoading] = useState({})

  useEffect(() => {
    api.broadcasts({ page: 1, per_page: 20 })
      .then(({ data }) => setBroadcasts(data.data || []))
      .finally(() => setLoading(false))
  }, [])

  const loadMessages = async (bid) => {
    if (messages[bid]) return setExpanded(expanded === bid ? null : bid)
    setMsgLoading(p => ({ ...p, [bid]: true }))
    try {
      const { data } = await api.messages({ broadcast_id: bid, page: 1, per_page: 100 })
      setMessages(p => ({ ...p, [bid]: data.data || [] }))
      setExpanded(bid)
    } finally { setMsgLoading(p => ({ ...p, [bid]: false })) }
  }

  return (
    <div className="space-y-3">
      {loading ? <p className="text-gray-400 text-center py-8">Loading…</p>
        : broadcasts.length === 0 ? <p className="text-gray-400 text-center py-8">No broadcasts yet</p>
        : broadcasts.map(b => (
        <div key={b.id} className="card overflow-hidden">
          <div className="px-4 py-3 flex items-center gap-3 cursor-pointer hover:bg-gray-50" onClick={() => loadMessages(b.id)}>
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium text-gray-900 truncate">{b.message}</p>
              {b.template_name && <p className="text-xs text-gray-400">Template: {b.template_name}</p>}
            </div>
            <div className="flex items-center gap-3 flex-shrink-0">
              <span className="text-xs text-gray-500">{b.sent}/{b.total} sent</span>
              <StatusBadge s={b.status}/>
              <span className="text-xs text-gray-400">{timeAgo(b.created_at)}</span>
              {msgLoading[b.id]
                ? <div className="w-4 h-4 border-2 border-gray-300 border-t-green-600 rounded-full animate-spin"/>
                : <svg className={`w-4 h-4 text-gray-400 transition-transform ${expanded === b.id ? 'rotate-90' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
                  </svg>
              }
            </div>
          </div>
          {expanded === b.id && messages[b.id] && (
            <div className="border-t border-gray-100">
              <table className="w-full text-xs">
                <thead className="bg-gray-50">
                  <tr>
                    {['Name', 'Phone', 'Status', 'Response', 'Sent'].map(h => (
                      <th key={h} className="px-3 py-2 text-left text-gray-500 font-medium">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {messages[b.id].map(m => (
                    <tr key={m.id} className="hover:bg-gray-50/50">
                      <td className="px-3 py-2 text-gray-800">{m.name || '—'}</td>
                      <td className="px-3 py-2 text-gray-500">{m.phone}</td>
                      <td className="px-3 py-2"><StatusBadge s={m.status}/></td>
                      <td className="px-3 py-2 text-gray-600 max-w-xs truncate">{m.response || '—'}</td>
                      <td className="px-3 py-2 text-gray-400">{timeAgo(m.sent_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

// ── Responses Tab ─────────────────────────────────────────────────────────────
function ResponsesTab() {
  const [rows, setRows]       = useState([])
  const [total, setTotal]     = useState(0)
  const [page, setPage]       = useState(1)
  const [loading, setLoading] = useState(true)
  const perPage = 50

  const load = async (p = 1) => {
    setLoading(true)
    try {
      const { data } = await api.responses({ page: p, per_page: perPage })
      setRows(data.data || []); setTotal(data.total || 0); setPage(p)
    } finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <p className="text-sm text-gray-500">{total} incoming message{total !== 1 ? 's' : ''}</p>
        <button onClick={() => load(page)} disabled={loading}
          className="btn-secondary py-1.5 flex items-center gap-1.5 text-xs">
          <svg className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
          </svg>
          Refresh
        </button>
      </div>
      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              {['Phone', 'Message', 'Event', 'Received'].map(h => (
                <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {loading ? (
              <tr><td colSpan={4} className="px-4 py-10 text-center text-gray-400">Loading…</td></tr>
            ) : rows.length === 0 ? (
              <tr><td colSpan={4} className="px-4 py-10 text-center text-gray-400">No responses yet</td></tr>
            ) : rows.map(r => (
              <tr key={r.id} className="hover:bg-gray-50/50">
                <td className="px-4 py-3 font-mono text-xs text-gray-700">{r.phone}</td>
                <td className="px-4 py-3 text-gray-800 max-w-sm">{r.text}</td>
                <td className="px-4 py-3"><span className="px-2 py-0.5 rounded text-xs bg-gray-100 text-gray-600">{r.event}</span></td>
                <td className="px-4 py-3 text-gray-400 text-xs">{timeAgo(r.received_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {Math.ceil(total/perPage) > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
            <span>Page {page} of {Math.ceil(total/perPage)}</span>
            <div className="flex gap-2">
              <button onClick={() => load(page-1)} disabled={page<=1} className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">←</button>
              <button onClick={() => load(page+1)} disabled={page>=Math.ceil(total/perPage)} className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">→</button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ── Main Page ─────────────────────────────────────────────────────────────────
export default function WatiPage() {
  const [tab, setTab] = useState('broadcast')

  return (
    <div className="p-8">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>WhatsApp</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Broadcast</span>
      </div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">WhatsApp Messaging</h1>
        <p className="text-sm text-gray-500 mt-1">Send messages and view responses via WATI</p>
      </div>

      <div className="flex border-b border-gray-200 mb-6 gap-1">
        <Tab label="Send Message"    active={tab==='broadcast'} onClick={() => setTab('broadcast')}/>
        <Tab label="Broadcast History" active={tab==='history'}   onClick={() => setTab('history')}/>
        <Tab label="Incoming Responses" active={tab==='responses'} onClick={() => setTab('responses')}/>
      </div>

      {tab === 'broadcast'  && <BroadcastTab/>}
      {tab === 'history'    && <HistoryTab/>}
      {tab === 'responses'  && <ResponsesTab/>}
    </div>
  )
}
