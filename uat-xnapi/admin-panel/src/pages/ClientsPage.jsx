import { useState, useEffect, useRef } from 'react'
import { clientsApi } from '../services/api'

function Avatar({ name }) {
  const i = name?.[0]?.toUpperCase() || '?'
  return (
    <div className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold flex-shrink-0"
         style={{ backgroundColor: '#e8f5ec', color: '#1e7a38' }}>{i}</div>
  )
}

function Pagination({ page, perPage, total, onPage }) {
  const totalPages = Math.ceil(total / perPage)
  if (totalPages <= 1) return null
  const pages = []
  for (let i = Math.max(1, page - 2); i <= Math.min(totalPages, page + 2); i++) pages.push(i)
  return (
    <div className="flex items-center justify-between px-5 py-3 border-t border-gray-200">
      <p className="text-sm text-gray-500">
        Showing <span className="font-medium">{(page-1)*perPage+1}</span>–
        <span className="font-medium">{Math.min(page*perPage, total)}</span> of <span className="font-medium">{total}</span>
      </p>
      <div className="flex items-center gap-1">
        <button onClick={() => onPage(page-1)} disabled={page===1}
                className="px-2 py-1 rounded text-sm text-gray-600 hover:bg-gray-100 disabled:opacity-40">‹</button>
        {pages.map(p => (
          <button key={p} onClick={() => onPage(p)}
                  className={`px-3 py-1 rounded text-sm font-medium ${p===page?'text-white':'text-gray-600 hover:bg-gray-100'}`}
                  style={p===page?{backgroundColor:'#1e7a38'}:{}}>
            {p}
          </button>
        ))}
        <button onClick={() => onPage(page+1)} disabled={page===Math.ceil(total/perPage)}
                className="px-2 py-1 rounded text-sm text-gray-600 hover:bg-gray-100 disabled:opacity-40">›</button>
      </div>
    </div>
  )
}

export default function ClientsPage() {
  const [clients, setClients]   = useState([])
  const [total, setTotal]       = useState(0)
  const [page, setPage]         = useState(1)
  const [perPage]               = useState(20)
  const [search, setSearch]     = useState('')
  const [status, setStatus]     = useState('')
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState(null)
  const [selected, setSelected] = useState(null)
  const debounceRef             = useRef(null)

  const load = async (pg = page, srch = search, st = status) => {
    setLoading(true)
    setError(null)
    try {
      const { data } = await clientsApi.list({
        skip: (pg - 1) * perPage, limit: perPage,
        ...(srch ? { search: srch } : {}),
        ...(st   ? { status: st }   : {}),
      })
      setClients(data.data || [])
      setTotal(data.total || 0)
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to load clients')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load(1, '', '') }, [])

  const handleSearch = (val) => {
    setSearch(val)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => { setPage(1); load(1, val, status) }, 500)
  }

  const handleStatus = (val) => {
    setStatus(val)
    setPage(1)
    load(1, search, val)
  }

  const handlePage = (pg) => {
    setPage(pg)
    load(pg, search, status)
  }

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Master</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>Clients</span>
      </div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Clients</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {loading ? 'Loading…' : `${total} client${total !== 1 ? 's' : ''} in database`}
          </p>
        </div>
      </div>

      {/* Filters */}
      <div className="card mb-5 p-4">
        <div className="flex flex-wrap gap-3 items-center">
          <div className="relative flex-1 min-w-48">
            <svg className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2"
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
            </svg>
            <input type="text" className="input pl-9" placeholder="Search name, email, phone, county…"
              value={search} onChange={e => handleSearch(e.target.value)} />
          </div>
          <select value={status} onChange={e => handleStatus(e.target.value)} className="input w-36 py-1.5">
            <option value="">All statuses</option>
            <option value="active">Active</option>
            <option value="inactive">Inactive</option>
          </select>
          {(search || status) && (
            <button onClick={() => { setSearch(''); setStatus(''); setPage(1); load(1,'','') }}
                    className="btn-secondary text-sm flex items-center gap-1.5">
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
              </svg>
              Clear
            </button>
          )}
          <div className="flex-1" />
          <button onClick={() => load(page, search, status)} disabled={loading}
                  className="btn-secondary flex items-center gap-2 py-2">
            <svg className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
            </svg>
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg flex items-center justify-between text-sm text-red-700">
          <span>{error}</span>
          <button onClick={() => load()} className="ml-4 font-medium underline">Retry</button>
        </div>
      )}

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50">
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Name</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Email</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Phone</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Client Type</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">County</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
                <th className="px-5 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {loading ? (
                Array.from({length:8}).map((_,i) => (
                  <tr key={i}>{Array.from({length:7}).map((_,j) => (
                    <td key={j} className="px-5 py-3.5"><div className="h-4 bg-gray-100 rounded animate-pulse"/></td>
                  ))}</tr>
                ))
              ) : clients.length === 0 ? (
                <tr><td colSpan={7} className="px-5 py-16 text-center">
                  <p className="text-sm text-gray-400">
                    {total === 0
                      ? 'No clients in database — use XN API Calls → Client List to sync'
                      : 'No results found'}
                  </p>
                </td></tr>
              ) : (
                clients.map(c => (
                  <tr key={c._id} className="hover:bg-gray-50 cursor-pointer transition-colors"
                      onClick={() => setSelected(c)}>
                    <td className="px-5 py-3.5">
                      <div className="flex items-center gap-3">
                        <Avatar name={c.name} />
                        <span className="font-medium text-gray-900">{c.name || '—'}</span>
                      </div>
                    </td>
                    <td className="px-5 py-3.5 text-gray-600">{c.email || '—'}</td>
                    <td className="px-5 py-3.5 text-gray-500">{c.phone || '—'}</td>
                    <td className="px-5 py-3.5 text-gray-500 text-xs">{c.client_type || '—'}</td>
                    <td className="px-5 py-3.5 text-gray-500 text-xs">{c.county || '—'}</td>
                    <td className="px-5 py-3.5">
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium
                                        ${c.is_active !== false ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                        {c.is_active !== false ? 'Active' : 'Inactive'}
                      </span>
                    </td>
                    <td className="px-5 py-3.5 text-right text-gray-400">
                      <svg className="w-4 h-4 inline" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
                      </svg>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <Pagination page={page} perPage={perPage} total={total} onPage={handlePage} />
      </div>

      {/* Detail drawer */}
      {selected && (
        <>
          <div className="fixed inset-0 bg-black/30 z-40" onClick={() => setSelected(null)} />
          <div className="fixed right-0 top-0 h-full w-full max-w-md bg-white z-50 shadow-2xl flex flex-col">
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
              <h2 className="text-base font-semibold text-gray-900">Client Details</h2>
              <button onClick={() => setSelected(null)} className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100">
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
                </svg>
              </button>
            </div>
            <div className="flex-1 overflow-y-auto px-6 py-5">
              <div className="flex items-center gap-4 mb-6">
                <Avatar name={selected.name} />
                <div>
                  <h3 className="text-lg font-semibold text-gray-900">{selected.name}</h3>
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium
                                    ${selected.is_active !== false ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                    {selected.is_active !== false ? 'Active' : 'Inactive'}
                  </span>
                </div>
              </div>
              {/* All fields */}
              <div className="space-y-4">
                {Object.entries(selected)
                  .filter(([k]) => !['_id'].includes(k))
                  .map(([k, v]) => v !== null && v !== undefined && v !== '' ? (
                    <div key={k}>
                      <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-0.5">
                        {k.replace(/_/g, ' ')}
                      </p>
                      <p className="text-sm text-gray-900 break-all">
                        {typeof v === 'boolean' ? (v ? 'Yes' : 'No')
                          : typeof v === 'object' ? JSON.stringify(v)
                          : String(v)}
                      </p>
                    </div>
                  ) : null)}
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
