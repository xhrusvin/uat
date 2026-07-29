import { useState, useEffect, useRef } from 'react'
import { promptsApi } from '../services/api'

const api = promptsApi

function Badge({ active }) {
  return active
    ? <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">Active</span>
    : <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-500">Inactive</span>
}

function Modal({ item, onClose, onSave, saving }) {
  const [form, setForm] = useState({
    document_type_code: item?.document_type_code || '',
    prompt_text:        item?.prompt_text        || '',
    version:            item?.version            ?? 1,
    is_active:          item?.is_active          ?? true,
    level:              item?.level              ?? 1,
  })
  const isEdit = !!item?.id
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-2xl flex flex-col max-h-[90vh]">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 flex-shrink-0">
          <h2 className="text-base font-semibold text-gray-900">{isEdit ? 'Edit Prompt' : 'Add Prompt'}</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
            </svg>
          </button>
        </div>
        <div className="px-6 py-5 overflow-y-auto flex-1 space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Document Type Code</label>
            <input className="input" placeholder="e.g. GARDA_VETTING"
              value={form.document_type_code}
              onChange={e => set('document_type_code', e.target.value)} />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Prompt Text</label>
            <textarea className="input font-mono text-xs" rows={10} placeholder="Enter prompt text..."
              value={form.prompt_text}
              onChange={e => set('prompt_text', e.target.value)} />
          </div>
          <div className="flex gap-4">
            <div className="flex-1">
              <label className="block text-xs font-medium text-gray-500 mb-1">Version</label>
              <input type="number" min={1} className="input"
                value={form.version}
                onChange={e => set('version', parseInt(e.target.value) || 1)} />
            </div>
            <div className="flex-1">
              <label className="block text-xs font-medium text-gray-500 mb-1">Level</label>
              <select className="input" value={form.level} onChange={e => set('level', parseInt(e.target.value))}>
                {[1,2,3,4,5].map(l => (
                  <option key={l} value={l}>Level {l}</option>
                ))}
              </select>
            </div>
            <div className="flex items-end pb-1">
              <label className="flex items-center gap-2 cursor-pointer">
                <input type="checkbox" checked={form.is_active}
                  onChange={e => set('is_active', e.target.checked)}
                  className="w-4 h-4 rounded" />
                <span className="text-sm text-gray-700">Active</span>
              </label>
            </div>
          </div>
        </div>
        <div className="px-6 py-4 border-t border-gray-100 flex justify-end gap-2 flex-shrink-0">
          <button onClick={onClose} className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-600">
            Cancel
          </button>
          <button onClick={() => onSave(form)} disabled={saving || !form.document_type_code || !form.prompt_text}
            className="flex items-center gap-2 px-4 py-2 text-sm rounded-lg text-white disabled:opacity-50"
            style={{ backgroundColor: '#1e7a38' }}>
            {saving
              ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"/>
              : null}
            {saving ? 'Saving…' : isEdit ? 'Update' : 'Create'}
          </button>
        </div>
      </div>
    </div>
  )
}

function DeleteModal({ item, onConfirm, onCancel, loading }) {
  if (!item) return null
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-md mx-4">
        <div className="px-6 py-5 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">Delete Prompt</h2>
        </div>
        <div className="px-6 py-5">
          <p className="text-sm text-gray-700">Are you sure you want to delete <strong>{item.document_type_code}</strong>?</p>
          <p className="text-xs text-gray-400 mt-1">This action cannot be undone.</p>
        </div>
        <div className="px-6 py-4 border-t border-gray-100 flex justify-end gap-2">
          <button onClick={onCancel} disabled={loading}
            className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-600">Cancel</button>
          <button onClick={onConfirm} disabled={loading}
            className="px-4 py-2 text-sm rounded-lg bg-red-600 hover:bg-red-700 text-white disabled:opacity-50">
            {loading ? 'Deleting…' : 'Delete'}
          </button>
        </div>
      </div>
    </div>
  )
}

export default function PromptsPage() {
  const [items, setItems]       = useState([])
  const [total, setTotal]       = useState(0)
  const [page, setPage]         = useState(1)
  const [search, setSearch]     = useState('')
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState(null)
  const [showAdd, setShowAdd]   = useState(false)
  const [editItem, setEditItem] = useState(null)
  const [deleteItem, setDeleteItem] = useState(null)
  const [saving, setSaving]     = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [toast, setToast]       = useState(null)
  const debounce                = useRef(null)
  const perPage = 20

  const showToast = (msg, type = 'success') => {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 3000)
  }

  const load = async (p = page, s = search) => {
    setLoading(true); setError(null)
    try {
      const { data } = await api.list({ search: s, page: p, per_page: perPage })
      setItems(data.data || []); setTotal(data.total || 0); setPage(p)
    } catch { setError('Failed to load prompts') }
    finally { setLoading(false) }
  }

  useEffect(() => { load(1, '') }, [])

  const handleSearch = v => {
    setSearch(v); clearTimeout(debounce.current)
    debounce.current = setTimeout(() => load(1, v), 400)
  }

  const handleSave = async (form) => {
    setSaving(true)
    try {
      if (editItem) {
        await api.update(editItem.id, form)
        showToast('Prompt updated')
        setEditItem(null)
      } else {
        await api.create(form)
        showToast('Prompt created')
        setShowAdd(false)
      }
      load(page, search)
    } catch (err) {
      showToast(err?.response?.data?.detail || 'Failed to save', 'error')
    } finally { setSaving(false) }
  }

  const handleDelete = async () => {
    setDeleting(true)
    try {
      await api.delete(deleteItem.id)
      showToast('Prompt deleted')
      setDeleteItem(null)
      load(page, search)
    } catch { showToast('Failed to delete', 'error') }
    finally { setDeleting(false) }
  }

  const totalPages = Math.ceil(total / perPage)
  const [expanded, setExpanded] = useState(null)

  return (
    <div className="p-8 max-w-5xl">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Master</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Prompts</span>
      </div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Prompts</h1>
          <p className="text-sm text-gray-500 mt-1">{total} prompt{total !== 1 ? 's' : ''} in collection</p>
        </div>
        <button onClick={() => setShowAdd(true)}
          className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white"
          style={{ backgroundColor: '#1e7a38' }}>
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4"/>
          </svg>
          Add Prompt
        </button>
      </div>

      <div className="card mb-5 p-4 flex gap-3 items-center">
        <div className="relative flex-1">
          <svg className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
          </svg>
          <input type="text" className="input pl-9" placeholder="Search by code or prompt text…"
            value={search} onChange={e => handleSearch(e.target.value)} />
        </div>
        <button onClick={() => load(page, search)} disabled={loading}
          className="btn-secondary flex items-center gap-2 py-2">
          <svg className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
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
              {['Document Type Code', 'Version', 'Level', 'Status', 'Created', ''].map(h => (
                <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {loading ? (
              <tr><td colSpan={5} className="px-4 py-12 text-center text-gray-400">Loading…</td></tr>
            ) : items.length === 0 ? (
              <tr><td colSpan={5} className="px-4 py-12 text-center text-gray-400">No prompts found</td></tr>
            ) : items.map(item => (
              <>
                <tr key={item.id} className="hover:bg-gray-50/50 cursor-pointer" onClick={() => setExpanded(expanded === item.id ? null : item.id)}>
                  <td className="px-4 py-3 font-medium text-gray-900 font-mono text-xs">{item.document_type_code}</td>
                  <td className="px-4 py-3 text-gray-500">v{item.version}</td>
                  <td className="px-4 py-3">
                    <span className="px-2 py-0.5 rounded text-xs font-medium bg-blue-50 text-blue-700">L{item.level ?? 1}</span>
                  </td>
                  <td className="px-4 py-3"><Badge active={item.is_active}/></td>
                  <td className="px-4 py-3 text-gray-400 text-xs">
                    {item.created_at ? new Date(item.created_at).toLocaleDateString('en-GB', { day:'2-digit', month:'short', year:'numeric' }) : '—'}
                  </td>
                  <td className="px-4 py-3 text-right">
                    <div className="flex items-center justify-end gap-1">
                      <button onClick={e => { e.stopPropagation(); setEditItem(item) }}
                        className="p-1.5 rounded hover:bg-blue-50 text-gray-400 hover:text-blue-600">
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/>
                        </svg>
                      </button>
                      <button onClick={e => { e.stopPropagation(); setDeleteItem(item) }}
                        className="p-1.5 rounded hover:bg-red-50 text-gray-400 hover:text-red-600">
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/>
                        </svg>
                      </button>
                      <svg className={`w-4 h-4 text-gray-400 transition-transform ${expanded === item.id ? 'rotate-90' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
                      </svg>
                    </div>
                  </td>
                </tr>
                {expanded === item.id && (
                  <tr key={`${item.id}-expanded`} className="bg-gray-50/50">
                    <td colSpan={5} className="px-4 py-3">
                      <pre className="text-xs text-gray-600 whitespace-pre-wrap font-mono bg-white border border-gray-100 rounded-lg p-3 max-h-60 overflow-y-auto">
                        {item.prompt_text}
                      </pre>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>

        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
            <span>Page {page} of {totalPages}</span>
            <div className="flex gap-2">
              <button onClick={() => load(page - 1, search)} disabled={page <= 1}
                className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">←</button>
              <button onClick={() => load(page + 1, search)} disabled={page >= totalPages}
                className="px-3 py-1 rounded border border-gray-200 disabled:opacity-40 hover:bg-gray-50">→</button>
            </div>
          </div>
        )}
      </div>

      {(showAdd || editItem) && (
        <Modal
          item={editItem}
          onClose={() => { setShowAdd(false); setEditItem(null) }}
          onSave={handleSave}
          saving={saving}
        />
      )}

      <DeleteModal item={deleteItem} loading={deleting} onConfirm={handleDelete} onCancel={() => setDeleteItem(null)} />

      {toast && (
        <div className={`fixed bottom-4 right-4 z-50 px-4 py-3 rounded-lg text-sm shadow-lg border ${
          toast.type === 'error' ? 'bg-red-50 border-red-200 text-red-700' : 'bg-green-50 border-green-200 text-green-700'
        }`}>{toast.msg}</div>
      )}
    </div>
  )
}
