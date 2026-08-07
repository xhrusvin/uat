import { useState, useEffect } from 'react'
import { qqiStatusesApi } from '../services/api'

const api = qqiStatusesApi

function Modal({ item, onClose, onSave, saving }) {
  const isEdit = !!item?.id
  const [form, setForm] = useState({
    name:      item?.name      || '',
    code:      item?.code      || '',
    is_active: item?.is_active ?? true,
  })
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-sm mx-4">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">{isEdit ? 'Edit QQI Status' : 'Add QQI Status'}</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
            </svg>
          </button>
        </div>
        <div className="px-6 py-5 space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Name</label>
            <input className="input" placeholder="e.g. Approved" value={form.name}
              onChange={e => set('name', e.target.value)} autoFocus />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Code <span className="text-gray-400">(optional)</span></label>
            <input className="input" placeholder="e.g. APPROVED" value={form.code}
              onChange={e => set('code', e.target.value)} />
          </div>
          <label className="flex items-center gap-2 cursor-pointer">
            <input type="checkbox" checked={form.is_active} onChange={e => set('is_active', e.target.checked)} className="w-4 h-4 rounded" />
            <span className="text-sm text-gray-700">Active</span>
          </label>
        </div>
        <div className="px-6 py-4 border-t border-gray-100 flex justify-end gap-2">
          <button onClick={onClose} className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-600">Cancel</button>
          <button onClick={() => onSave(form)} disabled={saving || !form.name.trim()}
            className="flex items-center gap-2 px-4 py-2 text-sm rounded-lg text-white disabled:opacity-50"
            style={{ backgroundColor: '#1e7a38' }}>
            {saving ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"/> : null}
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
      <div className="bg-white rounded-xl shadow-xl w-full max-w-sm mx-4">
        <div className="px-6 py-5 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">Delete QQI Status</h2>
        </div>
        <div className="px-6 py-5">
          <p className="text-sm text-gray-700">Delete <strong>{item.name}</strong>?</p>
          <p className="text-xs text-gray-400 mt-1">This cannot be undone.</p>
        </div>
        <div className="px-6 py-4 border-t border-gray-100 flex justify-end gap-2">
          <button onClick={onCancel} disabled={loading} className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-600">Cancel</button>
          <button onClick={onConfirm} disabled={loading} className="px-4 py-2 text-sm rounded-lg bg-red-600 hover:bg-red-700 text-white disabled:opacity-50">
            {loading ? 'Deleting…' : 'Delete'}
          </button>
        </div>
      </div>
    </div>
  )
}

export default function QQIStatusesPage() {
  const [items, setItems]       = useState([])
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState(null)
  const [showAdd, setShowAdd]   = useState(false)
  const [editItem, setEditItem] = useState(null)
  const [deleteItem, setDeleteItem] = useState(null)
  const [saving, setSaving]     = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [toast, setToast]       = useState(null)

  const showToast = (msg, type = 'success') => {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 3000)
  }

  const load = async () => {
    setLoading(true); setError(null)
    try {
      const { data } = await api.list()
      setItems(data.data || [])
    } catch { setError('Failed to load QQI statuses') }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const handleSave = async (form) => {
    setSaving(true)
    try {
      if (editItem) { await api.update(editItem.id, form); showToast('Updated'); setEditItem(null) }
      else          { await api.create(form);              showToast('Created'); setShowAdd(false) }
      load()
    } catch (err) { showToast(err?.response?.data?.detail || 'Failed to save', 'error') }
    finally { setSaving(false) }
  }

  const handleDelete = async () => {
    setDeleting(true)
    try { await api.delete(deleteItem.id); showToast('Deleted'); setDeleteItem(null); load() }
    catch { showToast('Failed to delete', 'error') }
    finally { setDeleting(false) }
  }

  return (
    <div className="p-8 max-w-2xl">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Master</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/></svg>
        <span>QQI Statuses</span>
      </div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">QQI Statuses</h1>
          <p className="text-sm text-gray-500 mt-1">{items.length} status{items.length !== 1 ? 'es' : ''}</p>
        </div>
        <button onClick={() => setShowAdd(true)}
          className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white"
          style={{ backgroundColor: '#1e7a38' }}>
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4"/>
          </svg>
          Add QQI Status
        </button>
      </div>

      {error && <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">{error}</div>}

      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              <th className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Name</th>
              <th className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Code</th>
              <th className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
              <th className="px-5 py-3" />
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {loading ? (
              <tr><td colSpan={4} className="px-5 py-10 text-center text-gray-400">Loading…</td></tr>
            ) : items.length === 0 ? (
              <tr><td colSpan={4} className="px-5 py-10 text-center text-gray-400">No QQI statuses found</td></tr>
            ) : items.map(item => (
              <tr key={item.id} className="hover:bg-gray-50/50">
                <td className="px-5 py-3.5 font-medium text-gray-900">{item.name}</td>
                <td className="px-5 py-3.5">
                  {item.code
                    ? <span className="px-2 py-0.5 rounded text-xs font-mono bg-gray-100 text-gray-600">{item.code}</span>
                    : <span className="text-gray-300">—</span>}
                </td>
                <td className="px-5 py-3.5">
                  {item.is_active
                    ? <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">Active</span>
                    : <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-500">Inactive</span>}
                </td>
                <td className="px-5 py-3.5 text-right">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => setEditItem(item)} className="p-1.5 rounded hover:bg-blue-50 text-gray-400 hover:text-blue-600">
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/>
                      </svg>
                    </button>
                    <button onClick={() => setDeleteItem(item)} className="p-1.5 rounded hover:bg-red-50 text-gray-400 hover:text-red-600">
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/>
                      </svg>
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {(showAdd || editItem) && (
        <Modal item={editItem} onClose={() => { setShowAdd(false); setEditItem(null) }} onSave={handleSave} saving={saving} />
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
