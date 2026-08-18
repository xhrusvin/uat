import { useState, useEffect } from 'react'
import { sequencesApi } from '../services/api'

const ICON_MAP = {
  'clock':      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>,
  'star':       <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M11.049 2.927c.3-.921 1.603-.921 1.902 0l1.519 4.674a1 1 0 00.95.69h4.915c.969 0 1.371 1.24.588 1.81l-3.976 2.888a1 1 0 00-.363 1.118l1.518 4.674c.3.922-.755 1.688-1.538 1.118l-3.976-2.888a1 1 0 00-1.176 0l-3.976 2.888c-.783.57-1.838-.197-1.538-1.118l1.518-4.674a1 1 0 00-.363-1.118l-3.976-2.888c-.784-.57-.38-1.81.588-1.81h4.914a1 1 0 00.951-.69l1.519-4.674z"/></svg>,
  'heart':      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z"/></svg>,
  'map-pin':    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"/><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"/></svg>,
  'clock-fast': <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>,
  'clipboard':  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"/></svg>,
}

const ICON_OPTIONS = Object.keys(ICON_MAP)
const COLOR_OPTIONS = ['#6366f1','#f59e0b','#22c55e','#ec4899','#10b981','#f97316','#3b82f6','#8b5cf6','#ef4444','#14b8a6']

function SequenceCard({ seq, onEdit, onDelete, onToggle }) {
  const icon = ICON_MAP[seq.icon] || ICON_MAP['clock']
  const color = seq.icon_color || '#6366f1'
  const bg = color + '18'

  return (
    <div className={`relative bg-white rounded-xl border-2 transition-all flex flex-col
                     ${seq.is_active ? 'border-gray-200 hover:border-gray-300' : 'border-dashed border-gray-200 opacity-60'}`}>
      {seq.is_suggested && (
        <span className="absolute top-3 right-3 text-xs font-medium px-2.5 py-1 rounded-full
                         bg-blue-50 text-blue-600 border border-blue-200">
          Suggested
        </span>
      )}

      <div className="p-5 flex-1">
        <div className="w-10 h-10 rounded-full flex items-center justify-center mb-4 flex-shrink-0"
             style={{ backgroundColor: bg, color }}>
          {icon}
        </div>
        <h3 className="text-base font-semibold text-gray-900 mb-1.5">{seq.name}</h3>
        <p className="text-sm text-gray-500 leading-relaxed">{seq.description}</p>
      </div>

      <div className="px-5 py-3 border-t border-gray-100 flex items-center justify-between">
        <div className="flex items-center gap-1.5 text-xs text-gray-400">
          <svg className="w-3.5 h-3.5 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/>
          </svg>
          <span>Best for</span>
          <span className="font-medium text-blue-600">{seq.best_for || 'All shifts'}</span>
        </div>
        <div className="flex items-center gap-1">
          <button onClick={() => onEdit(seq)}
                  className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 hover:text-gray-700 transition-colors"
                  title="Edit">
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/>
            </svg>
          </button>
          <button onClick={() => onToggle(seq)}
                  className={`p-1.5 rounded-lg transition-colors ${seq.is_active ? 'text-green-500 hover:bg-green-50' : 'text-gray-400 hover:bg-gray-100'}`}
                  title={seq.is_active ? 'Disable' : 'Enable'}>
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={seq.is_active ? "M5 13l4 4L19 7" : "M6 18L18 6M6 6l12 12"}/>
            </svg>
          </button>
          {!seq.is_default && (
            <button onClick={() => onDelete(seq)}
                    className="p-1.5 rounded-lg text-gray-400 hover:bg-red-50 hover:text-red-600 transition-colors"
                    title="Delete">
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/>
              </svg>
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

function Modal({ title, onClose, children }) {
  return (
    <>
      <div className="fixed inset-0 bg-black/40 z-40" onClick={onClose} />
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg">
          <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
            <h3 className="text-base font-semibold text-gray-900">{title}</h3>
            <button onClick={onClose} className="p-1 rounded-lg text-gray-400 hover:bg-gray-100">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
              </svg>
            </button>
          </div>
          <div className="px-6 py-5">{children}</div>
        </div>
      </div>
    </>
  )
}

function SequenceForm({ initial, onSave, onCancel, saving }) {
  const [form, setForm] = useState({
    name:        initial?.name        || '',
    description: initial?.description || '',
    best_for:    initial?.best_for    || 'Routine shifts at familiar venues',
    icon:        initial?.icon        || 'clock',
    icon_color:  initial?.icon_color  || '#6366f1',
    is_suggested: initial?.is_suggested ?? false,
    is_active:   initial?.is_active   ?? true,
    sort_order:  initial?.sort_order  || '',
  })
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  const handleSubmit = (e) => {
    e.preventDefault()
    onSave({ ...form, sort_order: form.sort_order ? Number(form.sort_order) : undefined })
  }

  const preview = ICON_MAP[form.icon] || ICON_MAP['clock']

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <div className="col-span-2">
          <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">
            Name <span className="text-red-400">*</span>
          </label>
          <input required className="input" placeholder="e.g. By distance"
            value={form.name} onChange={e => set('name', e.target.value)} />
        </div>
        <div className="col-span-2">
          <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">Description</label>
          <textarea className="input resize-none" rows={2}
            value={form.description} onChange={e => set('description', e.target.value)}
            placeholder="Describe when to use this sequence…" />
        </div>
        <div className="col-span-2">
          <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">Best For</label>
          <input className="input" placeholder="e.g. Routine shifts at familiar venues"
            value={form.best_for} onChange={e => set('best_for', e.target.value)} />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">Icon</label>
          <select className="input" value={form.icon} onChange={e => set('icon', e.target.value)}>
            {ICON_OPTIONS.map(i => <option key={i} value={i}>{i}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">Icon Colour</label>
          <div className="flex items-center gap-2">
            <div className="w-9 h-9 rounded-lg flex items-center justify-center flex-shrink-0 border border-gray-200"
                 style={{ backgroundColor: form.icon_color + '18', color: form.icon_color }}>
              {preview}
            </div>
            <div className="flex flex-wrap gap-1.5">
              {COLOR_OPTIONS.map(c => (
                <button key={c} type="button" onClick={() => set('icon_color', c)}
                        className={`w-5 h-5 rounded-full border-2 transition-all ${form.icon_color === c ? 'border-gray-800 scale-110' : 'border-transparent'}`}
                        style={{ backgroundColor: c }} />
              ))}
            </div>
          </div>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">Sort Order</label>
          <input type="number" className="input" placeholder="auto"
            value={form.sort_order} onChange={e => set('sort_order', e.target.value)} />
        </div>
        <div className="flex items-end gap-4 pb-1">
          <label className="flex items-center gap-2 cursor-pointer">
            <button type="button" onClick={() => set('is_suggested', !form.is_suggested)}
                    className={`relative w-9 h-5 rounded-full transition-colors ${form.is_suggested ? 'bg-blue-500' : 'bg-gray-300'}`}>
              <span className={`absolute top-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${form.is_suggested ? 'translate-x-4' : 'translate-x-0.5'}`} />
            </button>
            <span className="text-sm text-gray-600">Suggested</span>
          </label>
          <label className="flex items-center gap-2 cursor-pointer">
            <button type="button" onClick={() => set('is_active', !form.is_active)}
                    className={`relative w-9 h-5 rounded-full transition-colors ${form.is_active ? 'bg-[#1e7a38]' : 'bg-gray-300'}`}>
              <span className={`absolute top-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${form.is_active ? 'translate-x-4' : 'translate-x-0.5'}`} />
            </button>
            <span className="text-sm text-gray-600">Active</span>
          </label>
        </div>
      </div>

      <div className="flex gap-3 pt-2">
        <button type="submit" disabled={saving}
                className="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg
                           text-sm font-medium text-white disabled:opacity-50"
                style={{ backgroundColor: '#1e7a38' }}>
          {saving && <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />}
          {saving ? 'Saving…' : initial ? 'Save changes' : 'Add sequence'}
        </button>
        <button type="button" onClick={onCancel} className="btn-secondary px-4">Cancel</button>
      </div>
    </form>
  )
}

export default function SequencesPage() {
  const [items, setItems]       = useState([])
  const [loading, setLoading]   = useState(true)
  const [saving, setSaving]     = useState(false)
  const [error, setError]       = useState(null)
  const [toast, setToast]       = useState(null)
  const [showAdd, setShowAdd]   = useState(false)
  const [editItem, setEditItem] = useState(null)
  const [deleteItem, setDeleteItem] = useState(null)

  const showToast = (msg, type = 'success') => {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 3000)
  }

  const load = async () => {
    setLoading(true)
    setError(null)
    try {
      const { data } = await sequencesApi.list()
      setItems(data.data || [])
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to load sequences')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const handleCreate = async (payload) => {
    setSaving(true)
    try {
      await sequencesApi.create(payload)
      showToast('Sequence added')
      setShowAdd(false)
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to add', 'error')
    } finally { setSaving(false) }
  }

  const handleUpdate = async (payload) => {
    setSaving(true)
    try {
      await sequencesApi.update(editItem.id, payload)
      showToast('Sequence updated')
      setEditItem(null)
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to update', 'error')
    } finally { setSaving(false) }
  }

  const handleToggle = async (seq) => {
    try {
      await sequencesApi.update(seq.id, { is_active: !seq.is_active })
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed', 'error')
    }
  }

  const handleDelete = async () => {
    setSaving(true)
    try {
      await sequencesApi.delete(deleteItem.id)
      showToast('Sequence deleted')
      setDeleteItem(null)
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to delete', 'error')
    } finally { setSaving(false) }
  }

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Master</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Sequences</span>
      </div>

      <div className="flex items-start justify-between mb-2">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Choose a Sequence</h1>
          <p className="text-sm text-gray-500 mt-0.5">Select strategy to rank staff and place rounds</p>
        </div>
        <button onClick={() => setShowAdd(true)}
                className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white"
                style={{ backgroundColor: '#1e7a38' }}>
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4"/>
          </svg>
          Add Sequence
        </button>
      </div>

      {/* Toast */}
      {toast && (
        <div className={`mb-4 px-4 py-3 rounded-lg text-sm font-medium flex items-center gap-2
                         ${toast.type === 'error' ? 'bg-red-50 text-red-700 border border-red-200' : 'bg-green-50 text-green-700 border border-green-200'}`}>
          <svg className="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d={toast.type === 'error' ? "M6 18L18 6M6 6l12 12" : "M5 13l4 4L19 7"}/>
          </svg>
          {toast.msg}
        </div>
      )}

      {error && (
        <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700 flex items-center justify-between">
          <span>{error}</span>
          <button onClick={load} className="underline font-medium">Retry</button>
        </div>
      )}

      {/* Grid */}
      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {Array.from({length: 6}).map((_, i) => (
            <div key={i} className="bg-white rounded-xl border-2 border-gray-100 p-5 animate-pulse">
              <div className="w-10 h-10 rounded-full bg-gray-100 mb-4" />
              <div className="h-4 bg-gray-100 rounded w-2/3 mb-2" />
              <div className="h-3 bg-gray-100 rounded w-full mb-1" />
              <div className="h-3 bg-gray-100 rounded w-4/5" />
            </div>
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {items.map(seq => (
            <SequenceCard key={seq.id} seq={seq}
              onEdit={setEditItem}
              onDelete={setDeleteItem}
              onToggle={handleToggle} />
          ))}
          {items.length === 0 && (
            <div className="col-span-3 text-center py-16 text-sm text-gray-400">
              No sequences yet — click Add Sequence to create one.
            </div>
          )}
        </div>
      )}

      {showAdd && (
        <Modal title="Add New Sequence" onClose={() => setShowAdd(false)}>
          <SequenceForm saving={saving} onSave={handleCreate} onCancel={() => setShowAdd(false)} />
        </Modal>
      )}

      {editItem && (
        <Modal title="Edit Sequence" onClose={() => setEditItem(null)}>
          <SequenceForm initial={editItem} saving={saving} onSave={handleUpdate} onCancel={() => setEditItem(null)} />
        </Modal>
      )}

      {deleteItem && (
        <Modal title="Delete Sequence" onClose={() => setDeleteItem(null)}>
          <p className="text-sm text-gray-600 mb-5">
            Delete <strong>"{deleteItem.name}"</strong>? This cannot be undone.
          </p>
          <div className="flex gap-3">
            <button onClick={handleDelete} disabled={saving}
                    className="flex-1 py-2.5 rounded-lg text-sm font-medium text-white bg-red-600 hover:bg-red-700 disabled:opacity-50">
              {saving ? 'Deleting…' : 'Delete'}
            </button>
            <button onClick={() => setDeleteItem(null)} className="btn-secondary px-4">Cancel</button>
          </div>
        </Modal>
      )}
    </div>
  )
}
