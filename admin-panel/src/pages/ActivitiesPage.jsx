import { useState, useEffect } from 'react'
import { activitiesApi } from '../services/api'

const ICON_SVG = {
  play:         <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"/><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>,
  pause:        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>,
  'check-circle':<svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>,
  phone:        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z"/></svg>,
  message:      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"/></svg>,
  refresh:      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>,
  check:        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7"/></svg>,
  stop:         <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 10h6v4H9z"/></svg>,
}

function Modal({ title, onClose, children }) {
  return (
    <>
      <div className="fixed inset-0 bg-black/40 z-40" onClick={onClose} />
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <div className="bg-white rounded-xl shadow-2xl w-full max-w-md">
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

function TypeForm({ initial, onSave, onCancel, saving }) {
  const [key, setKey]           = useState(initial?.key || '')
  const [label, setLabel]       = useState(initial?.label || '')
  const [description, setDesc]  = useState(initial?.description || '')
  const [icon, setIcon]         = useState(initial?.icon || 'check')
  const [color, setColor]       = useState(initial?.color || '#6366f1')
  const [isActive, setIsActive] = useState(initial?.is_active ?? true)

  const handleSubmit = (e) => {
    e.preventDefault()
    onSave({ key: key.trim(), label: label.trim(), description: description.trim(), icon, color, is_active: isActive })
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      {!initial && (
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">
            Key <span className="text-red-400">*</span>
          </label>
          <input required className="input font-mono text-sm" placeholder="e.g. sequence_started"
            value={key} onChange={e => setKey(e.target.value.toLowerCase().replace(/\s+/g,'_'))} />
        </div>
      )}
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">
          Label <span className="text-red-400">*</span>
        </label>
        <input required className="input" placeholder="e.g. Sequence Started"
          value={label} onChange={e => setLabel(e.target.value)} />
      </div>
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">Description</label>
        <input className="input" placeholder="Optional description"
          value={description} onChange={e => setDesc(e.target.value)} />
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">Icon</label>
          <select className="input text-sm" value={icon} onChange={e => setIcon(e.target.value)}>
            {Object.keys(ICON_SVG).map(i => <option key={i} value={i}>{i}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">Colour</label>
          <div className="flex items-center gap-2">
            <div className="w-9 h-9 rounded-lg flex items-center justify-center border border-gray-200"
                 style={{ backgroundColor: color + '22', color }}>
              {ICON_SVG[icon] || ICON_SVG.check}
            </div>
            <input type="color" value={color} onChange={e => setColor(e.target.value)}
                   className="w-9 h-9 rounded cursor-pointer border border-gray-200" />
          </div>
        </div>
      </div>
      <div className="flex items-center gap-3">
        <button type="button" onClick={() => setIsActive(!isActive)}
                className={`relative w-10 h-5 rounded-full transition-colors ${isActive ? 'bg-[#1e7a38]' : 'bg-gray-300'}`}>
          <span className={`absolute top-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${isActive ? 'translate-x-5' : 'translate-x-0.5'}`} />
        </button>
        <span className="text-sm text-gray-600">Active</span>
      </div>
      <div className="flex gap-3 pt-2">
        <button type="submit" disabled={saving}
                className="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg text-sm font-medium text-white disabled:opacity-50"
                style={{ backgroundColor: '#1e7a38' }}>
          {saving && <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />}
          {saving ? 'Saving…' : initial ? 'Save changes' : 'Add activity type'}
        </button>
        <button type="button" onClick={onCancel} className="btn-secondary px-4">Cancel</button>
      </div>
    </form>
  )
}

export default function ActivitiesPage() {
  const [types, setTypes]       = useState([])
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
      const { data } = await activitiesApi.listTypes()
      setTypes(data.data || [])
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to load activity types')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const handleCreate = async (payload) => {
    setSaving(true)
    try {
      await activitiesApi.createType(payload)
      showToast('Activity type added')
      setShowAdd(false)
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to add', 'error')
    } finally { setSaving(false) }
  }

  const handleUpdate = async (payload) => {
    setSaving(true)
    try {
      await activitiesApi.updateType(editItem.id, payload)
      showToast('Activity type updated')
      setEditItem(null)
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to update', 'error')
    } finally { setSaving(false) }
  }

  const handleToggle = async (item) => {
    try {
      await activitiesApi.updateType(item.id, { is_active: !item.is_active })
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed', 'error')
    }
  }

  const handleDelete = async () => {
    setSaving(true)
    try {
      await activitiesApi.deleteType(deleteItem.id)
      showToast('Activity type deleted')
      setDeleteItem(null)
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to delete', 'error')
    } finally { setSaving(false) }
  }

  return (
    <div className="p-8">
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Master</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Activity Types</span>
      </div>

      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Activity Types</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {loading ? 'Loading…' : `${types.length} activity types — events logged during outreach`}
          </p>
        </div>
        <button onClick={() => setShowAdd(true)}
                className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white"
                style={{ backgroundColor: '#1e7a38' }}>
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4"/>
          </svg>
          Add Type
        </button>
      </div>

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

      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Type</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Key</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Description</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Kind</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
              <th className="px-5 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wide">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading ? (
              Array.from({length: 6}).map((_, i) => (
                <tr key={i}>{Array.from({length: 6}).map((_, j) => (
                  <td key={j} className="px-5 py-4"><div className="h-4 bg-gray-100 rounded animate-pulse"/></td>
                ))}</tr>
              ))
            ) : types.map(item => (
              <tr key={item.id} className="hover:bg-gray-50 transition-colors">
                <td className="px-5 py-3.5">
                  <div className="flex items-center gap-3">
                    <div className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0"
                         style={{ backgroundColor: (item.color || '#6366f1') + '18', color: item.color || '#6366f1' }}>
                      {ICON_SVG[item.icon] || ICON_SVG.check}
                    </div>
                    <span className="font-medium text-gray-900">{item.label}</span>
                  </div>
                </td>
                <td className="px-5 py-3.5">
                  <code className="text-xs bg-gray-100 text-gray-700 px-2 py-0.5 rounded font-mono">{item.key}</code>
                </td>
                <td className="px-5 py-3.5 text-xs text-gray-500">{item.description || '—'}</td>
                <td className="px-5 py-3.5">
                  {item.is_default
                    ? <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-700">Default</span>
                    : <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-600">Custom</span>}
                </td>
                <td className="px-5 py-3.5">
                  <button onClick={() => handleToggle(item)}
                          className={`relative w-9 h-5 rounded-full transition-colors ${item.is_active ? 'bg-[#1e7a38]' : 'bg-gray-300'}`}>
                    <span className={`absolute top-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${item.is_active ? 'translate-x-4' : 'translate-x-0.5'}`}/>
                  </button>
                </td>
                <td className="px-5 py-3.5 text-right">
                  <div className="flex items-center gap-2 justify-end">
                    <button onClick={() => setEditItem(item)}
                            className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 hover:text-gray-700 transition-colors">
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/>
                      </svg>
                    </button>
                    {!item.is_default && (
                      <button onClick={() => setDeleteItem(item)}
                              className="p-1.5 rounded-lg text-gray-400 hover:bg-red-50 hover:text-red-600 transition-colors">
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/>
                        </svg>
                      </button>
                    )}
                  </div>
                </td>
              </tr>
            ))}
            {!loading && types.length === 0 && (
              <tr><td colSpan={6} className="px-5 py-12 text-center text-sm text-gray-400">No activity types yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {showAdd && (
        <Modal title="Add Activity Type" onClose={() => setShowAdd(false)}>
          <TypeForm saving={saving} onSave={handleCreate} onCancel={() => setShowAdd(false)}/>
        </Modal>
      )}
      {editItem && (
        <Modal title="Edit Activity Type" onClose={() => setEditItem(null)}>
          <TypeForm initial={editItem} saving={saving} onSave={handleUpdate} onCancel={() => setEditItem(null)}/>
        </Modal>
      )}
      {deleteItem && (
        <Modal title="Delete Activity Type" onClose={() => setDeleteItem(null)}>
          <p className="text-sm text-gray-600 mb-5">Delete <strong>"{deleteItem.label}"</strong>? This cannot be undone.</p>
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
