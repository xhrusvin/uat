import { useState, useEffect } from 'react'
import { criteriaApi } from '../services/api'

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
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
          <div className="px-6 py-5">{children}</div>
        </div>
      </div>
    </>
  )
}

function CriteriaForm({ initial, onSave, onCancel, saving }) {
  const [label, setLabel]       = useState(initial?.label || '')
  const [field, setField]       = useState(initial?.field || '')
  const [description, setDesc]  = useState(initial?.description || '')
  const [isActive, setIsActive] = useState(initial?.is_active ?? true)

  const handleSubmit = (e) => {
    e.preventDefault()
    onSave({ label: label.trim(), field: field.trim(), description: description.trim(), is_active: isActive })
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">
          Label <span className="text-red-400">*</span>
        </label>
        <input required type="text" className="input" placeholder="e.g. User Type"
          value={label} onChange={e => setLabel(e.target.value)} />
        <p className="text-xs text-gray-400 mt-1">Shown in the Filter by… dropdown</p>
      </div>
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">
          DB Field <span className="text-red-400">*</span>
        </label>
        <input required type="text" className="input font-mono text-sm" placeholder="e.g. user_type"
          value={field} onChange={e => setField(e.target.value)} />
        <p className="text-xs text-gray-400 mt-1">MongoDB field name in the shifts collection</p>
      </div>
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1.5 uppercase tracking-wide">Description</label>
        <input type="text" className="input" placeholder="Optional description"
          value={description} onChange={e => setDesc(e.target.value)} />
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
                className="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg
                           text-sm font-medium text-white disabled:opacity-50"
                style={{ backgroundColor: '#1e7a38' }}>
          {saving ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" /> : null}
          {saving ? 'Saving…' : initial ? 'Save changes' : 'Add criteria'}
        </button>
        <button type="button" onClick={onCancel} className="btn-secondary px-4">Cancel</button>
      </div>
    </form>
  )
}

export default function CriteriaPage() {
  const [items, setItems]     = useState([])
  const [loading, setLoading] = useState(true)
  const [saving, setSaving]   = useState(false)
  const [error, setError]     = useState(null)
  const [toast, setToast]     = useState(null)

  const [showAdd, setShowAdd]       = useState(false)
  const [editItem, setEditItem]     = useState(null)
  const [deleteItem, setDeleteItem] = useState(null)

  const showToast = (msg, type = 'success') => {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 3000)
  }

  const load = async () => {
    setLoading(true)
    setError(null)
    try {
      const { data } = await criteriaApi.list()
      setItems(data.data || [])
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to load criteria')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const handleCreate = async (payload) => {
    setSaving(true)
    try {
      await criteriaApi.create(payload)
      showToast('Criteria added successfully')
      setShowAdd(false)
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to add', 'error')
    } finally {
      setSaving(false)
    }
  }

  const handleUpdate = async (payload) => {
    setSaving(true)
    try {
      await criteriaApi.update(editItem._id, payload)
      showToast('Criteria updated')
      setEditItem(null)
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to update', 'error')
    } finally {
      setSaving(false)
    }
  }

  const handleToggleActive = async (item) => {
    try {
      await criteriaApi.update(item._id, { is_active: !item.is_active })
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to update', 'error')
    }
  }

  const handleDelete = async () => {
    setSaving(true)
    try {
      await criteriaApi.delete(deleteItem._id)
      showToast('Criteria deleted')
      setDeleteItem(null)
      load()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed to delete', 'error')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Master</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <span>Criteria</span>
      </div>

      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Filter Criteria</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {loading ? 'Loading…' : `${items.length} criteria — used in the Shifts filter dropdown`}
          </p>
        </div>
        <button onClick={() => setShowAdd(true)}
                className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white"
                style={{ backgroundColor: '#1e7a38' }}>
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          Add Criteria
        </button>
      </div>

      {/* Toast */}
      {toast && (
        <div className={`mb-4 px-4 py-3 rounded-lg text-sm font-medium flex items-center gap-2
                         ${toast.type === 'error' ? 'bg-red-50 text-red-700 border border-red-200' : 'bg-green-50 text-green-700 border border-green-200'}`}>
          <svg className="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d={toast.type === 'error' ? "M6 18L18 6M6 6l12 12" : "M5 13l4 4L19 7"} />
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

      {/* Table */}
      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Label</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">DB Field</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Description</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Type</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
              <th className="px-5 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wide">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading ? (
              Array.from({ length: 6 }).map((_, i) => (
                <tr key={i}>{Array.from({ length: 6 }).map((_, j) => (
                  <td key={j} className="px-5 py-4">
                    <div className="h-4 bg-gray-100 rounded animate-pulse" />
                  </td>
                ))}</tr>
              ))
            ) : items.length === 0 ? (
              <tr><td colSpan={6} className="px-5 py-12 text-center text-sm text-gray-400">
                No criteria yet — click Add Criteria to create one.
              </td></tr>
            ) : (
              items.map(item => (
                <tr key={item._id} className="hover:bg-gray-50 transition-colors">
                  <td className="px-5 py-3.5">
                    <span className="font-medium text-gray-900">{item.label}</span>
                  </td>
                  <td className="px-5 py-3.5">
                    <code className="text-xs bg-gray-100 text-gray-700 px-2 py-0.5 rounded font-mono">
                      {item.field}
                    </code>
                  </td>
                  <td className="px-5 py-3.5 text-gray-500 text-xs">{item.description || '—'}</td>
                  <td className="px-5 py-3.5">
                    {item.is_default
                      ? <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-700">Default</span>
                      : <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-600">Custom</span>}
                  </td>
                  <td className="px-5 py-3.5">
                    <button onClick={() => handleToggleActive(item)}
                            className={`relative w-9 h-5 rounded-full transition-colors ${item.is_active ? 'bg-[#1e7a38]' : 'bg-gray-300'}`}
                            title={item.is_active ? 'Click to disable' : 'Click to enable'}>
                      <span className={`absolute top-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${item.is_active ? 'translate-x-4' : 'translate-x-0.5'}`} />
                    </button>
                  </td>
                  <td className="px-5 py-3.5 text-right">
                    <div className="flex items-center gap-2 justify-end">
                      <button onClick={() => setEditItem(item)}
                              className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 hover:text-gray-700 transition-colors"
                              title="Edit">
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                            d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                        </svg>
                      </button>
                      {!item.is_default && (
                        <button onClick={() => setDeleteItem(item)}
                                className="p-1.5 rounded-lg text-gray-400 hover:bg-red-50 hover:text-red-600 transition-colors"
                                title="Delete">
                          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                              d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                          </svg>
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Add modal */}
      {showAdd && (
        <Modal title="Add New Criteria" onClose={() => setShowAdd(false)}>
          <CriteriaForm saving={saving} onSave={handleCreate} onCancel={() => setShowAdd(false)} />
        </Modal>
      )}

      {/* Edit modal */}
      {editItem && (
        <Modal title="Edit Criteria" onClose={() => setEditItem(null)}>
          <CriteriaForm initial={editItem} saving={saving}
                        onSave={handleUpdate} onCancel={() => setEditItem(null)} />
        </Modal>
      )}

      {/* Delete confirm */}
      {deleteItem && (
        <Modal title="Delete Criteria" onClose={() => setDeleteItem(null)}>
          <p className="text-sm text-gray-600 mb-5">
            Are you sure you want to delete <strong>"{deleteItem.label}"</strong>?
            This will remove it from the filter dropdown.
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
