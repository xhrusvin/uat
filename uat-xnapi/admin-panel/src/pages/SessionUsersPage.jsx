import { useState, useEffect, useRef } from 'react'
import { sessionUsersApi } from '../services/api'

// ── Modal ──────────────────────────────────────────────────────────────────────
function Modal({ item, onClose, onSave, saving }) {
  const isEdit = !!item?.id
  const [form, setForm] = useState({
    first_name: item?.first_name || '',
    last_name:  item?.last_name  || '',
    email:      item?.email      || '',
    phone:      item?.phone      || '',
    role:       item?.role       || '',
    is_active:  item?.is_active  !== false,
  })

  const set = (field) => (e) =>
    setForm(f => ({ ...f, [field]: e.target.type === 'checkbox' ? e.target.checked : e.target.value }))

  const valid = form.first_name.trim().length > 0

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-md mx-4">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">
            {isEdit ? 'Edit Session User' : 'Add Session User'}
          </h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12"/>
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="px-6 py-5 space-y-4">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">
                First Name <span className="text-red-400">*</span>
              </label>
              <input className="input text-sm" placeholder="Jane" value={form.first_name}
                onChange={set('first_name')} autoFocus />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Last Name</label>
              <input className="input text-sm" placeholder="Doe" value={form.last_name}
                onChange={set('last_name')} />
            </div>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Email</label>
            <input className="input text-sm" type="email" placeholder="jane@example.com"
              value={form.email} onChange={set('email')} />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Phone</label>
              <input className="input text-sm" placeholder="+353 1 234 5678"
                value={form.phone} onChange={set('phone')} />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Role / Type</label>
              <input className="input text-sm" placeholder="Admin"
                value={form.role} onChange={set('role')} />
            </div>
          </div>
          <div className="flex items-center gap-2">
            <input id="is_active" type="checkbox" className="w-4 h-4 accent-[#1e7a38]"
              checked={form.is_active} onChange={set('is_active')} />
            <label htmlFor="is_active" className="text-sm text-gray-700 select-none">Active</label>
          </div>
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-gray-100 flex justify-end gap-2">
          <button onClick={onClose}
            className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-600">
            Cancel
          </button>
          <button onClick={() => onSave(form)} disabled={saving || !valid}
            className="flex items-center gap-2 px-4 py-2 text-sm rounded-lg text-white disabled:opacity-50"
            style={{ backgroundColor: '#1e7a38' }}>
            {saving
              ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
              : null}
            {saving ? 'Saving…' : isEdit ? 'Update' : 'Create'}
          </button>
        </div>
      </div>
    </div>
  )
}

// ── Delete confirm ────────────────────────────────────────────────────────────
function DeleteModal({ item, onConfirm, onCancel, loading }) {
  if (!item) return null
  const name = item.name || [item.first_name, item.last_name].filter(Boolean).join(' ') || item.email || 'this user'
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-sm mx-4">
        <div className="px-6 py-5 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">Delete Session User</h2>
        </div>
        <div className="px-6 py-5">
          <p className="text-sm text-gray-700">Delete <strong>{name}</strong>?</p>
          <p className="text-xs text-gray-400 mt-1">This cannot be undone.</p>
        </div>
        <div className="px-6 py-4 border-t border-gray-100 flex justify-end gap-2">
          <button onClick={onCancel} disabled={loading}
            className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-600">
            Cancel
          </button>
          <button onClick={onConfirm} disabled={loading}
            className="px-4 py-2 text-sm rounded-lg bg-red-600 hover:bg-red-700 text-white disabled:opacity-50">
            {loading ? 'Deleting…' : 'Delete'}
          </button>
        </div>
      </div>
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function SessionUsersPage() {
  const [items,      setItems]      = useState([])
  const [total,      setTotal]      = useState(0)
  const [loading,    setLoading]    = useState(true)
  const [error,      setError]      = useState(null)
  const [search,     setSearch]     = useState('')
  const [page,       setPage]       = useState(1)
  const [showAdd,    setShowAdd]    = useState(false)
  const [editItem,   setEditItem]   = useState(null)
  const [deleteItem, setDeleteItem] = useState(null)
  const [saving,     setSaving]     = useState(false)
  const [deleting,   setDeleting]   = useState(false)
  const [toast,      setToast]      = useState(null)
  const searchTimer                 = useRef(null)
  const PER_PAGE = 20

  const showToast = (msg, type = 'success') => {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 3000)
  }

  const load = async (s = search, p = page) => {
    setLoading(true); setError(null)
    try {
      const { data } = await sessionUsersApi.list({ search: s, page: p, per_page: PER_PAGE })
      setItems(data.data || [])
      setTotal(data.total || 0)
    } catch {
      setError('Failed to load session users')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])   // eslint-disable-line react-hooks/exhaustive-deps

  // Debounce search
  const handleSearch = (val) => {
    setSearch(val)
    setPage(1)
    clearTimeout(searchTimer.current)
    searchTimer.current = setTimeout(() => load(val, 1), 350)
  }

  const handlePage = (p) => {
    setPage(p)
    load(search, p)
  }

  const handleSave = async (form) => {
    setSaving(true)
    try {
      if (editItem) {
        await sessionUsersApi.update(editItem.id, form)
        showToast('Session user updated')
        setEditItem(null)
      } else {
        await sessionUsersApi.create(form)
        showToast('Session user created')
        setShowAdd(false)
      }
      load()
    } catch (err) {
      showToast(err?.response?.data?.detail || 'Failed to save', 'error')
    } finally {
      setSaving(false)
    }
  }

  const handleDelete = async () => {
    setDeleting(true)
    try {
      await sessionUsersApi.delete(deleteItem.id)
      showToast('Session user deleted')
      setDeleteItem(null)
      load()
    } catch {
      showToast('Failed to delete', 'error')
    } finally {
      setDeleting(false)
    }
  }

  const totalPages = Math.max(1, Math.ceil(total / PER_PAGE))

  return (
    <div className="p-8">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
        <span>Master</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7"/>
        </svg>
        <span>Session Users</span>
      </div>

      {/* Title row */}
      <div className="flex items-center justify-between mb-5">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Session Users</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {total} user{total !== 1 ? 's' : ''} in <code className="bg-gray-100 px-1 rounded text-xs font-mono">session_users</code> collection
          </p>
        </div>
        <button
          onClick={() => setShowAdd(true)}
          className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white"
          style={{ backgroundColor: '#1e7a38' }}
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4"/>
          </svg>
          Add User
        </button>
      </div>

      {/* Search */}
      <div className="mb-4">
        <div className="relative max-w-xs">
          <svg className="absolute left-3 top-2.5 w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
          </svg>
          <input
            className="input pl-9 text-sm"
            placeholder="Search name, email, role…"
            value={search}
            onChange={e => handleSearch(e.target.value)}
          />
        </div>
      </div>

      {error && (
        <div className="mb-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Name</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Email</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Phone</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Role / Type</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Status</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Synced At</th>
                <th className="px-5 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {loading ? (
                <tr>
                  <td colSpan={7} className="px-5 py-10 text-center text-gray-400">
                    <div className="flex items-center justify-center gap-2">
                      <div className="w-4 h-4 border-2 border-gray-300 border-t-transparent rounded-full animate-spin" />
                      Loading…
                    </div>
                  </td>
                </tr>
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-5 py-12 text-center text-gray-400">
                    <svg className="w-10 h-10 mx-auto mb-3 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                        d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
                    </svg>
                    {search ? 'No users match your search' : 'No session users yet — sync from XN API Calls'}
                  </td>
                </tr>
              ) : items.map(item => {
                const name = item.name || [item.first_name, item.last_name].filter(Boolean).join(' ') || '—'
                const syncedAt = item.synced_at
                  ? new Date(item.synced_at).toLocaleString('en-IE', { dateStyle: 'short', timeStyle: 'short' })
                  : '—'
                return (
                  <tr key={item.id} className="hover:bg-gray-50/50">
                    <td className="px-5 py-3.5 font-medium text-gray-900">{name}</td>
                    <td className="px-5 py-3.5 text-gray-600">{item.email || '—'}</td>
                    <td className="px-5 py-3.5 text-gray-500 text-xs">{item.phone || '—'}</td>
                    <td className="px-5 py-3.5 text-gray-500 text-xs">{item.role || '—'}</td>
                    <td className="px-5 py-3.5">
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium
                        ${item.is_active !== false ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'}`}>
                        {item.is_active !== false ? 'Active' : 'Inactive'}
                      </span>
                    </td>
                    <td className="px-5 py-3.5 text-gray-400 text-xs">{syncedAt}</td>
                    <td className="px-5 py-3.5 text-right">
                      <div className="flex items-center justify-end gap-1">
                        <button
                          onClick={() => setEditItem(item)}
                          className="p-1.5 rounded hover:bg-blue-50 text-gray-400 hover:text-blue-600"
                          title="Edit"
                        >
                          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                              d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/>
                          </svg>
                        </button>
                        <button
                          onClick={() => setDeleteItem(item)}
                          className="p-1.5 rounded hover:bg-red-50 text-gray-400 hover:text-red-600"
                          title="Delete"
                        >
                          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                              d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/>
                          </svg>
                        </button>
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="px-5 py-3 border-t border-gray-100 flex items-center justify-between">
            <span className="text-xs text-gray-400">
              Page {page} of {totalPages} &nbsp;·&nbsp; {total} total
            </span>
            <div className="flex items-center gap-1">
              <button
                disabled={page <= 1}
                onClick={() => handlePage(page - 1)}
                className="px-3 py-1.5 text-xs rounded border border-gray-200 text-gray-600
                           hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                ← Prev
              </button>
              {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                const start = Math.max(1, Math.min(page - 2, totalPages - 4))
                const p = start + i
                return (
                  <button
                    key={p}
                    onClick={() => handlePage(p)}
                    className={`w-8 h-8 text-xs rounded border text-gray-600
                      ${p === page
                        ? 'border-[#1e7a38] bg-[#e8f5ec] text-[#1e7a38] font-medium'
                        : 'border-gray-200 hover:bg-gray-50'}`}
                  >
                    {p}
                  </button>
                )
              })}
              <button
                disabled={page >= totalPages}
                onClick={() => handlePage(page + 1)}
                className="px-3 py-1.5 text-xs rounded border border-gray-200 text-gray-600
                           hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                Next →
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Modals */}
      {(showAdd || editItem) && (
        <Modal
          item={editItem}
          onClose={() => { setShowAdd(false); setEditItem(null) }}
          onSave={handleSave}
          saving={saving}
        />
      )}
      <DeleteModal
        item={deleteItem}
        loading={deleting}
        onConfirm={handleDelete}
        onCancel={() => setDeleteItem(null)}
      />

      {/* Toast */}
      {toast && (
        <div className={`fixed bottom-4 right-4 z-50 px-4 py-3 rounded-lg text-sm shadow-lg border ${
          toast.type === 'error'
            ? 'bg-red-50 border-red-200 text-red-700'
            : 'bg-green-50 border-green-200 text-green-700'
        }`}>
          {toast.msg}
        </div>
      )}
    </div>
  )
}
