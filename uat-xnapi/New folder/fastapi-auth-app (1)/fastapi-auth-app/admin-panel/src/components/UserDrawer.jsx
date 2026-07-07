import { useEffect, useState } from 'react'
import { useUsersStore } from '../store/usersStore'
import { usersService } from '../services/usersService'
import toast from 'react-hot-toast'

function Field({ label, value }) {
  return (
    <div>
      <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-1">{label}</p>
      <p className="text-sm text-gray-900">{value || <span className="text-gray-400 italic">—</span>}</p>
    </div>
  )
}

function StatusBadge({ status }) {
  const cls = status?.toLowerCase() === 'enabled'  ? 'badge-enabled'
    : status?.toLowerCase() === 'disabled' ? 'badge-disabled'
    : 'badge-default'
  return <span className={cls}>{status || 'Unknown'}</span>
}

export default function UserDrawer({ userId, onClose }) {
  const user    = useUsersStore((s) => s.selectedUser)
  const loading = useUsersStore((s) => s.drawerLoading)
  const saving  = useUsersStore((s) => s.saving)
  const clearSelected = useUsersStore((s) => s.clearSelected)
  const fetchUser  = usersService.fetchUser
  const updateUser = usersService.updateUser

  const [editMode, setEditMode] = useState(false)
  const [xnUserId, setXnUserId]     = useState('')
  const [designation, setDesignation] = useState('')

  useEffect(() => {
    if (userId) fetchUser(userId)
    setEditMode(false)
    return () => clearSelected()
  }, [userId])

  // Populate form when user loads
  useEffect(() => {
    if (user) {
      setXnUserId(user.xn_user_id || '')
      setDesignation(user.designation || '')
    }
  }, [user])

  const handleSave = async () => {
    const result = await updateUser(userId, {
      xn_user_id: xnUserId.trim() || null,
      designation: designation.trim() || null,
    })
    if (result.success) {
      toast.success('User updated successfully')
      setEditMode(false)
    } else {
      toast.error(result.error || 'Failed to save')
    }
  }

  const handleCancel = () => {
    setXnUserId(user?.xn_user_id || '')
    setDesignation(user?.designation || '')
    setEditMode(false)
  }

  if (!userId) return null

  return (
    <>
      {/* Backdrop */}
      <div className="fixed inset-0 bg-black/30 z-40" onClick={onClose} />

      {/* Drawer */}
      <div className="fixed right-0 top-0 h-full w-full max-w-md bg-white z-50 shadow-2xl flex flex-col">

        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <h2 className="text-base font-semibold text-gray-900">User Details</h2>
          <div className="flex items-center gap-2">
            {!editMode && !loading && user && (
              <button
                onClick={() => setEditMode(true)}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium
                           text-white transition-colors"
                style={{ backgroundColor: '#1e7a38' }}
              >
                <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                    d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                </svg>
                Edit
              </button>
            )}
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-600 p-1 rounded-lg hover:bg-gray-100"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-6 py-5">
          {loading ? (
            <div className="flex items-center justify-center h-40">
              <div className="w-6 h-6 border-2 border-t-transparent rounded-full animate-spin"
                   style={{ borderColor: '#1e7a38', borderTopColor: 'transparent' }} />
            </div>
          ) : user ? (
            <>
              {/* Avatar + name */}
              <div className="flex items-center gap-4 mb-6">
                <div className="w-14 h-14 rounded-full flex items-center justify-center
                                text-white text-xl font-bold flex-shrink-0"
                     style={{ backgroundColor: '#1e7a38' }}>
                  {user.first_name?.[0]?.toUpperCase() || user.email?.[0]?.toUpperCase() || '?'}
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-gray-900">{user.full_name || '—'}</h3>
                  <p className="text-sm text-gray-500">{user.email}</p>
                  <div className="mt-1"><StatusBadge status={user.status} /></div>
                </div>
              </div>

              {/* Read-only info */}
              <div className="grid grid-cols-2 gap-4 mb-6">
                <Field label="First Name"  value={user.first_name} />
                <Field label="Last Name"   value={user.last_name} />
                <Field label="Phone"       value={user.phone} />
                <Field label="Status"      value={user.status} />
                <div className="col-span-2">
                  <Field label="Email" value={user.email} />
                </div>
                <div className="col-span-2">
                  <Field label="User ID" value={user.id} />
                </div>
                {user.created_at && (
                  <div className="col-span-2">
                    <Field label="Registered"
                      value={new Date(user.created_at).toLocaleString()} />
                  </div>
                )}
              </div>

              {/* ── Editable section ── */}
              <div className="border-t border-gray-100 pt-5">
                <div className="flex items-center justify-between mb-4">
                  <h4 className="text-sm font-semibold text-gray-700">Portal Fields</h4>
                  {editMode && (
                    <span className="text-xs text-amber-600 bg-amber-50 px-2 py-0.5 rounded-full font-medium">
                      Editing
                    </span>
                  )}
                </div>

                {editMode ? (
                  <div className="space-y-4">
                    {/* XN User ID */}
                    <div>
                      <label className="block text-xs font-medium text-gray-500 uppercase tracking-wide mb-1.5">
                        XN User ID
                      </label>
                      <input
                        type="text"
                        className="input font-mono text-sm"
                        placeholder="e.g. 69d793713f719babe405578e"
                        value={xnUserId}
                        onChange={(e) => setXnUserId(e.target.value)}
                      />
                      <p className="text-xs text-gray-400 mt-1">
                        XpressHealth portal user identifier
                      </p>
                    </div>

                    {/* Designation */}
                    <div>
                      <label className="block text-xs font-medium text-gray-500 uppercase tracking-wide mb-1.5">
                        Designation
                      </label>
                      <input
                        type="text"
                        className="input"
                        placeholder="e.g. Registered Nurse, Healthcare Assistant"
                        value={designation}
                        onChange={(e) => setDesignation(e.target.value)}
                      />
                      <p className="text-xs text-gray-400 mt-1">
                        User's job title or professional role
                      </p>
                    </div>

                    {/* Action buttons */}
                    <div className="flex gap-3 pt-1">
                      <button
                        onClick={handleSave}
                        disabled={saving}
                        className="flex-1 flex items-center justify-center gap-2 py-2 rounded-lg
                                   text-sm font-medium text-white transition-colors
                                   disabled:opacity-50 disabled:cursor-not-allowed"
                        style={{ backgroundColor: '#1e7a38' }}
                      >
                        {saving ? (
                          <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                        ) : (
                          <>
                            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                            </svg>
                            Save changes
                          </>
                        )}
                      </button>
                      <button
                        onClick={handleCancel}
                        disabled={saving}
                        className="btn-secondary px-4"
                      >
                        Cancel
                      </button>
                    </div>
                  </div>
                ) : (
                  <div className="grid grid-cols-1 gap-4">
                    <div>
                      <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-1">
                        XN User ID
                      </p>
                      {user.xn_user_id ? (
                        <p className="text-sm font-mono text-gray-900 bg-gray-50 px-2 py-1.5
                                      rounded border border-gray-200 break-all">
                          {user.xn_user_id}
                        </p>
                      ) : (
                        <p className="text-sm text-gray-400 italic">Not set</p>
                      )}
                    </div>
                    <div>
                      <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-1">
                        Designation
                      </p>
                      {user.designation ? (
                        <p className="text-sm text-gray-900">{user.designation}</p>
                      ) : (
                        <p className="text-sm text-gray-400 italic">Not set</p>
                      )}
                    </div>
                  </div>
                )}
              </div>
            </>
          ) : (
            <p className="text-sm text-gray-400">User not found.</p>
          )}
        </div>
      </div>
    </>
  )
}
