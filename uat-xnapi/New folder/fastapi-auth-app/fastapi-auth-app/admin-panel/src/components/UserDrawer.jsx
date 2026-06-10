import { useEffect } from 'react'
import { useUsersStore } from '../store/usersStore'

function Field({ label, value }) {
  return (
    <div>
      <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-1">{label}</p>
      <p className="text-sm text-gray-900">{value || <span className="text-gray-400 italic">—</span>}</p>
    </div>
  )
}

function StatusBadge({ status }) {
  const cls = status?.toLowerCase() === 'enabled' ? 'badge-enabled'
    : status?.toLowerCase() === 'disabled' ? 'badge-disabled'
    : 'badge-default'
  return <span className={cls}>{status || 'Unknown'}</span>
}

export default function UserDrawer({ userId, onClose }) {
  const { selectedUser: user, loading, fetchUser, clearSelected } = useUsersStore()

  useEffect(() => {
    if (userId) fetchUser(userId)
    return () => clearSelected()
  }, [userId])

  if (!userId) return null

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/30 z-40 transition-opacity"
        onClick={onClose}
      />

      {/* Drawer */}
      <div className="fixed right-0 top-0 h-full w-full max-w-md bg-white z-50 shadow-2xl
                      flex flex-col overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <h2 className="text-base font-semibold text-gray-900">User Details</h2>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600 transition-colors p-1 rounded-lg hover:bg-gray-100"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-6 py-5">
          {loading ? (
            <div className="flex items-center justify-center h-40">
              <div className="w-6 h-6 border-2 border-brand-500 border-t-transparent rounded-full animate-spin" />
            </div>
          ) : user ? (
            <>
              {/* Avatar + name */}
              <div className="flex items-center gap-4 mb-6">
                <div className="w-14 h-14 rounded-full bg-brand-500 flex items-center justify-center
                                text-white text-xl font-bold flex-shrink-0">
                  {user.first_name?.[0]?.toUpperCase() || user.email?.[0]?.toUpperCase() || '?'}
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-gray-900">{user.full_name || '—'}</h3>
                  <p className="text-sm text-gray-500">{user.email}</p>
                  <div className="mt-1">
                    <StatusBadge status={user.status} />
                  </div>
                </div>
              </div>

              {/* Details grid */}
              <div className="grid grid-cols-2 gap-5">
                <Field label="First Name" value={user.first_name} />
                <Field label="Last Name" value={user.last_name} />
                <Field label="Email" value={user.email} />
                <Field label="Phone" value={user.phone} />
                <Field label="Status" value={user.status} />
                <Field label="Admin" value={user.is_admin ? 'Yes' : 'No'} />
                <div className="col-span-2">
                  <Field label="User ID" value={user.id} />
                </div>
                {user.created_at && (
                  <div className="col-span-2">
                    <Field
                      label="Registered"
                      value={new Date(user.created_at).toLocaleString()}
                    />
                  </div>
                )}
              </div>
            </>
          ) : (
            <p className="text-sm text-gray-500">User not found.</p>
          )}
        </div>
      </div>
    </>
  )
}
