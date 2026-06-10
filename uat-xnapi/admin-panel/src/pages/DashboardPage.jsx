import { useEffect } from 'react'
import { useUsersStore } from '../store/usersStore'
import { useAuthStore } from '../store/authStore'
import { Link } from 'react-router-dom'

function StatCard({ label, value, icon, color, to }) {
  const content = (
    <div className={`card p-5 flex items-center gap-4 ${to ? 'hover:shadow-md transition-shadow' : ''}`}>
      <div className={`w-12 h-12 rounded-xl flex items-center justify-center ${color}`}>
        {icon}
      </div>
      <div>
        <p className="text-2xl font-bold text-gray-900">{value}</p>
        <p className="text-sm text-gray-500">{label}</p>
      </div>
    </div>
  )
  return to ? <Link to={to}>{content}</Link> : content
}

export default function DashboardPage() {
  const { fetchUsers, total, users, loading } = useUsersStore()
  const { user } = useAuthStore()

  useEffect(() => { fetchUsers() }, [])

  const enabled = users.filter(u => u.status?.toLowerCase() === 'enabled').length
  const recent = users.filter(u => {
    if (!u.created_at) return false
    return new Date(u.created_at) > new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)
  }).length

  return (
    <div className="p-8">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">
          Welcome back, {user?.first_name || 'Admin'} 👋
        </h1>
        <p className="text-gray-500 text-sm mt-1">Here's an overview of the platform.</p>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5 mb-8">
        <StatCard
          label="Total Users"
          value={loading ? '…' : total}
          to="/users"
          color="bg-brand-50 text-brand-600"
          icon={
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" />
            </svg>
          }
        />
        <StatCard
          label="Active Users"
          value={loading ? '…' : enabled}
          color="bg-green-50 text-green-600"
          icon={
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          }
        />
        <StatCard
          label="New This Week"
          value={loading ? '…' : recent}
          color="bg-purple-50 text-purple-600"
          icon={
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
            </svg>
          }
        />
      </div>

      {/* Recent users table */}
      <div className="card">
        <div className="px-5 py-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="font-semibold text-gray-900">Recent Users</h2>
          <Link to="/users" className="text-sm text-brand-500 hover:text-brand-600 font-medium">
            View all →
          </Link>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <div className="w-6 h-6 border-2 border-brand-500 border-t-transparent rounded-full animate-spin" />
          </div>
        ) : (
          <div className="divide-y divide-gray-100">
            {users.slice(0, 5).map((u) => (
              <div key={u.id} className="px-5 py-3 flex items-center gap-3">
                <div className="w-8 h-8 rounded-full bg-brand-100 text-brand-700 flex items-center
                                justify-center text-xs font-bold flex-shrink-0">
                  {u.first_name?.[0]?.toUpperCase() || u.email?.[0]?.toUpperCase() || '?'}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-900 truncate">
                    {u.full_name || '—'}
                  </p>
                  <p className="text-xs text-gray-400 truncate">{u.email}</p>
                </div>
                <span className={u.status?.toLowerCase() === 'enabled' ? 'badge-enabled' : 'badge-default'}>
                  {u.status || '—'}
                </span>
              </div>
            ))}
            {users.length === 0 && (
              <p className="text-sm text-gray-400 text-center py-8">No users found.</p>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
