import { useUsersStore } from '../store/usersStore'
import { useAuthStore } from '../store/authStore'
import { Link } from 'react-router-dom'

function StatCard({ label, value, icon, color, to }) {
  const content = (
    <div className={`card p-5 flex items-center gap-4 ${to ? 'hover:shadow-md transition-shadow cursor-pointer' : ''}`}>
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
  // Read from store — no fetch triggered here
  // UsersPage handles loading when navigated to
  const { total, users, listLoading } = useUsersStore()
  const { user } = useAuthStore()

  const enabled = users.filter(u => u.status?.toLowerCase() === 'enabled').length
  const recent  = users.filter(u => {
    if (!u.created_at) return false
    return new Date(u.created_at) > new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)
  }).length

  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">
          Welcome back, {user?.first_name || 'Admin'} 👋
        </h1>
        <p className="text-gray-500 text-sm mt-1">Here's an overview of the platform.</p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5 mb-8">
        <StatCard
          label="Total Users" value={listLoading ? '…' : total} to="/users"
          color="bg-green-50 text-green-700"
          icon={<svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z"/></svg>}
        />
        <StatCard
          label="Active Users" value={listLoading ? '…' : enabled}
          color="bg-blue-50 text-blue-700"
          icon={<svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>}
        />
        <StatCard
          label="New This Week" value={listLoading ? '…' : recent}
          color="bg-purple-50 text-purple-700"
          icon={<svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"/></svg>}
        />
      </div>

      <div className="card">
        <div className="px-5 py-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="font-semibold text-gray-900">Recent Users</h2>
          <Link to="/users" className="text-sm font-medium" style={{ color: '#1e7a38' }}>
            View all →
          </Link>
        </div>

        {listLoading ? (
          <div className="flex items-center justify-center py-12">
            <div className="w-6 h-6 border-2 border-t-transparent rounded-full animate-spin"
                 style={{ borderColor: '#1e7a38', borderTopColor: 'transparent' }} />
          </div>
        ) : (
          <div className="divide-y divide-gray-100">
            {users.slice(0, 5).map((u) => (
              <div key={u.id} className="px-5 py-3 flex items-center gap-3">
                <div className="w-8 h-8 rounded-full flex items-center justify-center
                                text-xs font-bold flex-shrink-0"
                     style={{ backgroundColor: '#e8f5ec', color: '#1e7a38' }}>
                  {u.first_name?.[0]?.toUpperCase() || u.email?.[0]?.toUpperCase() || '?'}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-900 truncate">{u.full_name || '—'}</p>
                  <p className="text-xs text-gray-400 truncate">{u.email}</p>
                </div>
                <span className={u.status?.toLowerCase() === 'enabled' ? 'badge-enabled' : 'badge-default'}>
                  {u.status || '—'}
                </span>
              </div>
            ))}
            {users.length === 0 && !listLoading && (
              <div className="text-center py-8 text-sm text-gray-400">
                No users yet —{' '}
                <Link to="/users" className="underline" style={{ color: '#1e7a38' }}>go to Users</Link>
                {' '}to load them.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
