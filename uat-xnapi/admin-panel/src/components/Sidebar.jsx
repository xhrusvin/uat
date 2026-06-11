import { useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { useAuthStore } from '../store/authStore'
import logoSquare from '../assets/logo-square.png'
import logo from '../assets/logo.png'

const navItems = [
  {
    to: '/dashboard',
    label: 'Dashboard',
    icon: <svg className="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" /></svg>,
  },
  {
    to: '/users',
    label: 'Users',
    icon: <svg className="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" /></svg>,
  },
  {
    to: '/shifts',
    label: 'Shifts',
    icon: <svg className="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>,
  },
  {
    label: 'XN API Calls',
    icon: <svg className="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" /></svg>,
    children: [
      {
        to: '/xn-api/shifts',
        label: 'Shift List',
        icon: <svg className="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" /></svg>,
      },
    ],
  },
]

export default function Sidebar() {
  const { user, logout } = useAuthStore()
  const location = useLocation()
  const [collapsed, setCollapsed] = useState(true)

  // Auto-expand XN API group if on a child route
  const xnApiOpen = location.pathname.startsWith('/xn-api')
  const [xnExpanded, setXnExpanded] = useState(xnApiOpen)

  return (
    <aside className={`relative flex flex-col bg-white border-r border-gray-200 shadow-sm
                       transition-all duration-300 ease-in-out min-h-screen
                       ${collapsed ? 'w-16' : 'w-60'}`}>

      {/* Toggle button */}
      <button onClick={() => setCollapsed(!collapsed)}
              className="absolute -right-3 top-6 z-10 w-6 h-6 rounded-full bg-white border
                         border-gray-200 shadow-md flex items-center justify-center hover:bg-gray-50">
        <svg className={`w-3 h-3 text-gray-500 transition-transform duration-300 ${collapsed ? '' : 'rotate-180'}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
      </button>

      {/* Logo */}
      <div className={`flex items-center border-b border-gray-100 overflow-hidden transition-all duration-300
                       ${collapsed ? 'px-3 py-4 justify-center' : 'px-4 py-4 gap-3'}`}>
        <img src={logoSquare} alt="Xpress Health" className="w-8 h-8 rounded-lg flex-shrink-0" />
        {!collapsed && <img src={logo} alt="Xpress Health" className="h-6 object-contain" />}
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2 py-3 space-y-1">
        {navItems.map((item) => {
          // Group item with children
          if (item.children) {
            const isGroupActive = item.children.some(c => location.pathname.startsWith(c.to))
            return (
              <div key={item.label}>
                <button
                  onClick={() => !collapsed && setXnExpanded(!xnExpanded)}
                  title={collapsed ? item.label : undefined}
                  className={`w-full flex items-center rounded-lg text-sm font-medium transition-colors
                    ${collapsed ? 'justify-center px-2 py-2.5' : 'gap-3 px-3 py-2.5 justify-between'}
                    ${isGroupActive ? 'text-[#1e7a38] bg-[#e8f5ec]' : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'}`}>
                  <div className={`flex items-center ${collapsed ? '' : 'gap-3'}`}>
                    {item.icon}
                    {!collapsed && <span>{item.label}</span>}
                  </div>
                  {!collapsed && (
                    <svg className={`w-3.5 h-3.5 transition-transform ${xnExpanded ? 'rotate-90' : ''}`}
                      fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                    </svg>
                  )}
                </button>

                {/* Children */}
                {(xnExpanded || isGroupActive) && !collapsed && (
                  <div className="ml-4 mt-1 space-y-1 pl-3 border-l-2 border-gray-100">
                    {item.children.map((child) => (
                      <NavLink key={child.to} to={child.to}
                        className={({ isActive }) =>
                          `flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition-colors
                           ${isActive ? 'text-[#1e7a38] bg-[#e8f5ec]' : 'text-gray-500 hover:bg-gray-100 hover:text-gray-800'}`
                        }>
                        {child.icon}
                        {child.label}
                      </NavLink>
                    ))}
                  </div>
                )}

                {/* Collapsed: show children as tooltips */}
                {collapsed && (
                  <div className="space-y-1 mt-1">
                    {item.children.map((child) => (
                      <NavLink key={child.to} to={child.to} title={child.label}
                        className={({ isActive }) =>
                          `flex items-center justify-center px-2 py-2.5 rounded-lg transition-colors
                           ${isActive ? 'text-[#1e7a38] bg-[#e8f5ec]' : 'text-gray-400 hover:bg-gray-100'}`
                        }>
                        {child.icon}
                      </NavLink>
                    ))}
                  </div>
                )}
              </div>
            )
          }

          // Regular nav item
          return (
            <NavLink key={item.to} to={item.to} title={collapsed ? item.label : undefined}
              className={({ isActive }) =>
                `flex items-center rounded-lg text-sm font-medium transition-colors group
                 ${collapsed ? 'justify-center px-2 py-2.5' : 'gap-3 px-3 py-2.5'}
                 ${isActive ? 'bg-[#e8f5ec] text-[#1e7a38]' : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'}`
              }>
              {item.icon}
              {!collapsed && <span>{item.label}</span>}
            </NavLink>
          )
        })}
      </nav>

      {/* User footer */}
      <div className="border-t border-gray-100 py-3 px-2 space-y-1">
        <div className={`flex items-center overflow-hidden ${collapsed ? 'justify-center px-1 py-2' : 'gap-3 px-3 py-2'}`}>
          <div className="w-7 h-7 rounded-full flex items-center justify-center text-white text-xs font-bold flex-shrink-0"
               style={{ backgroundColor: '#1e7a38' }}>
            {user?.first_name?.[0]?.toUpperCase() || user?.email?.[0]?.toUpperCase() || 'A'}
          </div>
          {!collapsed && (
            <div className="flex-1 min-w-0">
              <p className="text-xs font-medium text-gray-900 truncate">{user?.full_name || 'Admin'}</p>
              <p className="text-xs text-gray-400 truncate">{user?.email}</p>
            </div>
          )}
        </div>
        <button onClick={logout} title={collapsed ? 'Sign out' : undefined}
                className={`w-full flex items-center rounded-lg text-xs font-medium text-gray-500
                            hover:bg-gray-100 hover:text-red-600 transition-colors
                            ${collapsed ? 'justify-center px-2 py-2.5' : 'gap-2 px-3 py-2'}`}>
          <svg className="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
          </svg>
          {!collapsed && 'Sign out'}
        </button>
      </div>
    </aside>
  )
}
