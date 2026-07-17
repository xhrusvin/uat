import { useState, useRef, useEffect } from 'react'
import { NavLink, useLocation, useNavigate } from 'react-router-dom'
import { useAuthStore } from '../store/authStore'
import logoSquare from '../assets/logo-square.png'
import logo from '../assets/logo.png'

// ── Navigation config ─────────────────────────────────────────────────────────
const NAV = [
  {
    id: 'dashboard', to: '/dashboard', label: 'Dashboard',
    icon: <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" /></svg>,
  },
  {
    id: 'users', to: '/users', label: 'Users',
    icon: <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" /></svg>,
  },
  {
    id: 'shifts', to: '/shifts', label: 'Shifts',
    icon: <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>,
  },
  {
    id: 'master', label: 'Master',
    icon: <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4" /></svg>,
    children: [
      {
        id: 'client-type', to: '/master/client-type', label: 'Client Type',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" /></svg>,
        description: 'Manage client types',
      },
      {
        id: 'clients', to: '/master/clients', label: 'Clients',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" /></svg>,
        description: 'View synced clients',
      },
      {
        id: 'criteria', to: '/master/criteria', label: 'Criteria',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" /></svg>,
        description: 'Manage filter criteria',
      },
      {
        id: 'sequences', to: '/master/sequences', label: 'Sequences',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 10h16M4 14h16M4 18h16" /></svg>,
        description: 'Manage sequence strategies',
      },
      {
        id: 'end-reasons', to: '/master/end-reasons', label: 'End Reasons',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4" /></svg>,
        description: 'End sequence reasons',
      },
      {
        id: 'activities', to: '/master/activities', label: 'Activity Types',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>,
        description: 'Manage outreach activity types',
      },
      {
        id: 'user-types', to: '/master/user-types', label: 'User Types',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" /></svg>,
        description: 'Manage staff user types',
      },
    ],
  },
  {
    id: 'xn-api', label: 'XN API Calls',
    icon: <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" /></svg>,
    children: [
      {
        id: 'xn-shifts', to: '/xn-api/shifts', label: 'Shift List',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" /></svg>,
        description: 'Fetch & sync shifts from Shift API',
      },
      {
        id: 'xn-shift-details', to: '/xn-api/shift-details', label: 'Shift Details',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>,
        description: 'Sync a single shift from XN API',
      },
      {
        id: 'xn-client-types', to: '/xn-api/client-type-list', label: 'Client Type List',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" /></svg>,
        description: 'Get client types from User API',
      },
      {
        id: 'xn-client-list', to: '/xn-api/client-list', label: 'Client List',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" /></svg>,
        description: 'Fetch & sync clients from User API',
      },
      {
        id: 'xn-user-details', to: '/xn-api/user-details', label: 'User Details',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" /></svg>,
        description: 'Fetch recruitment detail by user ID',
      },
      {
        id: 'xn-client-details', to: '/xn-api/client-details', label: 'Client Details',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" /></svg>,
        description: 'Fetch client details from User API',
      },
    ],
  },
  {
    id: 'webhook-monitor', label: 'Webhook Monitor',
    icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"/></svg>,
    children: [
      {
        id: 'webhook-doc-uploaded', to: '/webhook/document-uploaded', label: 'Document Uploaded',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>,
        description: 'View uploaded document webhooks',
      },
      {
        id: 'webhook-shift-updated', to: '/webhook/shift-updated', label: 'Shift Updated',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>,
        description: 'View shift updated webhooks',
      },
      {
        id: 'webhook-staff-updated', to: '/webhook/staff-updated', label: 'Staff Updated',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/></svg>,
        description: 'View staff updated webhooks',
      },
    ],
  },
  {
    id: 'cron', label: 'Cron Jobs',
    icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>,
    children: [
      {
        id: 'cron-sync-shifts', to: '/cron/sync-shifts', label: 'Sync Latest Shifts',
        icon: <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>,
        description: 'Fetch and sync latest 10 shifts from upstream API',
      },
    ],
  },
]

// ── Sub-sidebar panel ─────────────────────────────────────────────────────────
function SubSidebar({ group, onClose }) {
  const location = useLocation()
  const ref = useRef(null)

  useEffect(() => {
    const handler = (e) => {
      if (ref.current && !ref.current.contains(e.target)) onClose()
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [onClose])

  return (
    <div
      ref={ref}
      className="fixed left-16 top-0 h-full w-56 bg-white border-r border-gray-200
                 shadow-xl z-30 flex flex-col"
      style={{ animation: 'slideIn 0.15s ease-out' }}
    >
      <style>{`@keyframes slideIn { from { transform: translateX(-8px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }`}</style>

      {/* Group header */}
      <div className="px-4 py-4 border-b border-gray-100 flex items-center gap-3">
        <div className="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0"
             style={{ backgroundColor: '#e8f5ec', color: '#1e7a38' }}>
          {group.icon}
        </div>
        <div>
          <p className="text-sm font-semibold text-gray-900">{group.label}</p>
          <p className="text-xs text-gray-400">{group.children.length} item{group.children.length !== 1 ? 's' : ''}</p>
        </div>
      </div>

      {/* Children */}
      <nav className="flex-1 px-2 py-3 space-y-0.5">
        {group.children.map((child) => {
          const isActive = location.pathname === child.to || location.pathname.startsWith(child.to + '/')
          return (
            <NavLink
              key={child.id}
              to={child.to}
              onClick={onClose}
              className={`flex items-start gap-3 px-3 py-2.5 rounded-lg transition-colors group
                          ${isActive
                            ? 'text-[#1e7a38] bg-[#e8f5ec]'
                            : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'}`}
            >
              <div className={`mt-0.5 flex-shrink-0 ${isActive ? 'text-[#1e7a38]' : 'text-gray-400 group-hover:text-gray-600'}`}>
                {child.icon}
              </div>
              <div>
                <p className="text-sm font-medium leading-tight">{child.label}</p>
                {child.description && (
                  <p className="text-xs text-gray-400 mt-0.5 leading-tight">{child.description}</p>
                )}
              </div>
            </NavLink>
          )
        })}
      </nav>

      {/* Close hint */}
      <div className="px-4 py-3 border-t border-gray-100">
        <p className="text-xs text-gray-400">Click outside to close</p>
      </div>
    </div>
  )
}

// ── Main Sidebar ──────────────────────────────────────────────────────────────
export default function Sidebar() {
  const { user, logout } = useAuthStore()
  const location = useLocation()
  const [collapsed, setCollapsed]       = useState(true)
  const [openSubId, setOpenSubId]       = useState(null)
  const [masterExpanded, setMasterExpanded] = useState(
    location.pathname.startsWith('/master')
  )
  const [xnExpanded, setXnExpanded]     = useState(
    location.pathname.startsWith('/xn-api')
  )

  const isChildActive = (item) =>
    item.children?.some(c => location.pathname === c.to || location.pathname.startsWith(c.to + '/'))

  const handleGroupClick = (item) => {
    if (collapsed) {
      setOpenSubId(openSubId === item.id ? null : item.id)
    } else {
      if (item.id === 'master') setMasterExpanded(!masterExpanded)
      else setXnExpanded(!xnExpanded)
      setOpenSubId(null)
    }
  }

  // Close sub-sidebar when route changes
  useEffect(() => { setOpenSubId(null) }, [location.pathname])

  const activeGroup = NAV.find(i => i.id === openSubId && i.children)

  return (
    <>
      <aside className={`relative flex flex-col bg-white border-r border-gray-200 shadow-sm
                         transition-all duration-300 ease-in-out min-h-screen z-40
                         ${collapsed ? 'w-16' : 'w-60'}`}>

        {/* Toggle */}
        <button
          onClick={() => { setCollapsed(!collapsed); setOpenSubId(null) }}
          className="absolute -right-3 top-6 z-50 w-6 h-6 rounded-full bg-white border
                     border-gray-200 shadow-md flex items-center justify-center hover:bg-gray-50"
        >
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
        <nav className="flex-1 px-2 py-3 space-y-0.5">
          {NAV.map((item) => {

            // ── Group with children ───────────────────────────────────────────
            if (item.children) {
              const active   = isChildActive(item)
              const subOpen  = openSubId === item.id

              return (
                <div key={item.id}>
                  <button
                    onClick={() => handleGroupClick(item)}
                    title={collapsed ? item.label : undefined}
                    className={`w-full flex items-center rounded-lg text-sm font-medium transition-colors
                      ${collapsed ? 'justify-center px-2 py-2.5' : 'gap-3 px-3 py-2.5 justify-between'}
                      ${active || subOpen
                        ? 'text-[#1e7a38] bg-[#e8f5ec]'
                        : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'}`}
                  >
                    <div className={`flex items-center ${collapsed ? '' : 'gap-3'}`}>
                      {item.icon}
                      {!collapsed && <span>{item.label}</span>}
                    </div>
                    {!collapsed && (
                      <svg className={`w-3.5 h-3.5 transition-transform ${(item.id === 'master' ? masterExpanded : xnExpanded) ? 'rotate-90' : ''}`}
                        fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                      </svg>
                    )}
                    {/* Collapsed indicator dot when sub is open */}
                    {collapsed && subOpen && (
                      <span className="absolute right-1 top-1 w-2 h-2 rounded-full bg-[#1e7a38]" />
                    )}
                  </button>

                  {/* Inline accordion — expanded sidebar only */}
                  {!collapsed && ((item.id === 'master' ? masterExpanded : xnExpanded) || active) && (
                    <div className="ml-4 mt-0.5 space-y-0.5 pl-3 border-l-2 border-gray-100">
                      {item.children.map((child) => (
                        <NavLink
                          key={child.id}
                          to={child.to}
                          className={({ isActive }) =>
                            `flex items-center gap-2.5 px-3 py-2.5 rounded-lg text-xs font-medium transition-colors
                             ${isActive ? 'text-[#1e7a38] bg-[#e8f5ec]' : 'text-gray-500 hover:bg-gray-100 hover:text-gray-800'}`
                          }
                        >
                          {child.icon}
                          <span>{child.label}</span>
                        </NavLink>
                      ))}
                    </div>
                  )}
                </div>
              )
            }

            // ── Regular item ──────────────────────────────────────────────────
            return (
              <NavLink
                key={item.id}
                to={item.to}
                title={collapsed ? item.label : undefined}
                onClick={() => setOpenSubId(null)}
                className={({ isActive }) =>
                  `flex items-center rounded-lg text-sm font-medium transition-colors
                   ${collapsed ? 'justify-center px-2 py-2.5' : 'gap-3 px-3 py-2.5'}
                   ${isActive ? 'bg-[#e8f5ec] text-[#1e7a38]' : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'}`
                }
              >
                {item.icon}
                {!collapsed && <span>{item.label}</span>}
              </NavLink>
            )
          })}
        </nav>

        {/* User footer */}
        <div className="border-t border-gray-100 py-3 px-2 space-y-0.5">
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
          <button
            onClick={logout}
            title={collapsed ? 'Sign out' : undefined}
            className={`w-full flex items-center rounded-lg text-xs font-medium text-gray-500
                        hover:bg-gray-100 hover:text-red-600 transition-colors
                        ${collapsed ? 'justify-center px-2 py-2.5' : 'gap-2 px-3 py-2'}`}
          >
            <svg className="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
            </svg>
            {!collapsed && 'Sign out'}
          </button>
        </div>
      </aside>

      {/* Sub-sidebar panel — shown when collapsed + group clicked */}
      {activeGroup && collapsed && (
        <SubSidebar group={activeGroup} onClose={() => setOpenSubId(null)} />
      )}
    </>
  )
}
