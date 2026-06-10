import { create } from 'zustand'
import { authApi } from '../services/api'

const getStoredUser = () => {
  try {
    const raw = localStorage.getItem('xh_admin_user')
    return raw ? JSON.parse(raw) : null
  } catch { return null }
}

const getStoredToken = () => {
  try {
    return localStorage.getItem('xh_admin_token') || null
  } catch { return null }
}

export const useAuthStore = create((set) => ({
  user:    getStoredUser(),
  token:   getStoredToken(),
  loading: false,
  error:   null,

  login: async (email, password) => {
    set({ loading: true, error: null })
    try {
      // Step 1: get JWT token
      const { data: tokenData } = await authApi.login(email, password)
      const token = tokenData.access_token

      // Step 2: persist token immediately
      localStorage.setItem('xh_admin_token', token)
      set({ token })

      // Step 3: fetch profile using the new token
      const { data: user } = await authApi.me()
      localStorage.setItem('xh_admin_user', JSON.stringify(user))
      set({ user, loading: false })

      return { success: true }
    } catch (err) {
      // Clean up everything on any failure
      localStorage.removeItem('xh_admin_token')
      localStorage.removeItem('xh_admin_user')
      set({ token: null, user: null, loading: false })

      const detail = err.response?.data?.detail
      const msg = Array.isArray(detail)
        ? detail.map((d) => d.msg).join(', ')
        : detail || 'Login failed. Check your credentials.'
      set({ error: msg })
      return { success: false }
    }
  },

  // Called when a protected API call returns 401 (token expired)
  sessionExpired: () => {
    localStorage.removeItem('xh_admin_token')
    localStorage.removeItem('xh_admin_user')
    set({ user: null, token: null, error: 'Session expired. Please sign in again.' })
  },

  logout: () => {
    localStorage.removeItem('xh_admin_token')
    localStorage.removeItem('xh_admin_user')
    set({ user: null, token: null, error: null })
  },

  clearError: () => set({ error: null }),
}))
