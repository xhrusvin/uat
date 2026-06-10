import axios from 'axios'

const BASE_URL = import.meta.env.VITE_API_URL || ''
const API_KEY  = import.meta.env.VITE_API_KEY  || 'xh-uat-9f4a2c8b1d6e3f7a0b5c9d2e4f8a1b3c'

// ── Auth API client (uses JWT token) ─────────────────────────────────────────
const authApi_client = axios.create({ baseURL: BASE_URL, timeout: 15000 })

authApi_client.interceptors.request.use((config) => {
  const token = localStorage.getItem('xh_admin_token')
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
})

// ── Users API client (uses API key) ──────────────────────────────────────────
const usersApi_client = axios.create({ baseURL: BASE_URL, timeout: 15000 })

usersApi_client.interceptors.request.use((config) => {
  config.headers.Authorization = `Bearer ${API_KEY}`
  return config
})

// No redirect interceptors on either — ProtectedRoute handles session expiry

export const authApi = {
  login: (email, password) => authApi_client.post('/auth/login', { email, password }),
  me:    ()                 => authApi_client.get('/auth/me'),
}

export const usersApi = {
  list: (params) => usersApi_client.get('/users/', { params }),
  get:  (id)     => usersApi_client.get(`/users/${id}`),
}

// Default export for interceptor attachment in ProtectedRoute
export default authApi_client
