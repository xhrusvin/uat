import axios from 'axios'

const BASE_URL = import.meta.env.VITE_API_URL || ''
const API_KEY  = import.meta.env.VITE_API_KEY  || 'xh-uat-9f4a2c8b1d6e3f7a0b5c9d2e4f8a1b3c'

// ── Auth client — JWT token ───────────────────────────────────────────────────
const authClient = axios.create({ baseURL: BASE_URL, timeout: 15000 })
authClient.interceptors.request.use((config) => {
  const token = localStorage.getItem('xh_admin_token')
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
})

// ── Users client — API key for all /users/ calls ─────────────────────────────
const usersClient = axios.create({ baseURL: BASE_URL, timeout: 15000 })
usersClient.interceptors.request.use((config) => {
  config.headers.Authorization = `Bearer ${API_KEY}`
  return config
})

export const authApi = {
  login: (email, password) => authClient.post('/auth/login', { email, password }),
  me:    ()                 => authClient.get('/auth/me'),
}

export const usersApi = {
  // signal is an AbortController.signal — cancels the request if a new one starts
  list:   (params, signal) => usersClient.get('/users/', { params, signal }),
  get:    (id)             => usersClient.get(`/users/${id}`),
  update: (id, data)       => usersClient.patch(`/users/${id}`, data),
}

export default authClient
