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

export const shiftsDbApi = {
  list:   (body)   => usersClient.post('/shifts-db/', body),
  get:    (id)     => usersClient.get(`/shifts-db/${id}`),
  detail: (id)     => usersClient.get(`/shifts-db/${id}/detail`),
}

// No auth required
const publicClient = axios.create({ baseURL: BASE_URL, timeout: 15000 })

export const recruitmentsApi = {
  detail: (id) => publicClient.post('/recruitments/detail', { _id: id }),
}

export const clientsApi = {
  sync:    (payload) => usersClient.post('/clients/sync', payload),
  list:    (params)  => usersClient.get('/clients/', { params }),
  get:     (id)      => usersClient.get(`/clients/${id}`),
}

export const userTypesApi = {
  list:    (body)   => usersClient.post('/user-types/', body),
  create:  (data)   => usersClient.post('/user-types/', data),
  update:  (id, data) => usersClient.patch(`/user-types/${id}`, data),
  delete:  (id)     => usersClient.delete(`/user-types/${id}`),
}

export const sequencesApi = {
  list:    (params) => usersClient.get('/sequences/', { params }),
  create:  (data)   => usersClient.post('/sequences/', data),
  update:  (id, data) => usersClient.patch(`/sequences/${id}`, data),
  delete:  (id)     => usersClient.delete(`/sequences/${id}`),
}

export const criteriaApi = {
  list:    (params) => usersClient.get('/criteria/', { params }),
  create:  (data)   => usersClient.post('/criteria/', data),
  update:  (id, data) => usersClient.patch(`/criteria/${id}`, data),
  delete:  (id)     => usersClient.delete(`/criteria/${id}`),
}

export const commonApi = {
  clientTypeList:     () => usersClient.get('/common/client-type-list'),
  clientTypesFromDb:  () => usersClient.get('/common/client-types'),
  clientDetail:       (id) => usersClient.post('/common/client-detail', { client_id: id }),
}

export const shiftsApi = {
  list: (payload) => usersClient.post('/shifts/list', payload),
}

export const usersApi = {
  // signal is an AbortController.signal — cancels the request if a new one starts
  list:   (params, signal) => usersClient.get('/users/', { params, signal }),
  get:    (id)             => usersClient.get(`/users/${id}`),
  update: (id, data)       => usersClient.patch(`/users/${id}`, data),
}

export default authClient
