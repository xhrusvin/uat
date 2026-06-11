import { create } from 'zustand'
import { usersApi } from '../services/api'

// Abort controller ref — cancels in-flight requests when a new one starts
let abortController = null

export const useUsersStore = create((set, get) => ({
  users: [],
  total: 0,
  page: 1,
  perPage: 20,
  search: '',
  dateFrom: '',
  dateTo: '',
  listLoading: false,   // separate from drawer loading
  drawerLoading: false,
  saving: false,
  error: null,
  selectedUser: null,

  // ── Setters — update state then fetch, passing new values directly ──────────
  setSearch: (search) => {
    set({ search, page: 1, error: null })
    get()._fetchWith({ ...get(), search, page: 1 })
  },

  setPage: (page) => {
    set({ page })
    get()._fetchWith({ ...get(), page })
  },

  setPerPage: (perPage) => {
    set({ perPage, page: 1 })
    get()._fetchWith({ ...get(), perPage, page: 1 })
  },

  setDateRange: (dateFrom, dateTo) => {
    set({ dateFrom, dateTo, page: 1, error: null })
    get()._fetchWith({ ...get(), dateFrom, dateTo, page: 1 })
  },

  clearFilters: () => {
    set({ search: '', dateFrom: '', dateTo: '', page: 1, error: null })
    get()._fetchWith({ ...get(), search: '', dateFrom: '', dateTo: '', page: 1 })
  },

  // ── Internal fetch — receives explicit params to avoid stale closure ────────
  _fetchWith: async ({ page, perPage, search, dateFrom, dateTo }) => {
    // Cancel any previous in-flight request
    if (abortController) abortController.abort()
    abortController = new AbortController()

    set({ listLoading: true, error: null })

    const params = { skip: (page - 1) * perPage, limit: perPage }
    if (search)   params.search    = search
    if (dateFrom) params.date_from = dateFrom
    if (dateTo)   params.date_to   = dateTo

    try {
      const { data } = await usersApi.list(params, abortController.signal)
      set({ users: data.users, total: data.total, listLoading: false })
    } catch (err) {
      // Ignore abort errors — they're intentional
      if (err.name === 'CanceledError' || err.code === 'ERR_CANCELED') return
      const msg = err.response?.data?.detail
        || err.message
        || 'Failed to load users. Check your connection and API key.'
      set({ error: msg, listLoading: false })
    }
  },

  // Public fetch — reads current state (called from components on mount)
  fetchUsers: () => {
    const { page, perPage, search, dateFrom, dateTo } = get()
    get()._fetchWith({ page, perPage, search, dateFrom, dateTo })
  },

  // ── Single user fetch (drawer) — uses separate loading flag ────────────────
  fetchUser: async (id) => {
    set({ drawerLoading: true, selectedUser: null })
    try {
      const { data } = await usersApi.get(id)
      set({ selectedUser: data, drawerLoading: false })
    } catch (err) {
      set({ drawerLoading: false })
    }
  },

  // ── Update user ────────────────────────────────────────────────────────────
  updateUser: async (id, payload) => {
    set({ saving: true })
    try {
      const { data } = await usersApi.update(id, payload)
      set((state) => ({
        saving: false,
        selectedUser: data,
        users: state.users.map((u) => u.id === id ? data : u),
      }))
      return { success: true }
    } catch (err) {
      const msg = err.response?.data?.detail || 'Failed to save changes'
      set({ saving: false })
      return { success: false, error: msg }
    }
  },

  clearSelected: () => set({ selectedUser: null }),
}))
