import { create } from 'zustand'
import { usersApi } from '../services/api'

let currentRequest = null   // holds the active axios cancellation token

export const useUsersStore = create((set, get) => ({
  users: [],
  total: 0,
  page: 1,
  perPage: 20,
  search: '',
  dateFrom: '',
  dateTo: '',
  listLoading: false,
  drawerLoading: false,
  saving: false,
  error: null,
  selectedUser: null,
  hasFetched: false,        // true once the first successful load completes

  // ── Internal: single fetch entry-point ─────────────────────────────────────
  _fetch: async (overrides = {}) => {
    // Merge overrides into current state so callers don't need to pass everything
    const state  = { ...get(), ...overrides }
    const { page, perPage, search, dateFrom, dateTo } = state

    // Cancel any previous in-flight request
    if (currentRequest) {
      currentRequest.abort()
      currentRequest = null
    }
    const controller = new AbortController()
    currentRequest = controller

    set({ listLoading: true, error: null })

    const params = { skip: (page - 1) * perPage, limit: perPage }
    if (search)   params.search    = search
    if (dateFrom) params.date_from = dateFrom
    if (dateTo)   params.date_to   = dateTo

    try {
      const { data } = await usersApi.list(params, controller.signal)
      // Only update if this request wasn't cancelled
      if (currentRequest === controller) {
        set({ users: data.users, total: data.total, listLoading: false, hasFetched: true })
        currentRequest = null
      }
    } catch (err) {
      if (err.name === 'CanceledError' || err.code === 'ERR_CANCELED') return
      set({
        error: err.response?.data?.detail || 'Failed to load users',
        listLoading: false,
      })
      currentRequest = null
    }
  },

  // ── Public actions ──────────────────────────────────────────────────────────

  // Called on page mount — only fetches if not already loaded
  initUsers: () => {
    const { hasFetched, listLoading } = get()
    if (!hasFetched && !listLoading) get()._fetch()
  },

  // Force a fresh fetch (e.g. Refresh button)
  fetchUsers: () => get()._fetch(),

  setSearch: (search) => {
    set({ search, page: 1 })
    get()._fetch({ search, page: 1 })
  },

  setPage: (page) => {
    set({ page })
    get()._fetch({ page })
  },

  setPerPage: (perPage) => {
    set({ perPage, page: 1 })
    get()._fetch({ perPage, page: 1 })
  },

  setDateRange: (dateFrom, dateTo) => {
    set({ dateFrom, dateTo, page: 1 })
    get()._fetch({ dateFrom, dateTo, page: 1 })
  },

  clearFilters: () => {
    set({ search: '', dateFrom: '', dateTo: '', page: 1 })
    get()._fetch({ search: '', dateFrom: '', dateTo: '', page: 1 })
  },

  // ── Drawer ──────────────────────────────────────────────────────────────────
  fetchUser: async (id) => {
    set({ drawerLoading: true, selectedUser: null })
    try {
      const { data } = await usersApi.get(id)
      set({ selectedUser: data, drawerLoading: false })
    } catch {
      set({ drawerLoading: false })
    }
  },

  clearSelected: () => set({ selectedUser: null }),

  // ── Update ──────────────────────────────────────────────────────────────────
  updateUser: async (id, payload) => {
    set({ saving: true })
    try {
      const { data } = await usersApi.update(id, payload)
      set((s) => ({
        saving: false,
        selectedUser: data,
        users: s.users.map((u) => (u.id === id ? data : u)),
      }))
      return { success: true }
    } catch (err) {
      set({ saving: false })
      return { success: false, error: err.response?.data?.detail || 'Failed to save' }
    }
  },
}))
