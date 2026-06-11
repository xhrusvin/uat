import { create } from 'zustand'
import { usersApi } from '../services/api'

export const useUsersStore = create((set, get) => ({
  users: [],
  total: 0,
  page: 1,
  perPage: 20,
  search: '',
  dateFrom: '',
  dateTo: '',
  loading: false,
  saving: false,
  error: null,
  selectedUser: null,

  setSearch: (search) => { set({ search, page: 1 }); get().fetchUsers() },
  setPage:   (page)   => { set({ page });             get().fetchUsers() },
  setPerPage:(perPage)=> { set({ perPage, page: 1 }); get().fetchUsers() },

  setDateRange: (dateFrom, dateTo) => {
    set({ dateFrom, dateTo, page: 1 })
    get().fetchUsers()
  },

  clearFilters: () => {
    set({ search: '', dateFrom: '', dateTo: '', page: 1 })
    get().fetchUsers()
  },

  fetchUsers: async () => {
    const { page, perPage, search, dateFrom, dateTo } = get()
    set({ loading: true, error: null })
    try {
      const params = {
        skip: (page - 1) * perPage,
        limit: perPage,
      }
      if (search)   params.search    = search
      if (dateFrom) params.date_from = dateFrom
      if (dateTo)   params.date_to   = dateTo

      const { data } = await usersApi.list(params)
      set({ users: data.users, total: data.total, loading: false })
    } catch (err) {
      set({ error: err.response?.data?.detail || 'Failed to load users', loading: false })
    }
  },

  fetchUser: async (id) => {
    set({ loading: true, error: null })
    try {
      const { data } = await usersApi.get(id)
      set({ selectedUser: data, loading: false })
    } catch (err) {
      set({ error: err.response?.data?.detail || 'User not found', loading: false })
    }
  },

  updateUser: async (id, payload) => {
    set({ saving: true, error: null })
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
      set({ saving: false, error: msg })
      return { success: false, error: msg }
    }
  },

  clearSelected: () => set({ selectedUser: null }),
}))
