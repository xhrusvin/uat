import { create } from 'zustand'
import { usersApi } from '../services/api'

export const useUsersStore = create((set, get) => ({
  users: [],
  total: 0,
  page: 1,
  perPage: 20,
  search: '',
  loading: false,
  error: null,
  selectedUser: null,

  setSearch: (search) => {
    set({ search, page: 1 })
    get().fetchUsers()
  },

  setPage: (page) => {
    set({ page })
    get().fetchUsers()
  },

  setPerPage: (perPage) => {
    set({ perPage, page: 1 })
    get().fetchUsers()
  },

  fetchUsers: async () => {
    const { page, perPage, search } = get()
    set({ loading: true, error: null })
    try {
      const { data } = await usersApi.list({
        skip: (page - 1) * perPage,
        limit: perPage,
        ...(search ? { search } : {}),
      })
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

  clearSelected: () => set({ selectedUser: null }),
}))
