import { create } from 'zustand'

// Pure state store — NO fetching logic inside
// All fetching is done by the UsersPage component
export const useUsersStore = create((set) => ({
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

  // ── Simple setters — just update state, no side effects ───────────────────
  setPage:    (page)    => set({ page }),
  setPerPage: (perPage) => set({ perPage, page: 1 }),
  setSearch:  (search)  => set({ search, page: 1 }),
  setDateRange: (dateFrom, dateTo) => set({ dateFrom, dateTo, page: 1 }),
  clearFilters: () => set({ search: '', dateFrom: '', dateTo: '', page: 1 }),

  // ── Loading state setters (called by the component) ───────────────────────
  setListLoading: (v) => set({ listLoading: v }),
  setError:       (v) => set({ error: v }),
  setUsers:       (users, total) => set({ users, total, listLoading: false, error: null }),

  // ── Drawer ────────────────────────────────────────────────────────────────
  setSelectedUser:  (user) => set({ selectedUser: user, drawerLoading: false }),
  setDrawerLoading: (v)    => set({ drawerLoading: v }),
  clearSelected:    ()     => set({ selectedUser: null }),

  // ── Update user in list after save ────────────────────────────────────────
  updateUserInList: (id, data) =>
    set((s) => ({
      saving: false,
      selectedUser: data,
      users: s.users.map((u) => (u.id === id ? data : u)),
    })),
  setSaving: (v) => set({ saving: v }),
}))
