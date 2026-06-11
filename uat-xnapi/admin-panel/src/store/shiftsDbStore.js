import { create } from 'zustand'

export const useShiftsDbStore = create((set) => ({
  shifts: [],
  total: 0,
  page: 1,
  perPage: 20,
  search: '',
  status: '',
  dateFrom: '',
  dateTo: '',
  loading: false,
  error: null,
  selected: null,
  drawerLoading: false,

  setPage:    (page)    => set({ page }),
  setPerPage: (perPage) => set({ perPage, page: 1 }),
  setSearch:  (search)  => set({ search, page: 1 }),
  setStatus:  (status)  => set({ status, page: 1 }),
  setDates:   (dateFrom, dateTo) => set({ dateFrom, dateTo, page: 1 }),
  clearFilters: () => set({ search: '', status: '', dateFrom: '', dateTo: '', page: 1 }),

  setLoading:      (v) => set({ loading: v }),
  setError:        (v) => set({ error: v }),
  setData: (data, total) => set({ shifts: data, total, loading: false, error: null }),
  setSelected:     (v) => set({ selected: v, drawerLoading: false }),
  setDrawerLoading:(v) => set({ drawerLoading: v }),
  clearSelected:   ()  => set({ selected: null }),
}))
