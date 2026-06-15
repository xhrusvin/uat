import { create } from 'zustand'

export const useShiftsStore = create((set) => ({
  shifts: [],
  total: 0,
  page: 1,
  perPage: 20,
  startDate: '',
  endDate: '',
  search: '',
  sortBy: 'date',
  sortOrder: 'desc',
  locationFilters: [],
  loading: false,
  error: null,
  rawResponse: null,
  syncResult: null,

  setPage:     (page)      => set({ page }),
  setPerPage:  (perPage)   => set({ perPage, page: 1 }),
  setSearch:   (search)    => set({ search, page: 1 }),
  setDates:    (startDate, endDate) => set({ startDate, endDate, page: 1 }),
  setSortBy:   (sortBy)    => set({ sortBy }),
  setSortOrder:(sortOrder) => set({ sortOrder }),
  setLocations:(locs)      => set({ locationFilters: locs, page: 1 }),

  setLoading:  (v)    => set({ loading: v }),
  setError:    (v)    => set({ error: v }),
  setSyncResult: (v) => set({ syncResult: v }),
  setData:     (data) => set({
    shifts:      Array.isArray(data.data) ? data.data : [],
    total:       data.total || 0,
    rawResponse: data,
    loading:     false,
    error:       null,
  }),
}))
