import { create } from 'zustand'

export const useShiftsDbStore = create((set) => ({
  shifts: [],
  total: 0,
  page: 1,
  perPage: 20,
  search: '',
  status: '',
  clientId: '',
  userType: '',
  automationStatus: '',
  criteriaField: '',   // DB field name from criteria collection
  criteriaLabel: '',   // Display label e.g. 'Client', 'User Type'
  dateFrom: '',
  dateTo: '',
  loading: false,
  error: null,
  selected: null,
  drawerLoading: false,

  setPage:    (page)    => set({ page }),
  setPerPage: (perPage) => set({ perPage, page: 1 }),
  setSearch:   (search)   => set({ search, page: 1 }),
  setStatus:   (status)   => set({ status, page: 1 }),
  setClientId:        (clientId)        => set({ clientId, page: 1 }),
  setUserType:        (userType)        => set({ userType, page: 1 }),
  setAutomationStatus:(automationStatus) => set({ automationStatus, page: 1 }),
  setCriteriaField:   (criteriaField, criteriaLabel) => set({ criteriaField, criteriaLabel: criteriaLabel || criteriaField, page: 1 }),
  setDates:   (dateFrom, dateTo) => set({ dateFrom, dateTo, page: 1 }),
  clearFilters: () => set({ search: '', status: '', clientId: '', userType: '', automationStatus: '', criteriaField: '', criteriaLabel: '', dateFrom: '', dateTo: '', page: 1 }),

  setLoading:      (v) => set({ loading: v }),
  setError:        (v) => set({ error: v }),
  setData: (data, total) => set({ shifts: data, total, loading: false, error: null }),
  setSelected:     (v) => set({ selected: v, drawerLoading: false }),
  setDrawerLoading:(v) => set({ drawerLoading: v }),
  clearSelected:   ()  => set({ selected: null }),
}))
