import { useUsersStore } from '../store/usersStore'
import { usersApi } from './api'

let activeController = null

// Core fetch — always fires immediately with explicit params, no debounce for pagination
async function execute(params) {
  // Cancel previous request
  if (activeController) activeController.abort()
  activeController = new AbortController()
  const signal = activeController.signal

  useUsersStore.getState().setListLoading(true)
  useUsersStore.getState().setError(null)

  try {
    const { data } = await usersApi.list(params, signal)
    if (signal.aborted) return
    useUsersStore.getState().setUsers(data.users, data.total)
  } catch (err) {
    if (err.name === 'CanceledError' || err.code === 'ERR_CANCELED') return
    if (signal.aborted) return
    useUsersStore.getState().setError(
      err.response?.data?.detail || 'Failed to load users'
    )
    useUsersStore.getState().setListLoading(false)
  }
}

function getParams(overrides = {}) {
  const s = useUsersStore.getState()
  const page    = overrides.page    ?? s.page
  const perPage = overrides.perPage ?? s.perPage
  const search  = overrides.search  ?? s.search
  const dateFrom = overrides.dateFrom ?? s.dateFrom
  const dateTo   = overrides.dateTo   ?? s.dateTo
  const params = { skip: (page - 1) * perPage, limit: perPage }
  if (search)   params.search    = search
  if (dateFrom) params.date_from = dateFrom
  if (dateTo)   params.date_to   = dateTo
  return params
}

// Debounced version for text search only
let searchTimer = null
function scheduleSearch(params) {
  clearTimeout(searchTimer)
  searchTimer = setTimeout(() => execute(params), 500)
}

export const usersService = {
  // Mount init — only fetches once
  init() {
    const s = useUsersStore.getState()
    if (!s.listLoading && s.users.length === 0) {
      execute(getParams())
    }
  },

  refresh() {
    execute(getParams())
  },

  // Pagination — fires immediately, no debounce
  setPage(page) {
    useUsersStore.getState().setPage(page)
    execute(getParams({ page }))
  },

  setPerPage(perPage) {
    useUsersStore.getState().setPerPage(perPage)
    execute(getParams({ perPage, page: 1 }))
  },

  // Search — debounced
  setSearch(search) {
    useUsersStore.getState().setSearch(search)
    scheduleSearch(getParams({ search, page: 1 }))
  },

  // Date range — fires immediately
  setDateRange(dateFrom, dateTo) {
    useUsersStore.getState().setDateRange(dateFrom, dateTo)
    execute(getParams({ dateFrom, dateTo, page: 1 }))
  },

  clearFilters() {
    useUsersStore.getState().clearFilters()
    execute(getParams({ search: '', dateFrom: '', dateTo: '', page: 1 }))
  },

  async fetchUser(id) {
    useUsersStore.getState().setDrawerLoading(true)
    useUsersStore.getState().clearSelected()
    try {
      const { data } = await usersApi.get(id)
      useUsersStore.getState().setSelectedUser(data)
    } catch {
      useUsersStore.getState().setDrawerLoading(false)
    }
  },

  async updateUser(id, payload) {
    useUsersStore.getState().setSaving(true)
    try {
      const { data } = await usersApi.update(id, payload)
      useUsersStore.getState().updateUserInList(id, data)
      return { success: true }
    } catch (err) {
      useUsersStore.getState().setSaving(false)
      return { success: false, error: err.response?.data?.detail || 'Failed to save' }
    }
  },
}
