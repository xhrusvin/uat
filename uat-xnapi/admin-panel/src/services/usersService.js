// Module-level singleton service — NOT a React hook.
// All fetch logic lives here. Components call these functions directly.

import { useUsersStore } from '../store/usersStore'
import { usersApi } from './api'

let abortController = null
let initialized     = false
let fetchTimer      = null

// Build params from EXPLICIT values — never reads from store during fetch
function makeParams({ page, perPage, search, dateFrom, dateTo }) {
  const params = { skip: (page - 1) * perPage, limit: perPage }
  if (search)   params.search    = search
  if (dateFrom) params.date_from = dateFrom
  if (dateTo)   params.date_to   = dateTo
  return params
}

// Debounced fetch with explicit params
function scheduleFetch(params) {
  clearTimeout(fetchTimer)
  fetchTimer = setTimeout(() => doFetch(params), 80)
}

async function doFetch(params) {
  if (abortController) abortController.abort()
  const controller = new AbortController()
  abortController = controller

  useUsersStore.getState().setListLoading(true)
  useUsersStore.getState().setError(null)

  try {
    const { data } = await usersApi.list(params, controller.signal)
    if (controller.signal.aborted) return
    useUsersStore.getState().setUsers(data.users, data.total)
    initialized = true
  } catch (err) {
    if (err.name === 'CanceledError' || err.code === 'ERR_CANCELED') return
    if (controller.signal.aborted) return
    useUsersStore.getState().setError(
      err.response?.data?.detail || 'Failed to load users'
    )
    useUsersStore.getState().setListLoading(false)
  }
}

// Read current state snapshot for cases where we don't have explicit values
function currentParams() {
  const { page, perPage, search, dateFrom, dateTo } = useUsersStore.getState()
  return makeParams({ page, perPage, search, dateFrom, dateTo })
}

export const usersService = {
  // Only fetches the first time — safe to call on every page mount
  init() {
    if (!initialized && !useUsersStore.getState().listLoading) {
      doFetch(currentParams())
    }
  },

  // Force refresh with current filters
  refresh() {
    doFetch(currentParams())
  },

  setPage(page) {
    useUsersStore.getState().setPage(page)
    // Read other params from store, use explicit page
    const { perPage, search, dateFrom, dateTo } = useUsersStore.getState()
    scheduleFetch(makeParams({ page, perPage, search, dateFrom, dateTo }))
  },

  setPerPage(perPage) {
    useUsersStore.getState().setPerPage(perPage)
    const { search, dateFrom, dateTo } = useUsersStore.getState()
    scheduleFetch(makeParams({ page: 1, perPage, search, dateFrom, dateTo }))
  },

  setSearch(search) {
    useUsersStore.getState().setSearch(search)
    const { perPage, dateFrom, dateTo } = useUsersStore.getState()
    scheduleFetch(makeParams({ page: 1, perPage, search, dateFrom, dateTo }))
  },

  setDateRange(dateFrom, dateTo) {
    useUsersStore.getState().setDateRange(dateFrom, dateTo)
    const { perPage, search } = useUsersStore.getState()
    scheduleFetch(makeParams({ page: 1, perPage, search, dateFrom, dateTo }))
  },

  clearFilters() {
    useUsersStore.getState().clearFilters()
    const { perPage } = useUsersStore.getState()
    scheduleFetch(makeParams({ page: 1, perPage, search: '', dateFrom: '', dateTo: '' }))
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
