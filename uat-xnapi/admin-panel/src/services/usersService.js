// Module-level singleton service — NOT a React hook.
// Owns all users fetching. Components call these functions directly.
// No useEffect loops possible because nothing here subscribes to React.

import { useUsersStore } from '../store/usersStore'
import { usersApi } from './api'

let abortController = null
let initialized = false   // module-level — survives component unmounts

function buildParams() {
  const { page, perPage, search, dateFrom, dateTo } = useUsersStore.getState()
  const params = { skip: (page - 1) * perPage, limit: perPage }
  if (search)   params.search    = search
  if (dateFrom) params.date_from = dateFrom
  if (dateTo)   params.date_to   = dateTo
  return params
}

async function doFetch() {
  const store = useUsersStore.getState()

  // Cancel previous in-flight request
  if (abortController) abortController.abort()
  const controller = new AbortController()
  abortController = controller

  store.setListLoading(true)
  store.setError(null)

  try {
    const { data } = await usersApi.list(buildParams(), controller.signal)
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

export const usersService = {
  // Call on UsersPage mount — only fetches the FIRST time ever
  init() {
    if (!initialized) doFetch()
  },

  // Explicit refresh (Refresh button)
  refresh() {
    doFetch()
  },

  setPage(page) {
    useUsersStore.getState().setPage(page)
    doFetch()
  },

  setPerPage(perPage) {
    useUsersStore.getState().setPerPage(perPage)
    doFetch()
  },

  setSearch(search) {
    useUsersStore.getState().setSearch(search)
    doFetch()
  },

  setDateRange(from, to) {
    useUsersStore.getState().setDateRange(from, to)
    doFetch()
  },

  clearFilters() {
    useUsersStore.getState().clearFilters()
    doFetch()
  },

  async fetchUser(id) {
    const store = useUsersStore.getState()
    store.setDrawerLoading(true)
    store.clearSelected()
    try {
      const { data } = await usersApi.get(id)
      useUsersStore.getState().setSelectedUser(data)
    } catch {
      useUsersStore.getState().setDrawerLoading(false)
    }
  },

  async updateUser(id, payload) {
    const store = useUsersStore.getState()
    store.setSaving(true)
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
