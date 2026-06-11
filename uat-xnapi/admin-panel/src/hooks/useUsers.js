import { useEffect, useRef, useCallback } from 'react'
import { useUsersStore } from '../store/usersStore'
import { usersApi } from '../services/api'

/**
 * useUsers — owns all fetching logic for the users list.
 * The store is purely passive state; this hook drives it.
 */
export function useUsers() {
  const store = useUsersStore()
  const abortRef   = useRef(null)
  const paramsRef  = useRef(null)

  const fetch = useCallback(async (params) => {
    // Cancel any in-flight request
    if (abortRef.current) {
      abortRef.current.abort()
    }
    const controller = new AbortController()
    abortRef.current = controller

    store.setListLoading(true)
    store.setError(null)

    try {
      const { data } = await usersApi.list(params, controller.signal)
      // Only update state if this request wasn't superseded
      if (!controller.signal.aborted) {
        store.setUsers(data.users, data.total)
      }
    } catch (err) {
      if (err.name === 'CanceledError' || err.code === 'ERR_CANCELED') return
      if (!controller.signal.aborted) {
        store.setError(
          err.response?.data?.detail || 'Failed to load users. Check connection and API key.'
        )
        store.setListLoading(false)
      }
    }
  }, []) // no deps — never recreated

  // Build params from store state and fetch
  const fetchCurrent = useCallback(() => {
    const { page, perPage, search, dateFrom, dateTo } = useUsersStore.getState()
    const params = { skip: (page - 1) * perPage, limit: perPage }
    if (search)   params.search    = search
    if (dateFrom) params.date_from = dateFrom
    if (dateTo)   params.date_to   = dateTo
    paramsRef.current = params
    fetch(params)
  }, [fetch])

  // Fetch once on mount — never re-runs
  useEffect(() => {
    fetchCurrent()
    return () => {
      // Cancel on unmount
      if (abortRef.current) abortRef.current.abort()
    }
  }, []) // empty deps — mount only

  // Single user fetch for drawer
  const fetchUser = useCallback(async (id) => {
    store.setDrawerLoading(true)
    store.clearSelected()
    try {
      const { data } = await usersApi.get(id)
      store.setSelectedUser(data)
    } catch {
      store.setDrawerLoading(false)
    }
  }, [])

  // Update user
  const updateUser = useCallback(async (id, payload) => {
    store.setSaving(true)
    try {
      const { data } = await usersApi.update(id, payload)
      store.updateUserInList(id, data)
      return { success: true }
    } catch (err) {
      store.setSaving(false)
      return { success: false, error: err.response?.data?.detail || 'Failed to save' }
    }
  }, [])

  // Actions that update state then re-fetch
  const actions = {
    setPage: (page) => {
      store.setPage(page)
      const { perPage, search, dateFrom, dateTo } = useUsersStore.getState()
      const params = { skip: (page - 1) * perPage, limit: perPage }
      if (search)   params.search    = search
      if (dateFrom) params.date_from = dateFrom
      if (dateTo)   params.date_to   = dateTo
      fetch(params)
    },
    setPerPage: (perPage) => {
      store.setPerPage(perPage)
      const { search, dateFrom, dateTo } = useUsersStore.getState()
      const params = { skip: 0, limit: perPage }
      if (search)   params.search    = search
      if (dateFrom) params.date_from = dateFrom
      if (dateTo)   params.date_to   = dateTo
      fetch(params)
    },
    setSearch: (search) => {
      store.setSearch(search)
      const { perPage, dateFrom, dateTo } = useUsersStore.getState()
      const params = { skip: 0, limit: perPage }
      if (search)   params.search    = search
      if (dateFrom) params.date_from = dateFrom
      if (dateTo)   params.date_to   = dateTo
      fetch(params)
    },
    setDateRange: (dateFrom, dateTo) => {
      store.setDateRange(dateFrom, dateTo)
      const { perPage, search } = useUsersStore.getState()
      const params = { skip: 0, limit: perPage }
      if (search)   params.search    = search
      if (dateFrom) params.date_from = dateFrom
      if (dateTo)   params.date_to   = dateTo
      fetch(params)
    },
    clearFilters: () => {
      store.clearFilters()
      const { perPage } = useUsersStore.getState()
      fetch({ skip: 0, limit: perPage })
    },
    refresh: fetchCurrent,
    fetchUser,
    updateUser,
  }

  return { ...store, ...actions }
}
