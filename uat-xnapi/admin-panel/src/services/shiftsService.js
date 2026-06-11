import { useShiftsStore } from '../store/shiftsStore'
import { shiftsApi } from './api'

let activeController = null

async function execute(payload) {
  if (activeController) activeController.abort()
  activeController = new AbortController()

  useShiftsStore.getState().setLoading(true)
  useShiftsStore.getState().setError(null)

  try {
    const { data } = await shiftsApi.list(payload)
    useShiftsStore.getState().setData(data)
  } catch (err) {
    if (err.name === 'CanceledError' || err.code === 'ERR_CANCELED') return
    useShiftsStore.getState().setError(
      err.response?.data?.detail || 'Failed to fetch shifts'
    )
    useShiftsStore.getState().setLoading(false)
  }
}

function buildPayload(overrides = {}) {
  const s = useShiftsStore.getState()
  const payload = {
    search:     overrides.search     ?? s.search,
    page:       overrides.page       ?? s.page,
    per_page:   overrides.perPage    ?? s.perPage,
    sort_by:    overrides.sortBy     ?? s.sortBy,
    sort_order: overrides.sortOrder  ?? s.sortOrder,
  }
  const startDate = overrides.startDate ?? s.startDate
  const endDate   = overrides.endDate   ?? s.endDate
  if (startDate) payload.start_date = startDate
  if (endDate)   payload.end_date   = endDate

  const locations = overrides.locationFilters ?? s.locationFilters
  if (locations?.length) payload.filters = { location: locations }

  return payload
}

let searchTimer = null

export const shiftsService = {
  fetch(overrides = {}) {
    execute(buildPayload(overrides))
  },

  setPage(page) {
    useShiftsStore.getState().setPage(page)
    execute(buildPayload({ page }))
  },

  setPerPage(perPage) {
    useShiftsStore.getState().setPerPage(perPage)
    execute(buildPayload({ perPage, page: 1 }))
  },

  setSearch(search) {
    useShiftsStore.getState().setSearch(search)
    clearTimeout(searchTimer)
    searchTimer = setTimeout(() => execute(buildPayload({ search, page: 1 })), 500)
  },

  setDates(startDate, endDate) {
    useShiftsStore.getState().setDates(startDate, endDate)
    execute(buildPayload({ startDate, endDate, page: 1 }))
  },

  setSortOrder(sortOrder) {
    useShiftsStore.getState().setSortOrder(sortOrder)
    execute(buildPayload({ sortOrder }))
  },

  refresh() {
    execute(buildPayload())
  },
}
