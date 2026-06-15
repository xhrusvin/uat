import { useShiftsDbStore } from '../store/shiftsDbStore'
import { shiftsDbApi } from './api'

let controller = null
let searchTimer = null

function getParams(ov = {}) {
  const s = useShiftsDbStore.getState()
  const page    = ov.page    ?? s.page
  const perPage = ov.perPage ?? s.perPage
  const p = { skip: (page - 1) * perPage, limit: perPage }
  const search   = ov.search   ?? s.search
  const status   = ov.status   ?? s.status
  const clientId        = ov.clientId        ?? s.clientId
  const userType        = ov.userType        ?? s.userType
  const automationStatus = ov.automationStatus ?? s.automationStatus
  const dateFrom        = ov.dateFrom        ?? s.dateFrom
  const dateTo          = ov.dateTo          ?? s.dateTo
  if (search)           p.search            = search
  if (status)           p.status            = status
  if (clientId)         p.client_id         = clientId
  if (userType)         p.user_type         = userType
  if (automationStatus) p.automation_status = automationStatus
  if (dateFrom)         p.date_from         = dateFrom
  if (dateTo)           p.date_to           = dateTo
  return p
}

async function execute(params) {
  if (controller) controller.abort()
  controller = new AbortController()
  useShiftsDbStore.getState().setLoading(true)
  useShiftsDbStore.getState().setError(null)
  try {
    const { data } = await shiftsDbApi.list(params)
    useShiftsDbStore.getState().setData(data.data || [], data.total || 0)
  } catch (err) {
    if (err.name === 'CanceledError' || err.code === 'ERR_CANCELED') return
    useShiftsDbStore.getState().setError(err.response?.data?.detail || 'Failed to load shifts')
    useShiftsDbStore.getState().setLoading(false)
  }
}

export const shiftsDbService = {
  init() {
    const s = useShiftsDbStore.getState()
    if (!s.loading && s.shifts.length === 0) execute(getParams())
  },
  refresh()         { execute(getParams()) },
  setPage(page)     { useShiftsDbStore.getState().setPage(page);    execute(getParams({ page })) },
  setPerPage(pp)    { useShiftsDbStore.getState().setPerPage(pp);   execute(getParams({ perPage: pp, page: 1 })) },
  setStatus(status)     { useShiftsDbStore.getState().setStatus(status);   execute(getParams({ status, page: 1 })) },
  setClientId(clientId)               { useShiftsDbStore.getState().setClientId(clientId);               execute(getParams({ clientId, page: 1 })) },
  setUserType(userType)               { useShiftsDbStore.getState().setUserType(userType);               execute(getParams({ userType, page: 1 })) },
  setAutomationStatus(automationStatus) { useShiftsDbStore.getState().setAutomationStatus(automationStatus); execute(getParams({ automationStatus, page: 1 })) },
  setDates(f, t)    { useShiftsDbStore.getState().setDates(f, t);   execute(getParams({ dateFrom: f, dateTo: t, page: 1 })) },
  setSearch(search) {
    useShiftsDbStore.getState().setSearch(search)
    clearTimeout(searchTimer)
    searchTimer = setTimeout(() => execute(getParams({ search, page: 1 })), 500)
  },
  clearFilters() {
    useShiftsDbStore.getState().clearFilters()
    execute(getParams({ search: '', status: '', clientId: '', userType: '', automationStatus: '', dateFrom: '', dateTo: '', page: 1 }))
  },
  async fetchOne(id) {
    useShiftsDbStore.getState().setDrawerLoading(true)
    useShiftsDbStore.getState().clearSelected()
    try {
      const { data } = await shiftsDbApi.get(id)
      useShiftsDbStore.getState().setSelected(data.data || null)
    } catch {
      useShiftsDbStore.getState().setDrawerLoading(false)
    }
  },
}
