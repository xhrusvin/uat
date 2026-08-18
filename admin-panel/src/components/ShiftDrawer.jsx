import { useEffect } from 'react'
import { useShiftsDbStore } from '../store/shiftsDbStore'
import { shiftsDbService } from '../services/shiftsDbService'

function Row({ label, value }) {
  if (value === null || value === undefined || value === '') return null
  return (
    <div>
      <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-0.5">{label}</p>
      <p className="text-sm text-gray-900">{String(value)}</p>
    </div>
  )
}

function Section({ title, children }) {
  return (
    <div className="mb-6">
      <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3
                     pb-2 border-b border-gray-100">
        {title}
      </h4>
      <div className="grid grid-cols-2 gap-4">{children}</div>
    </div>
  )
}

const STATUS_COLORS = {
  'Upcoming':            'bg-blue-100 text-blue-700',
  'To be assigned':      'bg-yellow-100 text-yellow-700',
  'Completed':           'bg-green-100 text-green-700',
  'Cancelled':           'bg-red-100 text-red-600',
  'In Progress':         'bg-purple-100 text-purple-700',
}

export default function ShiftDrawer({ shiftId, onClose }) {
  const shift         = useShiftsDbStore((s) => s.selected)
  const loading       = useShiftsDbStore((s) => s.drawerLoading)
  const clearSelected = useShiftsDbStore((s) => s.clearSelected)

  useEffect(() => {
    if (shiftId) shiftsDbService.fetchOne(shiftId)
    return () => clearSelected()
  }, [shiftId])

  if (!shiftId) return null

  const statusCls = STATUS_COLORS[shift?.status] || 'bg-gray-100 text-gray-600'

  return (
    <>
      <div className="fixed inset-0 bg-black/30 z-40" onClick={onClose} />
      <div className="fixed right-0 top-0 h-full w-full max-w-lg bg-white z-50 shadow-2xl flex flex-col">

        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <div>
            <h2 className="text-base font-semibold text-gray-900">Shift Details</h2>
            {shift?.name && <p className="text-xs text-gray-400 font-mono mt-0.5">{shift.name}</p>}
          </div>
          <button onClick={onClose} className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 hover:text-gray-600">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-6 py-5">
          {loading ? (
            <div className="flex items-center justify-center h-40">
              <div className="w-6 h-6 border-2 border-t-transparent rounded-full animate-spin"
                   style={{ borderColor: '#1e7a38', borderTopColor: 'transparent' }} />
            </div>
          ) : shift ? (
            <>
              {/* Status badge + date hero */}
              <div className="flex items-center gap-3 mb-6">
                <div className="w-12 h-12 rounded-xl flex items-center justify-center flex-shrink-0"
                     style={{ backgroundColor: '#e8f5ec' }}>
                  <svg className="w-6 h-6" style={{ color: '#1e7a38' }} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                      d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
                  </svg>
                </div>
                <div>
                  <p className="text-lg font-bold text-gray-900">
                    {shift.date
                      ? (typeof shift.date === 'string' && shift.date.includes('T')
                          ? new Date(shift.date).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })
                          : shift.date)
                      : '—'}
                  </p>
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusCls}`}>
                    {shift.status || '—'}
                  </span>
                </div>
              </div>

              {/* Client */}
              <Section title="Client">
                <div className="col-span-2">
                  <Row label="Client Name" value={shift.client_name} />
                </div>
                <Row label="Client ID"    value={shift.client_id} />
                <Row label="Client Type"  value={shift.client_type} />
                <Row label="Email"        value={shift.client_email} />
                <Row label="Phone"        value={shift.client_phone} />
                {shift.client_address && (
                  <div className="col-span-2"><Row label="Address" value={shift.client_address} /></div>
                )}
                {shift.client_preference && shift.client_preference.length > 0 && (
                  <div className="col-span-2">
                    <p className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-1.5">Client Tags</p>
                    <div className="flex flex-wrap gap-1.5">
                      {shift.client_preference.map((tag) => (
                        <span key={tag.id || tag.name}
                              className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-50 text-blue-700 border border-blue-100">
                          {tag.name}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </Section>

              {/* Shift info */}
              <Section title="Shift Information">
                <Row label="Shift Code"   value={shift.name} />
                <Row label="XN ID"        value={shift.shift_xn_id} />
                <Row label="Location"     value={shift.location} />
                <Row label="County"       value={shift.client_county} />
                <Row label="Timing"       value={shift.shift_timing} />
                <Row label="Start Time"   value={shift.start_time} />
                <Row label="End Time"     value={shift.end_time} />
                <Row label="Unit"         value={shift.unit} />
                <Row label="User Type"    value={shift.user_type} />
                <Row label="Type"         value={shift.client_type} />
                <Row label="Premium"      value={shift.is_premium ? 'Yes' : 'No'} />
                <Row label="Pay Rate"     value={shift.rate ? `€${shift.rate}/hr` : null} />
              </Section>

              {/* Staff */}
              <Section title="Assigned Staff">
                <div className="col-span-2">
                  <Row label="Staff Name"  value={shift.assigned_staff || 'Unassigned'} />
                </div>
                <Row label="Staff Email"   value={shift.staff_email} />
                <Row label="Booking Type"  value={shift.booking_type} />
              </Section>

              {/* Admin */}
              <Section title="Admin">
                <Row label="Created By"    value={shift.created_by} />
                <Row label="Regional Mgr"  value={shift.regional_manager} />
                <Row label="SDR"           value={shift.sdr_name} />
                <Row label="PO Code"       value={shift.po_code} />
                <Row label="Invoice #"     value={shift.invoice_number} />
                <Row label="Working Hours" value={shift.working_hours} />
              </Section>

              {/* Slots */}
              {shift.slots?.length > 0 && (
                <div className="mb-6">
                  <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3
                                 pb-2 border-b border-gray-100">
                    Slots ({shift.slots.length})
                  </h4>
                  <div className="space-y-2">
                    {shift.slots.map((slot, i) => (
                      <div key={i} className="bg-gray-50 rounded-lg px-4 py-3 text-sm">
                        <div className="flex items-center justify-between">
                          <span className="font-medium text-gray-700">
                            {slot.date
                              ? (typeof slot.date === 'string' && slot.date.includes('T')
                                  ? new Date(slot.date).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })
                                  : slot.date)
                              : '—'}
                          </span>
                          <span className="text-gray-500 text-xs">
                            {slot.start_time} – {slot.end_time}
                          </span>
                        </div>
                        <div className="flex gap-4 mt-1 text-xs text-gray-400">
                          <span>{slot.shift_type}</span>
                          {slot.shift_xn_id && <span className="font-mono">{slot.shift_xn_id}</span>}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Timestamps */}
              <Section title="Timestamps">
                <Row label="Created At" value={shift.created_at ? new Date(shift.created_at).toLocaleString() : null} />
                <Row label="Updated At" value={shift.updated_at ? new Date(shift.updated_at).toLocaleString() : null} />
              </Section>
            </>
          ) : (
            <p className="text-sm text-gray-400 text-center py-12">Shift not found.</p>
          )}
        </div>
      </div>
    </>
  )
}
