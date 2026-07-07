import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { Toaster } from 'react-hot-toast'
import Layout from './components/Layout'
import ProtectedRoute from './components/ProtectedRoute'
import LoginPage from './pages/LoginPage'
import DashboardPage from './pages/DashboardPage'
import UsersPage from './pages/UsersPage'
import ShiftListPage from './pages/ShiftListPage'
import ShiftSyncDetailPage from './pages/ShiftSyncDetailPage'
import ShiftsPage from './pages/ShiftsPage'
import ClientTypeListPage from './pages/ClientTypeListPage'
import ClientTypePage from './pages/ClientTypePage'
import ClientListCallPage from './pages/ClientListCallPage'
import CriteriaPage from './pages/CriteriaPage'
import SequencesPage from './pages/SequencesPage'
import UserTypesPage from './pages/UserTypesPage'
import ActivitiesPage from './pages/ActivitiesPage'
import EndReasonsPage from './pages/EndReasonsPage'
import UserDetailsPage from './pages/UserDetailsPage'
import ClientDetailsPage from './pages/ClientDetailsPage'
import ClientsPage from './pages/ClientsPage'

const basename = import.meta.env.PROD ? '/xnadmin' : '/'

export default function App() {
  return (
    <BrowserRouter basename={basename}>
      <Toaster position="top-right" toastOptions={{ duration: 3000 }} />
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/" element={<ProtectedRoute><Layout /></ProtectedRoute>}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard"      element={<DashboardPage />} />
          <Route path="users"          element={<UsersPage />} />
          <Route path="shifts"         element={<ShiftsPage />} />
          <Route path="xn-api/shifts"            element={<ShiftListPage />} />
          <Route path="xn-api/shift-details"     element={<ShiftSyncDetailPage />} />
          <Route path="xn-api/client-type-list"  element={<ClientTypeListPage />} />
          <Route path="master/client-type"        element={<ClientTypePage />} />
          <Route path="master/clients"             element={<ClientsPage />} />
          <Route path="master/criteria"            element={<CriteriaPage />} />
          <Route path="master/sequences"           element={<SequencesPage />} />
          <Route path="master/user-types"          element={<UserTypesPage />} />
          <Route path="master/activities"           element={<ActivitiesPage />} />
          <Route path="master/end-reasons"           element={<EndReasonsPage />} />
          <Route path="xn-api/client-list"         element={<ClientListCallPage />} />
          <Route path="xn-api/user-details"         element={<UserDetailsPage />} />
          <Route path="xn-api/client-details"       element={<ClientDetailsPage />} />
        </Route>
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </BrowserRouter>
  )
}
