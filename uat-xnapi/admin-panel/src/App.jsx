import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { Toaster } from 'react-hot-toast'
import Layout from './components/Layout'
import ProtectedRoute from './components/ProtectedRoute'
import LoginPage from './pages/LoginPage'
import DashboardPage from './pages/DashboardPage'
import UsersPage from './pages/UsersPage'
import ShiftListPage from './pages/ShiftListPage'
import ShiftsPage from './pages/ShiftsPage'
import ClientTypeListPage from './pages/ClientTypeListPage'

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
          <Route path="xn-api/shifts"        element={<ShiftListPage />} />
          <Route path="xn-api/client-type-list" element={<ClientTypeListPage />} />
        </Route>
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </BrowserRouter>
  )
}
