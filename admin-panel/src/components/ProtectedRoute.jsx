import { useEffect } from 'react'
import { Navigate } from 'react-router-dom'
import { useAuthStore } from '../store/authStore'
import api from '../services/api'

export default function ProtectedRoute({ children }) {
  const { token, sessionExpired } = useAuthStore()

  useEffect(() => {
    const id = api.interceptors.response.use(
      (res) => res,
      (error) => {
        // Only treat 401 from /auth/ endpoints as session expiry
        // /users/ uses API key auth — its 401 should NOT log out the admin
        const url = error.config?.url || ''
        const isAuthEndpoint = url.includes('/auth/')
        if (error.response?.status === 401 && isAuthEndpoint) {
          sessionExpired()
        }
        return Promise.reject(error)
      }
    )
    return () => api.interceptors.response.eject(id)
  }, [])

  if (!token) return <Navigate to="/login" replace />
  return children
}
