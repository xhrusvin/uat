import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'

// Remove StrictMode — it double-invokes effects in dev causing duplicate API calls
ReactDOM.createRoot(document.getElementById('root')).render(
  <App />
)
