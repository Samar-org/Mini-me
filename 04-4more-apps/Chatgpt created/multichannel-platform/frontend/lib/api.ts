import axios from 'axios'
export const api = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_BASE || 'http://localhost:8000',
  headers: { 'X-Tenant-ID': process.env.NEXT_PUBLIC_TENANT_ID || '' }
})
