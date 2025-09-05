'use client'
import { useEffect, useState } from 'react'
import axios from 'axios'

export default function Home() {
  const [health, setHealth] = useState<any>(null)
  useEffect(() => {
    axios.get((process.env.NEXT_PUBLIC_API_BASE || 'http://localhost:8000') + '/healthz')
      .then(r => setHealth(r.data))
      .catch(() => setHealth({ status: 'unreachable' }))
  }, [])
  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-semibold">Welcome</h1>
      <p>This is your Next.js frontend, wired to the FastAPI backend.</p>
      <pre className="bg-white p-3 rounded border text-sm">{JSON.stringify(health, null, 2)}</pre>
    </div>
  )
}
