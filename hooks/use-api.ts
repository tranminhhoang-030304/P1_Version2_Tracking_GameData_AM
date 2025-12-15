import { useState, useEffect } from 'react'

interface ApiError {
  message: string
  status?: number
}

export function useApi<T>(url: string) {
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<ApiError | null>(null)

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true)
        setError(null)
        const response = await fetch(url)
        
        if (!response.ok) {
          throw new Error(`API error: ${response.status}`)
        }
        
        const result = await response.json()
        setData(result)
      } catch (err) {
        const apiError: ApiError = {
          message: err instanceof Error ? err.message : 'Failed to fetch data',
          status: err instanceof Error ? undefined : 0,
        }
        setError(apiError)
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [url])

  return { data, loading, error }
}
