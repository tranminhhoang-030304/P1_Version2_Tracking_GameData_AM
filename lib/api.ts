/**
 * API Configuration - Client-Side Only
 * All requests are made directly from browser to backend
 */

// Direct backend URL - no proxy, no server-side routing
export const API_BASE_URL = 'http://127.0.0.1:8000'

/**
 * Fetch wrapper with error handling and logging
 */
export async function apiFetch(
  endpoint: string,
  options: RequestInit = {}
): Promise<any> {
  const url = `${API_BASE_URL}${endpoint.startsWith('/') ? endpoint : '/' + endpoint}`

  try {
    console.log(`[API] Fetching: ${url}`)
    
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    })

    if (!response.ok) {
      throw new Error(`API error: ${response.status} ${response.statusText}`)
    }

    const data = await response.json()
    console.log(`[API] Success: ${url}`, data)
    return data
  } catch (error) {
    console.error(`[API] Failed: ${url}`, error)
    throw error
  }
}

/**
 * Analytics API endpoints - all client-side
 */
export const analyticsApi = {
  // Get top boosters by usage
  getBoosterStats: async () => {
    return apiFetch('/api/analytics/booster-stats')
  },

  // Get level booster breakdown
  getLevelBoosterBreakdown: async (level: string | number) => {
    return apiFetch(`/api/analytics/level-booster-breakdown/${level}`)
  },

  // Get level stats (for dashboard revenue calculation)
  getLevelStats: async () => {
    return apiFetch('/api/analytics/level-stats')
  },

  // Get fail rate data
  getFailRate: async () => {
    return apiFetch('/api/analytics/fail-rate')
  },

  // Get items by level
  getItemsByLevel: async () => {
    return apiFetch('/api/analytics/items-by-level')
  },

  // Get items detail for specific level
  getItemsByLevelDetail: async (level: string | number) => {
    return apiFetch(`/api/analytics/items-by-level/${level}`)
  },

  // Get revenue data
  getRevenue: async () => {
    return apiFetch('/api/analytics/revenue')
  },
}
