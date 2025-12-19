"use client"

import { useEffect, useState } from "react"
import { StatsCard } from "@/components/stats-card"
import { ApiErrorAlert } from "@/components/api-error-alert"
import { Skeleton } from "@/components/ui/skeleton"
import { DollarSign, Users, TrendingDown } from "lucide-react"
import { analyticsApi } from "@/lib/api"

export function DashboardStats() {
  const [revenue, setRevenue] = useState<number>(0)
  const [failRate, setFailRate] = useState<number>(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    console.log('[DashboardStats] Component mounted, fetching stats...')
    
    const fetchStats = async () => {
      try {
        setLoading(true)
        setError(null)

        // Fetch level stats to calculate total revenue
        try {
          console.log('[DashboardStats] Calling getLevelStats...')
            const levelStats = await analyticsApi.getLevelStats()
            const statsArray = Array.isArray(levelStats) ? levelStats : levelStats.data || []
            // Prefer server-reported total_coin_spent; fall back to revenue if needed
            const totalRevenue = statsArray.reduce((sum: number, stat: any) => sum + (stat.total_coin_spent || stat.revenue || 0), 0)
          console.log('[DashboardStats] Total revenue calculated:', totalRevenue)
          setRevenue(totalRevenue)
        } catch (err) {
          console.warn('[DashboardStats] getLevelStats failed, trying fallback...', err)
          setRevenue(0)
        }

        // Fetch fail rate data for average fail rate
        try {
          console.log('[DashboardStats] Calling getFailRate...')
          const failRateData = await analyticsApi.getFailRate()
          const avgFailRate = failRateData.length > 0
            ? failRateData.reduce((sum: number, day: any) => sum + (day.fail_rate || 0), 0) / failRateData.length
            : 0
          console.log('[DashboardStats] Average fail rate calculated:', avgFailRate)
          setFailRate(avgFailRate)
        } catch (err) {
          console.warn('[DashboardStats] getFailRate failed', err)
          setFailRate(0)
        }
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : 'Failed to fetch stats'
        console.error('[DashboardStats] Error:', errorMsg)
        setError(errorMsg)
      } finally {
        setLoading(false)
      }
    }

    fetchStats()
  }, [])

  if (error) {
    return <ApiErrorAlert title="Stats Load Error" message={error} />
  }

  if (loading) {
    return (
      <div className="mb-8 grid gap-6 md:grid-cols-3">
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
      </div>
    )
  }

  return (
    <div className="mb-8 grid gap-6 md:grid-cols-3">
      <StatsCard
        title="Total Revenue"
        value={`$${(revenue || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })}`}
        change="+12.5% from last month"
        changeType="positive"
        icon={DollarSign}
        accentColor="cyan"
      />
      <StatsCard
        title="Total Items"
        value="200+"
        change="Across 20 levels"
        changeType="neutral"
        icon={Users}
        accentColor="magenta"
      />
      <StatsCard
        title="Avg Fail Rate"
        value={`${failRate.toFixed(1)}%`}
        change="+3.1% from last month"
        changeType="negative"
        icon={TrendingDown}
        accentColor="green"
      />
    </div>
  )
}
