"use client"

import { Sidebar } from "@/components/sidebar"
import { RevenueChart } from "@/components/revenue-chart"
import { DrilldownSection } from "@/components/drilldown-section"
// Đã thêm 'Users' vào dòng import dưới đây
import { DollarSign, ShoppingBag, Activity, TrendingUp, TrendingDown, Users } from "lucide-react"

// 1. Dữ liệu giả lập
const mockChartData = [
  { name: "Level 1", total: 1200, failRate: 12 },
  { name: "Level 2", total: 1800, failRate: 15 },
  { name: "Level 3", total: 2400, failRate: 18 },
  { name: "Level 4", total: 3100, failRate: 22 },
  { name: "Level 5", total: 2900, failRate: 25 },
  { name: "Level 6", total: 3500, failRate: 30 },
  { name: "Level 7", total: 4200, failRate: 35 },
  { name: "Level 8", total: 3900, failRate: 40 },
  { name: "Level 9", total: 4500, failRate: 45 },
  { name: "Level 10", total: 5000, failRate: 50 },
]

const topBoosters = [
  { rank: 1, name: "Double XP", usage: "45,231", trend: "+12%" },
  { rank: 2, name: "Extra Life", usage: "32,105", trend: "+5%" },
  { rank: 3, name: "Time Freeze", usage: "28,450", trend: "-2%" },
  { rank: 4, name: "Shield Boost", usage: "15,300", trend: "+8%" },
]

export default function DashboardPage() {
  const totalRevenue = mockChartData.reduce((acc, item) => acc + item.total, 0)
  const avgFailRate = (
    mockChartData.reduce((acc, item) => acc + item.failRate, 0) / mockChartData.length
  ).toFixed(1)

  return (
    <div className="min-h-screen bg-background text-foreground">
      <Sidebar />
      <main className="pl-64 transition-all duration-300">
        <div className="p-8 space-y-8">
          
          {/* Header */}
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold tracking-tight text-primary neon-text-cyan">
                Dashboard
              </h1>
              <p className="text-muted-foreground mt-1">
                Tổng quan hiệu suất game & Doanh thu (Real-time Simulation)
              </p>
            </div>
          </div>

          {/* Stats Cards */}
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            {/* Card 1 */}
            <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
              <div className="flex flex-row items-center justify-between space-y-0 pb-2">
                <h3 className="tracking-tight text-sm font-medium">Total Revenue</h3>
                <DollarSign className="h-4 w-4 text-cyan-500" />
              </div>
              <div className="text-2xl font-bold">${totalRevenue.toLocaleString()}</div>
              <p className="text-xs text-muted-foreground mt-1 flex items-center">
                <TrendingUp className="h-3 w-3 mr-1 text-green-500" />
                +20.1% from last month
              </p>
            </div>

            {/* Card 2 */}
            <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
              <div className="flex flex-row items-center justify-between space-y-0 pb-2">
                <h3 className="tracking-tight text-sm font-medium">Total Items Sold</h3>
                <ShoppingBag className="h-4 w-4 text-purple-500" />
              </div>
              <div className="text-2xl font-bold">+12,234</div>
              <p className="text-xs text-muted-foreground mt-1 flex items-center">
                <TrendingUp className="h-3 w-3 mr-1 text-green-500" />
                +19% from last month
              </p>
            </div>

            {/* Card 3 */}
            <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
              <div className="flex flex-row items-center justify-between space-y-0 pb-2">
                <h3 className="tracking-tight text-sm font-medium">Avg Fail Rate</h3>
                <Activity className="h-4 w-4 text-red-500" />
              </div>
              <div className="text-2xl font-bold">{avgFailRate}%</div>
              <p className="text-xs text-muted-foreground mt-1 flex items-center">
                <TrendingDown className="h-3 w-3 mr-1 text-red-500" />
                +4% difficulty spike
              </p>
            </div>

            {/* Card 4 - ĐÃ SỬA LỖI Ở ĐÂY */}
            <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
              <div className="flex flex-row items-center justify-between space-y-0 pb-2">
                <h3 className="tracking-tight text-sm font-medium">Active Players</h3>
                <Users className="h-4 w-4 text-blue-500" />
              </div>
              <div className="text-2xl font-bold">573</div>
              <p className="text-xs text-muted-foreground mt-1 flex items-center">
                <TrendingUp className="h-3 w-3 mr-1 text-green-500" />
                +201 since last hour
              </p>
            </div>
          </div>

          {/* Main Chart */}
          <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
             <div className="mb-4">
                <h3 className="text-lg font-semibold">Revenue vs Fail Rate by Level</h3>
             </div>
            <RevenueChart />
          </div>

          {/* Bottom Section */}
          <div className="grid gap-6 lg:grid-cols-7">
            
            {/* Top Boosters Table */}
            <div className="col-span-4 rounded-xl border bg-card text-card-foreground shadow h-full">
              <div className="p-6">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h3 className="text-lg font-semibold">Top Used Boosters</h3>
                    <p className="text-sm text-muted-foreground">Vật phẩm được mua nhiều nhất</p>
                  </div>
                </div>
                
                <div className="relative w-full overflow-auto">
                  <table className="w-full caption-bottom text-sm text-left">
                    <thead className="[&_tr]:border-b">
                      <tr className="border-b transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted">
                        <th className="h-12 px-4 align-middle font-medium text-muted-foreground">Rank</th>
                        <th className="h-12 px-4 align-middle font-medium text-muted-foreground">Booster</th>
                        <th className="h-12 px-4 align-middle font-medium text-muted-foreground text-right">Usage</th>
                        <th className="h-12 px-4 align-middle font-medium text-muted-foreground text-right">Trend</th>
                      </tr>
                    </thead>
                    <tbody className="[&_tr:last-child]:border-0">
                      {topBoosters.map((item) => (
                        <tr key={item.rank} className="border-b transition-colors hover:bg-muted/50">
                          <td className="p-4 align-middle font-bold">#{item.rank}</td>
                          <td className="p-4 align-middle font-medium text-primary">{item.name}</td>
                          <td className="p-4 align-middle text-right">{item.usage}</td>
                          <td className={`p-4 align-middle text-right font-medium ${item.trend.includes('+') ? 'text-green-500' : 'text-red-500'}`}>
                            {item.trend}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            {/* Drilldown */}
            <div className="col-span-3 rounded-xl border bg-card text-card-foreground shadow h-full">
               <DrilldownSection />
            </div>
            
          </div>
        </div>
      </main>
    </div>
  )
}