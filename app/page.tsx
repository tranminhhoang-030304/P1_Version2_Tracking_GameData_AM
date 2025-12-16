"use client"

import { Sidebar } from "@/components/sidebar"
import { RevenueChart } from "@/components/revenue-chart"
import { DrilldownSection } from "@/components/drilldown-section"
import { DollarSign, ShoppingBag, Activity, TrendingUp, TrendingDown, Users } from "lucide-react"

// Mock Data (Dữ liệu này sẽ được truyền vào biểu đồ)
const mockChartData = [
  { name: "Level 1", total: 4000, failRate: 32 },
  { name: "Level 2", total: 4100, failRate: 30 },
  { name: "Level 3", total: 5200, failRate: 31 },
  { name: "Level 4", total: 4200, failRate: 28 },
  { name: "Level 5", total: 5100, failRate: 27 },
  { name: "Level 6", total: 5400, failRate: 27 },
  { name: "Level 7", total: 4800, failRate: 41 },
  { name: "Level 8", total: 4300, failRate: 33 },
  { name: "Level 9", total: 6300, failRate: 19 },
  { name: "Level 10", total: 3600, failRate: 32 },
]

const topBoosters = [
  { rank: 1, name: "Double XP", usage: "45,231", trend: "+12%" },
  { rank: 2, name: "Extra Life", usage: "32,105", trend: "+5%" },
  { rank: 3, name: "Time Freeze", usage: "28,450", trend: "-2%" },
  { rank: 4, name: "Shield Boost", usage: "15,300", trend: "+8%" },
]

export default function DashboardPage() {
  const totalRevenue = mockChartData.reduce((acc, item) => acc + item.total, 0)
  const avgFailRate = (mockChartData.reduce((acc, item) => acc + item.failRate, 0) / mockChartData.length).toFixed(1)

  return (
    <div className="min-h-screen bg-background text-foreground">
      <Sidebar />
      <main className="pl-64 transition-all duration-300">
        <div className="p-8 space-y-8">
          
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold tracking-tight text-primary neon-text-cyan">
                Dashboard
              </h1>
            </div>
          </div>

          {/* Cards Stats */}
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
              <div className="flex flex-row items-center justify-between space-y-0 pb-2">
                <h3 className="tracking-tight text-sm font-medium">Total Revenue</h3>
                <DollarSign className="h-4 w-4 text-cyan-500" />
              </div>
              <div className="text-2xl font-bold">${totalRevenue.toLocaleString()}</div>
              <p className="text-xs text-muted-foreground mt-1 flex items-center">
                <TrendingUp className="h-3 w-3 mr-1 text-green-500" /> +20.1%
              </p>
            </div>
            <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
              <div className="flex flex-row items-center justify-between space-y-0 pb-2">
                <h3 className="tracking-tight text-sm font-medium">Total Items Sold</h3>
                <ShoppingBag className="h-4 w-4 text-purple-500" />
              </div>
              <div className="text-2xl font-bold">+12,234</div>
              <p className="text-xs text-muted-foreground mt-1 flex items-center">
                <TrendingUp className="h-3 w-3 mr-1 text-green-500" /> +19%
              </p>
            </div>
            <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
              <div className="flex flex-row items-center justify-between space-y-0 pb-2">
                <h3 className="tracking-tight text-sm font-medium">Avg Fail Rate</h3>
                <Activity className="h-4 w-4 text-red-500" />
              </div>
              <div className="text-2xl font-bold">{avgFailRate}%</div>
              <p className="text-xs text-muted-foreground mt-1 flex items-center">
                <TrendingDown className="h-3 w-3 mr-1 text-red-500" /> +4%
              </p>
            </div>
            <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
              <div className="flex flex-row items-center justify-between space-y-0 pb-2">
                <h3 className="tracking-tight text-sm font-medium">Active Players</h3>
                <Users className="h-4 w-4 text-blue-500" />
              </div>
              <div className="text-2xl font-bold">573</div>
              <p className="text-xs text-muted-foreground mt-1 flex items-center">
                <TrendingUp className="h-3 w-3 mr-1 text-green-500" /> +201
              </p>
            </div>
          </div>

          {/* Charts Section */}
          <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
             <div className="mb-4">
                <h3 className="text-lg font-semibold">Revenue vs Fail Rate by Level</h3>
             </div>
             {/* QUAN TRỌNG: Đã truyền data vào đây */}
            <RevenueChart data={mockChartData} />
          </div>

          {/* Bottom Section */}
          <div className="grid gap-6 lg:grid-cols-7">
            <div className="col-span-4 rounded-xl border bg-card text-card-foreground shadow h-full">
              <div className="p-6">
                <h3 className="text-lg font-semibold mb-4">Top Used Boosters</h3>
                <div className="relative w-full overflow-auto">
                  <table className="w-full caption-bottom text-sm text-left">
                    <thead className="[&_tr]:border-b">
                      <tr className="border-b">
                        <th className="h-12 px-4">Rank</th>
                        <th className="h-12 px-4">Booster</th>
                        <th className="h-12 px-4 text-right">Usage</th>
                        <th className="h-12 px-4 text-right">Trend</th>
                      </tr>
                    </thead>
                    <tbody>
                      {topBoosters.map((item) => (
                        <tr key={item.rank} className="border-b hover:bg-muted/50">
                          <td className="p-4 font-bold">#{item.rank}</td>
                          <td className="p-4 text-primary">{item.name}</td>
                          <td className="p-4 text-right">{item.usage}</td>
                          <td className={`p-4 text-right font-medium ${item.trend.includes('+') ? 'text-green-500' : 'text-red-500'}`}>
                            {item.trend}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
            <div className="col-span-3 rounded-xl border bg-card text-card-foreground shadow h-full">
               <DrilldownSection />
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}