"use client"

import { Bar, BarChart, Line, ComposedChart, ResponsiveContainer, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts"

// Định nghĩa kiểu dữ liệu cho biểu đồ
interface RevenueChartProps {
  data: any[] 
}

export function RevenueChart({ data }: RevenueChartProps) {
  // Nếu không có dữ liệu thì hiện thông báo
  if (!data || data.length === 0) {
    return <div className="h-[350px] w-full flex items-center justify-center text-muted-foreground">No data available</div>
  }

  return (
    <ResponsiveContainer width="100%" height={350}>
      <ComposedChart data={data}>
        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" opacity={0.4} />
        
        <XAxis
          dataKey="name"
          stroke="#888888"
          fontSize={12}
          tickLine={false}
          axisLine={false}
          tick={{ fill: 'hsl(var(--muted-foreground))' }}
        />
        
        <YAxis
          yAxisId="left"
          stroke="#888888"
          fontSize={12}
          tickLine={false}
          axisLine={false}
          tickFormatter={(value) => `$${value}`}
          tick={{ fill: 'hsl(var(--muted-foreground))' }}
        />
        
        <YAxis
          yAxisId="right"
          orientation="right"
          stroke="#888888"
          fontSize={12}
          tickLine={false}
          axisLine={false}
          tickFormatter={(value) => `${value}%`}
          tick={{ fill: 'hsl(var(--muted-foreground))' }}
        />
        
        <Tooltip
          contentStyle={{ backgroundColor: 'hsl(var(--card))', borderColor: 'hsl(var(--border))', borderRadius: '8px' }}
          itemStyle={{ color: 'hsl(var(--foreground))' }}
        />

        {/* Cột Revenue (Màu Xanh) */}
        <Bar 
          yAxisId="left" 
          dataKey="total" 
          fill="#0ea5e9" // Màu xanh dương sáng
          radius={[4, 4, 0, 0]} 
          name="Revenue" 
          barSize={40}
        />

        {/* Đường Fail Rate (Màu Hồng) */}
        <Line 
          yAxisId="right" 
          type="monotone" 
          dataKey="failRate" 
          stroke="#ec4899" // Màu hồng
          strokeWidth={2} 
          dot={{ r: 4, fill: "#ec4899" }} 
          name="Fail Rate" 
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}