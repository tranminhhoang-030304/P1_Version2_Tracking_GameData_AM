"use client"

import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts"

interface RevenueChartProps {
  data: any[]
}

export function RevenueChart({ data }: RevenueChartProps) {
  return (
    <ResponsiveContainer width="100%" height={350}>
      <ComposedChart data={data}>
        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#333" />
        
        {/* Trục X: Level */}
        <XAxis 
          dataKey="name" 
          stroke="#888888" 
          fontSize={12} 
          tickLine={false} 
          axisLine={false}
          interval={0} // Cố gắng hiện hết các mốc level
          angle={-45}  // Nghiêng chữ nếu nhiều level quá
          textAnchor="end"
          height={60}
        />
        
        {/* Trục Y Trái: DOANH THU (Tiền) */}
        <YAxis
          yAxisId="left"
          stroke="#888888"
          fontSize={12}
          tickLine={false}
          axisLine={false}
          tickFormatter={(value) => `${value}`} // Có thể thêm 'k' nếu số quá to
        />

        {/* Trục Y Phải: FAIL RATE (%) */}
        <YAxis
          yAxisId="right"
          orientation="right"
          stroke="#ff4d4f"
          fontSize={12}
          tickLine={false}
          axisLine={false}
          unit="%"
        />

        <Tooltip
          contentStyle={{ backgroundColor: "#1f2937", border: "none", borderRadius: "8px" }}
          itemStyle={{ color: "#fff" }}
        />
        <Legend />

        {/* Cột Doanh Thu (Màu Xanh Cyan) - Gắn trục Trái */}
        <Bar 
          yAxisId="left"
          dataKey="total" 
          name="Revenue (Coin)" 
          fill="#0ea5e9" 
          radius={[4, 4, 0, 0]} 
          barSize={20}
        />

        {/* Đường Fail Rate (Màu Đỏ/Hồng) - Gắn trục Phải */}
        <Line
          yAxisId="right"
          type="monotone"
          dataKey="failRate"
          name="Fail Rate (%)"
          stroke="#f43f5e"
          strokeWidth={2}
          dot={false} // Bỏ chấm tròn cho đỡ rối mắt
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}