"use client"

import { useState, useMemo } from "react"
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts"
import { Filter } from "lucide-react"

// 1. Dữ liệu giả lập cho 20 Level
const generateDataForLevel = (level: number) => {
  const base = level * 2;
  return [
    { name: "Extra Life", value: 30 + (level % 2 === 0 ? 10 : -5), color: "#ec4899" }, // Pink
    { name: "Double XP", value: 20 + base, color: "#06b6d4" }, // Cyan
    { name: "Shield Boost", value: 15 + (level % 3 === 0 ? 15 : 0), color: "#8b5cf6" }, // Purple
    { name: "Time Freeze", value: 25 - (level > 10 ? 10 : 0), color: "#f59e0b" }, // Orange
    { name: "Score Multiplier", value: 10 + (level > 15 ? 20 : 0), color: "#10b981" }, // Green
  ]
}

export function DrilldownSection() {
  const [selectedLevel, setSelectedLevel] = useState<number>(1)
  const chartData = useMemo(() => generateDataForLevel(selectedLevel), [selectedLevel])
  const levels = Array.from({ length: 20 }, (_, i) => i + 1)

  return (
    <div className="flex flex-col h-full p-6">
      {/* Header & Filter */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h3 className="text-lg font-semibold text-foreground">Item Usage by Level</h3>
          <p className="text-sm text-muted-foreground">Tỷ lệ sử dụng vật phẩm tại Level {selectedLevel}</p>
        </div>
        
        {/* Dropdown chọn Level */}
        <div className="relative">
          <Filter className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground z-10" />
          <select 
            className="h-9 w-[130px] rounded-md border border-input bg-transparent pl-9 pr-3 text-sm shadow-sm focus:outline-none focus:ring-1 focus:ring-ring cursor-pointer"
            value={selectedLevel}
            onChange={(e) => setSelectedLevel(Number(e.target.value))}
            // Thêm style này để ép màu chữ khi chưa mở menu
            style={{ color: 'inherit' }} 
          >
            {levels.map((level) => (
              <option 
                key={level} 
                value={level} 
                // QUAN TRỌNG: Dùng style trực tiếp để ép trình duyệt hiển thị nền đen chữ trắng
                style={{ backgroundColor: '#09090b', color: 'white' }} 
              >
                Level {level}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Biểu đồ tròn */}
      <div className="flex-1 min-h-[300px]">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              innerRadius={60}
              outerRadius={80}
              paddingAngle={5}
              dataKey="value"
              stroke="none"
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Pie>
            <Tooltip 
              contentStyle={{ backgroundColor: '#09090b', borderColor: '#333', borderRadius: '8px', color: 'white' }}
              itemStyle={{ color: 'white' }}
            />
            <Legend verticalAlign="bottom" height={36} iconType="circle" />
          </PieChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}