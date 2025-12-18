"use client"

import { useState, useEffect } from "react"
import { Sidebar } from "@/components/sidebar"
import { Play, Activity, Clock, AlertTriangle, CheckCircle2 } from "lucide-react"
// 👇 1. IMPORT COMPONENT MỚI
import RawDataMonitor from "@/components/RawDataMonitor"

export default function MonitorPage() {
  const [logs, setLogs] = useState<any[]>([])
  const [isRunning, setIsRunning] = useState(false)
  const [isLoaded, setIsLoaded] = useState(false)

  // 1. KHI MỞ TRANG: Load lịch sử cũ
  useEffect(() => {
    const savedLogs = localStorage.getItem("etlLogs")
    if (savedLogs) {
      setLogs(JSON.parse(savedLogs))
    }
    setIsLoaded(true)
  }, [])

  // 2. KHI LOGS THAY ĐỔI: Lưu lại
  useEffect(() => {
    if (isLoaded) {
       localStorage.setItem("etlLogs", JSON.stringify(logs))
    }
  }, [logs, isLoaded])

  // Hàm chạy giả lập ETL
  const handleRunNow = () => {
    setIsRunning(true)
    
    // Giả vờ chạy mất 2 giây
    setTimeout(() => {
      const isSuccess = Math.random() > 0.3 // 70% thành công
      const rows = isSuccess ? Math.floor(Math.random() * 40000) + 10000 : 0
      const duration = isSuccess 
        ? `${Math.floor(Math.random() * 3) + 1}m ${Math.floor(Math.random() * 60)}s`
        : `${Math.floor(Math.random() * 10)}s`

      const newLog = {
        id: `ETL-${Date.now().toString().slice(-6)}`,
        status: isSuccess ? "success" : "failed",
        rows: rows,
        time: new Date().toLocaleString('vi-VN'),
        duration: duration,
      }

      setLogs([newLog, ...logs]) // Thêm log mới vào đầu danh sách
      setIsRunning(false)
    }, 2000)
  }

  // Hàm xóa lịch sử (để bạn test cho dễ)
  const clearLogs = () => {
      if(confirm("Xóa toàn bộ lịch sử chạy?")) {
          setLogs([])
      }
  }

  if (!isLoaded) return <div className="min-h-screen bg-background flex"><Sidebar /><div className="p-8">Loading...</div></div>

  return (
    <div className="min-h-screen bg-background text-foreground flex">
      <Sidebar />
      <main className="flex-1 pl-64 transition-all duration-300">
        <div className="p-8 space-y-8">
          
          <div className="flex items-center justify-between">
             <div>
                <h1 className="text-3xl font-bold tracking-tight text-primary neon-text-cyan">Monitor</h1>
                <p className="text-muted-foreground mt-1">Theo dõi trạng thái hệ thống & ETL Jobs</p>
             </div>
             <div className="flex gap-2">
                 {logs.length > 0 && (
                     <button onClick={clearLogs} className="inline-flex items-center justify-center rounded-md text-sm font-medium border border-input bg-background hover:bg-accent hover:text-accent-foreground h-10 px-4 py-2 text-red-500">
                        Xóa lịch sử
                     </button>
                 )}
                 <button 
                    onClick={handleRunNow} 
                    disabled={isRunning}
                    className="inline-flex items-center justify-center rounded-md text-sm font-medium bg-primary text-primary-foreground hover:bg-primary/90 h-10 px-4 py-2"
                 >
                    {isRunning ? <Clock className="mr-2 h-4 w-4 animate-spin"/> : <Play className="mr-2 h-4 w-4" />} 
                    {isRunning ? "Running..." : "Run Now"}
                 </button>
             </div>
          </div>

          <div className="grid gap-4 md:grid-cols-3">
             <div className="rounded-xl border bg-card p-6 flex flex-col items-center text-center">
                 <Activity className="h-8 w-8 text-green-500 mb-2" />
                 <h3 className="font-semibold">System Status</h3>
                 <span className="text-sm text-muted-foreground mt-1">All Services Operational</span>
             </div>
             <div className="rounded-xl border bg-card p-6 flex flex-col items-center text-center">
                 <Clock className="h-8 w-8 text-blue-500 mb-2" />
                 <h3 className="font-semibold">Last Sync</h3>
                 <span className="text-sm text-muted-foreground mt-1">{logs.length > 0 ? logs[0].time : "Never"}</span>
             </div>
             <div className="rounded-xl border bg-card p-6 flex flex-col items-center text-center">
                 <AlertTriangle className="h-8 w-8 text-yellow-500 mb-2" />
                 <h3 className="font-semibold">Pending Issues</h3>
                 <span className="text-sm text-muted-foreground mt-1">0 Critical Errors</span>
             </div>
          </div>

          {/* BẢNG LOG CŨ (LOCAL) */}
          <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
             <h3 className="text-lg font-semibold mb-4">ETL Job Logs (Lưu trên máy)</h3>
             <div className="relative w-full overflow-auto">
                 <table className="w-full caption-bottom text-sm text-left">
                     <thead className="bg-muted/50 [&_tr]:border-b">
                         <tr className="border-b">
                             <th className="h-10 px-4">Job ID</th>
                             <th className="h-10 px-4">Status</th>
                             <th className="h-10 px-4 text-right">Rows Imported</th>
                             <th className="h-10 px-4 text-right">Time</th>
                             <th className="h-10 px-4 text-right">Duration</th>
                         </tr>
                     </thead>
                     <tbody>
                         {logs.map((log) => (
                             <tr key={log.id} className="border-b transition-colors hover:bg-muted/50">
                                 <td className="p-4 font-mono text-xs">{log.id}</td>
                                 <td className="p-4">
                                     <span className={`inline-flex items-center rounded-md px-2 py-1 text-xs font-medium ring-1 ring-inset ${log.status === 'success' ? 'bg-green-500/10 text-green-400 ring-green-500/20' : 'bg-red-500/10 text-red-400 ring-red-500/20'}`}>
                                         {log.status === 'success' ? <CheckCircle2 className="w-3 h-3 mr-1"/> : <AlertTriangle className="w-3 h-3 mr-1"/>}
                                         {log.status}
                                     </span>
                                 </td>
                                 <td className="p-4 text-right">{log.rows.toLocaleString()}</td>
                                 <td className="p-4 text-right text-muted-foreground">{log.time}</td>
                                 <td className="p-4 text-right font-mono">{log.duration}</td>
                             </tr>
                         ))}
                         {logs.length === 0 && <tr><td colSpan={5} className="p-8 text-center text-muted-foreground">Chưa có log chạy nào. Bấm Run Now để thử!</td></tr>}
                     </tbody>
                 </table>
             </div>
          </div>

          {/* 👇 2. BẢNG MONITOR MỚI VÀO ĐÂY (TÁCH BIỆT KHỎI BẢNG CŨ) */}
          <div className="mt-8 pt-8 border-t border-dashed border-gray-700">
             <RawDataMonitor />
          </div>

        </div>
      </main>
    </div>
  )
}