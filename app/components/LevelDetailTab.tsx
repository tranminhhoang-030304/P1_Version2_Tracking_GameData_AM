'use client';

import React, { useState, useEffect } from 'react';
import { 
  Activity, Play, AlertCircle, ShoppingCart, 
  Filter, ChevronDown, Trophy, Skull, Coins, Clock, Calendar, AlertTriangle
} from 'lucide-react';

interface LevelDetailProps {
  appId: number;
}

export default function LevelDetailTab({ appId }: LevelDetailProps) {
  const [selectedLevel, setSelectedLevel] = useState<string>("1");
  // State quản lý khoảng thời gian (Mặc định rỗng = lấy tất cả)
  const [dateRange, setDateRange] = useState({ start: '', end: '' });
  
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  
  // List level cứng
  const levels = ["1", "2", "3", "4", "5", "10", "15", "17", "20", "25", "36", "52"];

  // Hàm gọi API (Đã nâng cấp để truyền thêm start/end date)
  const fetchLevelDetail = async (lvl: string, start: string, end: string) => {
    setLoading(true);
    try {
      // Xây dựng URL động
      let url = `http://127.0.0.1:8080/dashboard/${appId}/level-detail?level_id=${lvl}`;
      if (start) url += `&start_date=${start}`;
      if (end) url += `&end_date=${end}`;

      const res = await fetch(url);
      const json = await res.json();
      if (json.success) setData(json);
    } catch (error) {
      console.error("Lỗi tải level:", error);
    } finally {
      setLoading(false);
    }
  };

  // Tự động gọi khi đổi Level hoặc đổi Ngày
  useEffect(() => {
    fetchLevelDetail(selectedLevel, dateRange.start, dateRange.end);
  }, [selectedLevel, dateRange, appId]);

  // Logic tính màu cho Win Rate (Cảnh báo nếu < 20%)
  const getWinRateStatus = (rateStr: string) => {
    const rate = parseFloat(rateStr);
    if (isNaN(rate)) return "emerald"; // Mặc định xanh
    if (rate < 20) return "red";       // BÁO ĐỘNG ĐỎ
    if (rate < 40) return "yellow";    // Cảnh báo vàng
    return "emerald";                  // An toàn
  }

  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
      {/* 1. FILTER BAR (ĐÃ THÊM DATE PICKER) */}
      <div className="bg-white dark:bg-slate-800 p-4 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-col md:flex-row md:items-center justify-between gap-4">
        
        <div className="flex flex-wrap items-center gap-4">
          {/* Chọn Level */}
          <div className="flex items-center gap-2 bg-blue-50 text-blue-700 px-3 py-1.5 rounded-lg border border-blue-100">
             <Filter size={18}/>
             <span className="font-bold text-sm">Level</span>
          </div>
          <div className="relative">
             <select 
               value={selectedLevel}
               onChange={(e) => setSelectedLevel(e.target.value)}
               className="appearance-none bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-200 py-2 pl-4 pr-10 rounded-lg font-bold focus:ring-2 focus:ring-blue-500 outline-none cursor-pointer"
             >
               {levels.map(lvl => <option key={lvl} value={lvl}>Level {lvl}</option>)}
             </select>
             <ChevronDown size={16} className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none"/>
          </div>

          {/* Dải phân cách */}
          <div className="h-6 w-px bg-slate-200 hidden md:block"></div>

          {/* Chọn Ngày (MỚI) */}
          <div className="flex items-center gap-2">
             <div className="flex items-center gap-2 bg-slate-100 text-slate-600 px-3 py-1.5 rounded-lg border border-slate-200">
                <Calendar size={18}/>
                <span className="font-bold text-sm">Time Range</span>
             </div>
             <input 
                type="date" 
                className="bg-white dark:bg-slate-900 border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                value={dateRange.start}
                onChange={(e) => setDateRange({...dateRange, start: e.target.value})}
             />
             <span className="text-slate-400 font-bold">-</span>
             <input 
                type="date" 
                className="bg-white dark:bg-slate-900 border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                value={dateRange.end}
                onChange={(e) => setDateRange({...dateRange, end: e.target.value})}
             />
             {/* Nút Xóa lọc ngày */}
             {(dateRange.start || dateRange.end) && (
                <button 
                  onClick={() => setDateRange({start: '', end: ''})}
                  className="text-red-500 hover:bg-red-50 p-2 rounded-full transition-colors"
                  title="Clear Date Filter"
                >
                  ✕
                </button>
             )}
          </div>
        </div>
        
        {loading && <span className="text-sm text-blue-500 font-medium animate-pulse flex items-center gap-2"><div className="w-2 h-2 bg-blue-500 rounded-full animate-bounce"/> Loading Data...</span>}
      </div>

      {data ? (
        <>
          {/* 2. METRICS CARDS (C1) - ĐÃ CÓ CẢNH BÁO MÀU */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <Card title="Total Plays" value={data.metrics.total_plays} icon={Play} color="blue" />
            
            {/* THẺ WIN RATE THÔNG MINH */}
            <Card 
              title={getWinRateStatus(data.metrics.win_rate) === 'red' ? "CRITICAL WIN RATE" : "Win Rate"} 
              value={`${data.metrics.win_rate}%`} 
              icon={getWinRateStatus(data.metrics.win_rate) === 'red' ? AlertTriangle : Trophy} 
              color={getWinRateStatus(data.metrics.win_rate)} // Truyền màu động (red/yellow/emerald)
            />
            
            <Card title="ARPU (Avg Revenue)" value={data.metrics.arpu} icon={Coins} color="yellow" />
            <Card title="Top Package" value={data.metrics.top_item} icon={ShoppingCart} color="purple" />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* 3. FUNNEL CHART (C2) */}
            <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm lg:col-span-1">
               <h3 className="font-bold text-slate-700 dark:text-slate-200 mb-6 flex items-center gap-2">
                 <Skull size={20} className="text-red-500"/> Death Funnel
               </h3>
               <div className="space-y-4">
                 {data.funnel.map((step: any, idx: number) => {
                    const maxVal = Math.max(...data.funnel.map((f:any) => f.count));
                    const percent = Math.round((step.count / maxVal) * 100);
                    return (
                      <div key={idx} className="relative">
                        <div className="flex justify-between text-xs font-bold mb-1 uppercase tracking-wide text-slate-500">
                          <span>{step.event_type}</span>
                          <span>{step.count}</span>
                        </div>
                        <div className="w-full bg-slate-100 dark:bg-slate-700 rounded-full h-3 overflow-hidden">
                           <div 
                             className={`h-full rounded-full ${getBarColor(step.event_type)}`} 
                             style={{ width: `${percent}%`, transition: 'width 1s ease-out' }}
                           />
                        </div>
                        {step.revenue > 0 && <div className="text-[10px] text-right text-emerald-600 font-bold mt-0.5">+{step.revenue} Coin</div>}
                      </div>
                    )
                 })}
               </div>
            </div>

            {/* 4. REALTIME LOGS (D) */}
            <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm lg:col-span-2 flex flex-col">
               <h3 className="font-bold text-slate-700 dark:text-slate-200 mb-4 flex items-center gap-2">
                 <Clock size={20} className="text-blue-500"/> Realtime Logs (100 latest)
               </h3>
               <div className="flex-1 overflow-auto max-h-[400px] custom-scrollbar border rounded-lg">
                 <table className="w-full text-sm text-left relative border-collapse">
                   <thead className="bg-slate-50 dark:bg-slate-900 text-slate-500 sticky top-0 z-10">
                     <tr>
                       <th className="p-3 font-medium">Time</th>
                       <th className="p-3 font-medium">User ID</th>
                       <th className="p-3 font-medium">Event</th>
                       <th className="p-3 font-medium text-right">Detail</th>
                     </tr>
                   </thead>
                   <tbody className="divide-y divide-slate-100 dark:divide-slate-700">
                     {data.logs.map((log: any, i: number) => (
                       <tr key={i} className="hover:bg-slate-50 dark:hover:bg-slate-700/50">
                         <td className="p-3 font-mono text-xs text-slate-400">{log.time}</td>
                         <td className="p-3 text-blue-600 font-mono text-xs truncate max-w-[100px]" title={log.user_id}>{log.user_id.substring(0,8)}...</td>
                         <td className="p-3"><Badge type={log.event_name} /></td>
                         <td className="p-3 text-right">
                            {log.coin_spent > 0 
                               ? <span className="text-emerald-600 font-bold text-xs">-{log.coin_spent} ({log.item_name})</span>
                               : <span className="text-slate-300">-</span>
                            }
                         </td>
                       </tr>
                     ))}
                   </tbody>
                 </table>
               </div>
            </div>
          </div>
        </>
      ) : (
        <div className="h-64 flex items-center justify-center text-slate-400">Select a level to view details</div>
      )}
    </div>
  );
}

// --- SUB COMPONENTS ---
const Card = ({ title, value, icon: Icon, color }: any) => {
  const colorMap: any = {
    blue: "bg-blue-50 text-blue-600",
    emerald: "bg-emerald-50 text-emerald-600",
    yellow: "bg-yellow-50 text-yellow-600",
    purple: "bg-purple-50 text-purple-600",
    // MÀU MỚI: ĐỎ BÁO ĐỘNG
    red: "bg-red-50 text-red-600 border border-red-200 animate-pulse", 
  };
  return (
    <div className={`p-5 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm transition-all ${color === 'red' ? 'bg-red-50' : 'bg-white dark:bg-slate-800'}`}>
      <div className="flex justify-between items-start">
         <div>
           <p className={`text-xs font-bold uppercase tracking-wider ${color === 'red' ? 'text-red-500' : 'text-slate-400'}`}>{title}</p>
           <h4 className={`text-2xl font-bold mt-1 ${color === 'red' ? 'text-red-700' : 'text-slate-800 dark:text-white'}`}>{value}</h4>
         </div>
         <div className={`p-2 rounded-lg ${colorMap[color] || colorMap.blue}`}><Icon size={20}/></div>
      </div>
    </div>
  )
}

const Badge = ({ type }: { type: string }) => {
   let color = "bg-slate-100 text-slate-600";
   if(type.includes("Start")) color = "bg-blue-100 text-blue-700";
   if(type.includes("Complete")) color = "bg-emerald-100 text-emerald-700";
   if(type.includes("Fail")) color = "bg-red-100 text-red-700";
   if(type.includes("Spend")) color = "bg-orange-100 text-orange-700";
   return <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${color}`}>{type}</span>
}

const getBarColor = (type: string) => {
   if(type === 'WIN') return 'bg-emerald-500';
   if(type === 'FAIL') return 'bg-red-500';
   if(type === 'SPEND') return 'bg-orange-500';
   if(type === 'START') return 'bg-blue-500';
   return 'bg-slate-400';
}