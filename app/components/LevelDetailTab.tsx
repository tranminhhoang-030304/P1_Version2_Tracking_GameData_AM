'use client';

import React, { useState, useEffect } from 'react';
import {
  Activity, Play, ShoppingCart,
  Filter, ChevronDown, Trophy, Skull, Coins, Clock, Calendar, AlertTriangle,
  ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Zap, PieChart as PieIcon
} from 'lucide-react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, PieChart, Pie, Legend
} from 'recharts';

interface LevelDetailProps { appId: number; }

export default function LevelDetailTab({ appId }: LevelDetailProps) {
  const [levels, setLevels] = useState<string[]>([]);
  const [selectedLevel, setSelectedLevel] = useState<string>("");
  const [dateRange, setDateRange] = useState({ start: '', end: '' });

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [logPage, setLogPage] = useState(1);

  // 1. Fetch List Level
  useEffect(() => {
    fetch(`http://127.0.0.1:8080/api/levels/${appId}`)
      .then(res => res.json())
      .then(arr => {
        if (Array.isArray(arr) && arr.length > 0) {
          setLevels(arr);
          if (!selectedLevel) setSelectedLevel(arr[0]);
        }
      })
      .catch(() => { });
  }, [appId]);

  // 2. Fetch Detail Data
  useEffect(() => {
    if (!selectedLevel) return;
    setLoading(true);
    let url = `http://127.0.0.1:8080/dashboard/${appId}/level-detail?level_id=${selectedLevel}&page=${logPage}&limit=50`;
    if (dateRange.start) url += `&start_date=${dateRange.start}`;
    if (dateRange.end) url += `&end_date=${dateRange.end}`;

    fetch(url)
      .then(res => res.json())
      .then(json => { if (json.success) setData(json); })
      .finally(() => setLoading(false));
  }, [selectedLevel, dateRange, logPage, appId]);

  useEffect(() => setLogPage(1), [selectedLevel, dateRange]);

  const getWinRateStatus = (rate: any) => {
    const r = parseFloat(rate);
    if (isNaN(r)) return "emerald";
    if (r < 20) return "red";
    if (r < 40) return "yellow";
    return "emerald";
  }

  const getBarColor = (type: string) => {
    if (type === 'WIN') return 'bg-emerald-500';
    if (type === 'FAIL') return 'bg-red-500';
    if (type === 'SPEND') return 'bg-orange-500';
    if (type === 'START') return 'bg-blue-500';
    return 'bg-slate-400';
  }

  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
      {/* FILTER BAR */}
      <div className="bg-white dark:bg-slate-800 p-4 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="flex flex-wrap items-center gap-4">
          <div className="relative group">
            <div className="absolute left-3 top-1/2 -translate-y-1/2 text-blue-600 pointer-events-none"><Filter size={18} /></div>
            <select
              value={selectedLevel}
              onChange={(e) => setSelectedLevel(e.target.value)}
              className="appearance-none bg-blue-50 border border-blue-100 text-blue-700 py-2 pl-10 pr-10 rounded-lg font-bold focus:ring-2 focus:ring-blue-500 outline-none cursor-pointer min-w-[180px] hover:bg-blue-100 transition-colors"
            >
              {levels.length === 0 && <option value="" disabled>Loading...</option>}
              {levels.map(lvl => (
                <option key={lvl} value={lvl}>
                  {lvl === '0' ? '🏠 Lobby / Tutorial' : `Level ${lvl}`}
                </option>
              ))}
            </select>
            <ChevronDown size={16} className="absolute right-3 top-1/2 -translate-y-1/2 text-blue-400 pointer-events-none" />
          </div>
          <div className="h-6 w-px bg-slate-200 hidden md:block"></div>
          <div className="flex items-center gap-2">
            <div className="flex items-center gap-2 bg-slate-100 text-slate-600 px-3 py-1.5 rounded-lg border border-slate-200">
              <Calendar size={18} /> <span className="font-bold text-sm">Time Range</span>
            </div>
            <input type="date" className="bg-white border rounded-lg px-3 py-2 text-sm outline-none"
              value={dateRange.start} onChange={(e) => setDateRange({ ...dateRange, start: e.target.value })} />
            <span className="text-slate-400 font-bold">-</span>
            <input type="date" className="bg-white border rounded-lg px-3 py-2 text-sm outline-none"
              value={dateRange.end} onChange={(e) => setDateRange({ ...dateRange, end: e.target.value })} />
            {(dateRange.start || dateRange.end) && (
              <button onClick={() => setDateRange({ start: '', end: '' })} className="text-red-500 hover:bg-red-50 p-2 rounded-full transition-colors">✕</button>
            )}
          </div>
        </div>
        {loading && <span className="text-sm text-blue-500 font-medium animate-pulse flex items-center gap-2"><div className="w-2 h-2 bg-blue-500 rounded-full animate-bounce" /> Loading Data...</span>}
      </div>

      {data ? (
        <>
          {/* 1. METRICS CARDS */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <Card title="Total Plays" value={data.metrics.total_plays} icon={Play} color="blue" />
            <Card
              title={getWinRateStatus(data.metrics.win_rate) === 'red' ? "CRITICAL WIN RATE" : "Win Rate"}
              value={`${data.metrics.win_rate}%`}
              icon={getWinRateStatus(data.metrics.win_rate) === 'red' ? AlertTriangle : Trophy}
              color={getWinRateStatus(data.metrics.win_rate)}
            />
            <Card title="ARPU (Level Revenue)" value={data.metrics.arpu} icon={Coins} color="yellow" />
            <Card title="Top Package" value={data.metrics.top_item} icon={ShoppingCart} color="purple" />
          </div>

          {/* 2. MAIN CHARTS ROW (3 COLUMNS: Funnel | Pie | Bar) */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

            {/* [LEFT] DEATH FUNNEL */}
            <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 shadow-sm">
              <h3 className="font-bold text-slate-700 mb-6 flex items-center gap-2"><Skull size={20} className="text-red-500" /> Level Funnel</h3>
              <div className="space-y-4">
                {data.funnel.map((step: any, idx: number) => {
                  const maxVal = Math.max(...data.funnel.map((f: any) => f.count), 1);
                  const percent = Math.round((step.count / maxVal) * 100);
                  return (
                    <div key={idx} className="relative group">
                      <div className="flex justify-between text-xs font-bold mb-1 uppercase text-slate-500">
                        <span>{step.event_type}</span><span>{step.count.toLocaleString()}</span>
                      </div>
                      <div className="w-full bg-slate-100 rounded-full h-3 overflow-hidden">
                        <div className={`h-full rounded-full ${getBarColor(step.event_type)}`} style={{ width: `${percent}%` }} />
                      </div>
                      {step.revenue > 0 && <div className="text-[10px] text-right text-emerald-600 font-bold mt-0.5">+{step.revenue} Coin</div>}
                    </div>
                  )
                })}
              </div>
            </div>

            {/* [CENTER] COST EFFICIENCY PIE CHART */}
            <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 shadow-sm">
              <h4 className="font-bold text-slate-700 mb-4 flex items-center gap-2"><PieIcon className="text-emerald-500" size={18} /> Win/Fail Cost</h4>
              <div className="h-[250px] flex items-center justify-center">
                {data.cost_distribution.length > 0 ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie
                        data={data.cost_distribution}
                        dataKey="value"
                        nameKey="name"
                        cx="50%" cy="50%"
                        innerRadius={60} outerRadius={80}
                        paddingAngle={5}
                      >
                        {data.cost_distribution.map((entry: any, index: number) => (
                          <Cell key={`cell-${index}`} fill={entry.name === 'Cost to Win' ? '#10b981' : '#ef4444'} />
                        ))}
                      </Pie>
                      <Tooltip contentStyle={{ borderRadius: '8px' }} formatter={(val: number) => `${val.toLocaleString()} Coins`} />
                      <Legend verticalAlign="bottom" height={36} />
                    </PieChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="text-slate-400 italic text-center text-sm">No spending data<br />recorded for this level</div>
                )}
              </div>
            </div>

            {/* [RIGHT] BOOSTER USAGE BAR CHART (WITH REVENUE TOOLTIP) */}
            <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 shadow-sm">
              <h4 className="font-bold text-slate-700 mb-4 flex items-center gap-2">
                <Zap className="text-amber-500" size={18} /> Top Boosters
              </h4>
              <div className="h-[250px]">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={data.booster_usage} layout="vertical" margin={{ left: 10, right: 10 }}>
                    <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" hide />
                    <YAxis dataKey="item_name" type="category" width={80} tick={{ fontSize: 10 }} interval={0} />

                    {/* --- CUSTOM TOOLTIP: HIỂN THỊ CẢ SỐ LƯỢNG VÀ TIỀN --- */}
                    <Tooltip
                      cursor={{ fill: 'transparent' }}
                      content={({ active, payload }) => {
                        if (active && payload && payload.length) {
                          const item = payload[0].payload; // Lấy full dữ liệu (gồm cả revenue)
                          return (
                            <div className="bg-white p-3 border border-slate-200 shadow-xl rounded-xl text-xs z-50">
                              <p className="font-bold text-slate-700 mb-2 border-b pb-1 border-slate-100">
                                {item.item_name}
                              </p>
                              <div className="flex flex-col gap-1">
                                <div className="flex justify-between gap-6">
                                  <span className="text-slate-500">Used:</span>
                                  <span className="font-mono font-bold text-slate-700">{item.usage_count}</span>
                                </div>
                                <div className="flex justify-between gap-6">
                                  <span className="text-slate-500">Revenue:</span>
                                  <span className="font-mono font-bold text-emerald-600">
                                    +{item.revenue?.toLocaleString() || 0} 💰
                                  </span>
                                </div>
                                <div className="text-[10px] text-slate-400 mt-1 pt-1 border-t border-slate-50 italic">
                                  Price: {item.price} coin/unit
                                </div>
                              </div>
                            </div>
                          );
                        }
                        return null;
                      }}
                    />
                    {/* ----------------------------------------------------- */}

                    <Bar dataKey="usage_count" fill="#f59e0b" radius={[0, 4, 4, 0]} barSize={16} name="Used" />
                  </BarChart>
                </ResponsiveContainer>
                {data.booster_usage.length === 0 && (
                  <div className="text-center text-slate-400 text-xs mt-10">No items used</div>
                )}
              </div>
            </div>

          </div>

          {/* 3. LOGS ROW */}
          <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 shadow-sm mt-6 flex flex-col h-[500px]">
            <h3 className="font-bold text-slate-700 mb-4 flex items-center justify-between">
              <div className="flex items-center gap-2"><Clock size={20} className="text-blue-500" /> Realtime Logs</div>
              {data.logs.pagination && <span className="text-xs font-normal text-slate-400">Total: {data.logs.pagination.total_records.toLocaleString()}</span>}
            </h3>
            <div className="flex-1 overflow-auto custom-scrollbar border rounded-lg mb-4">
              <table className="w-full text-sm text-left">
                <thead className="bg-slate-50 text-slate-500 sticky top-0">
                  <tr><th className="p-3">Time</th><th className="p-3">User</th><th className="p-3">Event</th><th className="p-3 text-right">Detail</th></tr>
                </thead>
                <tbody className="divide-y">
                  {data.logs.data.map((log: any, i: number) => (
                    <tr key={i} className="hover:bg-slate-50">
                      <td className="p-3 font-mono text-xs text-slate-400">{log.time}</td>
                      <td className="p-3 text-blue-600 font-mono text-xs truncate max-w-[100px]" title={log.user_id}>{log.user_id}</td>
                      <td className="p-3"><Badge type={log.event_name} /></td>
                      <td className="p-3 text-right font-mono text-xs text-slate-600 whitespace-nowrap">
                        {log.item_name && log.item_name !== '-' ? (
                          <span title={log.item_name}>{log.item_name}</span>
                        ) : (
                          log.coin_spent > 0 ? <span className="text-emerald-600">-{log.coin_spent}</span> : '-'
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {/* Pagination controls... (Giữ nguyên) */}
            {data.logs.pagination && (
              <div className="flex justify-between items-center pt-2 border-t dark:border-slate-700">
                <div className="flex gap-1">
                  <button disabled={logPage <= 1} onClick={() => setLogPage(1)} className="p-2 hover:bg-slate-100 rounded disabled:opacity-30"><ChevronsLeft size={16} /></button>
                  <button disabled={logPage <= 1} onClick={() => setLogPage(p => p - 1)} className="p-2 hover:bg-slate-100 rounded disabled:opacity-30"><ChevronLeft size={16} /></button>
                </div>
                <span className="text-xs font-bold text-slate-500">Page {data.logs.pagination.current} / {data.logs.pagination.total_pages}</span>
                <div className="flex gap-1">
                  <button disabled={logPage >= data.logs.pagination.total_pages} onClick={() => setLogPage(p => p + 1)} className="p-2 hover:bg-slate-100 rounded disabled:opacity-30"><ChevronRight size={16} /></button>
                  <button disabled={logPage >= data.logs.pagination.total_pages} onClick={() => setLogPage(data.logs.pagination.total_pages)} className="p-2 hover:bg-slate-100 rounded disabled:opacity-30"><ChevronsRight size={16} /></button>
                </div>
              </div>
            )}
          </div>
        </>
      ) : (
        <div className="h-64 flex flex-col items-center justify-center text-slate-400 gap-3 border-2 border-dashed rounded-xl">
          <Activity size={48} className="opacity-20" />
          <span>Select a level above to view deep dive analytics</span>
        </div>
      )}
    </div>
  );
}

// Sub-components giữ nguyên
const Card = ({ title, value, icon: Icon, color }: any) => {
  const colorMap: any = {
    blue: "bg-blue-50 text-blue-600",
    emerald: "bg-emerald-50 text-emerald-600",
    yellow: "bg-yellow-50 text-yellow-600",
    purple: "bg-purple-50 text-purple-600",
    red: "bg-red-50 text-red-600 border border-red-200 animate-pulse",
  };
  return (
    <div className={`p-5 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm transition-all ${color === 'red' ? 'bg-red-50' : 'bg-white dark:bg-slate-800'}`}>
      <div className="flex justify-between items-start">
        <div>
          <p className={`text-xs font-bold uppercase tracking-wider ${color === 'red' ? 'text-red-500' : 'text-slate-400'}`}>{title}</p>
          <h4 className={`text-2xl font-bold mt-1 ${color === 'red' ? 'text-red-700' : 'text-slate-800 dark:text-white'}`}>{value}</h4>
        </div>
        <div className={`p-2 rounded-lg ${colorMap[color] || colorMap.blue}`}><Icon size={20} /></div>
      </div>
    </div>
  )
}

const Badge = ({ type }: { type: string }) => {
  let color = "bg-slate-100 text-slate-600";
  if (type.includes("Start") || type.includes("start")) color = "bg-blue-100 text-blue-700";
  if (type.includes("Complete") || type.includes("win")) color = "bg-emerald-100 text-emerald-700";
  if (type.includes("Fail") || type.includes("fail")) color = "bg-red-100 text-red-700";
  if (type.includes("Spend") || type.includes("boost") || type.includes("PRICE")) color = "bg-orange-100 text-orange-700"; // Added PRICE
  return <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${color}`}>{type}</span>
}