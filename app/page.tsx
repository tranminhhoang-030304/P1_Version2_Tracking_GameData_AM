"use client"

import React, { useState, useEffect } from 'react';
import { 
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, 
  PieChart, Pie, Cell, Legend 
} from 'recharts';
import { 
  LayoutDashboard, Activity, Settings, DollarSign, 
  PlayCircle, Video, CreditCard, Sun, Moon,
  Play, Trash2, Save, Plus, RotateCcw, XCircle, CheckCircle, AlertTriangle,
  Clock // Icon cho Settings mới
} from 'lucide-react';

// --- CONFIG ---
const API_URL = "https://p1-version2-tracking-gamedata-am.onrender.com"; 
const PIE_COLORS_1 = ['#00C49F', '#FFBB28']; 
const PIE_COLORS_2 = ['#3b82f6', '#ef4444']; 

// --- FORMAT HELPER ---
const formatCurrency = (value: number) => {
  if (value >= 1000000000) return (value / 1000000000).toFixed(2) + 'B';
  if (value >= 1000000) return (value / 1000000).toFixed(2) + 'M';
  if (value >= 1000) return (value / 1000).toFixed(1) + 'K';
  return value?.toLocaleString() || '0';
};

// ====================================================================================
// MAIN APP COMPONENT
// ====================================================================================
export default function GameAnalyticsApp() {
  const [activeTab, setActiveTab] = useState<'dashboard' | 'monitor' | 'settings'>('dashboard');
  const [darkMode, setDarkMode] = useState(true);
  
  // STATE TOÀN CỤC CHO LOG MONITOR
  const [monitorLogs, setMonitorLogs] = useState<string[]>([]);
  const [isJobRunning, setIsJobRunning] = useState(false);

  const themeClass = darkMode ? "bg-[#0f1219] text-slate-300" : "bg-slate-50 text-slate-800";
  const cardClass = darkMode ? "bg-[#1a1d26] border-slate-800" : "bg-white border-slate-200 shadow-sm";
  const textHeadClass = darkMode ? "text-white" : "text-slate-900";

  return (
    <div className={`flex min-h-screen font-sans transition-colors duration-300 ${themeClass}`}>
      
      {/* SIDEBAR */}
      <aside className={`w-64 border-r flex flex-col p-4 transition-colors duration-300 ${darkMode ? "bg-[#11141d] border-slate-800" : "bg-white border-slate-200"}`}>
        <div className="flex items-center gap-2 mb-2 px-2">
          <div className="text-blue-600 font-bold text-2xl">GameStats</div>
        </div>
        <div className="px-2 mb-8">
            <button onClick={() => setDarkMode(!darkMode)} className={`flex items-center gap-2 text-xs font-medium px-3 py-1.5 rounded-full border transition-all ${darkMode ? "border-slate-700 text-slate-400 hover:text-white" : "border-slate-300 text-slate-600 hover:text-black"}`}>
                {darkMode ? <><Sun size={14} /> Light Mode</> : <><Moon size={14} /> Dark Mode</>}
            </button>
        </div>
        <nav className="flex flex-col gap-2">
          <NavItem icon={<LayoutDashboard size={18}/>} label="Dashboard" active={activeTab === 'dashboard'} onClick={() => setActiveTab('dashboard')} darkMode={darkMode}/>
          <NavItem icon={<Activity size={18}/>} label="Monitor" active={activeTab === 'monitor'} onClick={() => setActiveTab('monitor')} darkMode={darkMode}/>
          <NavItem icon={<Settings size={18}/>} label="Settings" active={activeTab === 'settings'} onClick={() => setActiveTab('settings')} darkMode={darkMode}/>
        </nav>
      </aside>

      {/* MAIN AREA */}
      <main className="flex-1 p-8 overflow-y-auto h-screen custom-scrollbar">
        {activeTab === 'dashboard' && <DashboardView darkMode={darkMode} cardClass={cardClass} textHeadClass={textHeadClass} />}
        
        {activeTab === 'monitor' && (
            <MonitorView 
                darkMode={darkMode} cardClass={cardClass} textHeadClass={textHeadClass}
                logs={monitorLogs} setLogs={setMonitorLogs}
                isRunning={isJobRunning} setIsRunning={setIsJobRunning}
            />
        )}
        
        {activeTab === 'settings' && <SettingsView darkMode={darkMode} cardClass={cardClass} textHeadClass={textHeadClass} />}
      </main>
    </div>
  );
}

// ====================================================================================
// 1. DASHBOARD VIEW
// ====================================================================================
function DashboardView({ darkMode, cardClass, textHeadClass }: any) {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  const fetchDashboardData = async (manual = false) => {
    if (manual && !confirm("Làm mới dữ liệu Dashboard ngay lập tức?")) return;
    try {
      setLoading(true);
      const res = await fetch(`${API_URL}/dashboard/1`);
      if (res.ok) setData(await res.json());
    } catch (err) { console.error(err); } 
    finally { setLoading(false); }
  };

  useEffect(() => {
    fetchDashboardData();
    const interval = setInterval(() => fetchDashboardData(false), 30000);
    return () => clearInterval(interval);
  }, []);

  const chartData = data?.chart_data || [];
  const sortedByRevenue = [...chartData].sort((a: any, b: any) => b.total - a.total).slice(0, 8);
  
  const pieData1 = [{ name: 'Ads', value: data?.summary?.total_ads || 0 }, { name: 'IAP', value: data?.summary?.total_iap || 0 }].filter(i=>i.value>0);
  const pieData2 = [{ name: 'Win', value: data?.summary?.total_wins || 0 }, { name: 'Fail', value: data?.summary?.total_fails || 0 }].filter(i=>i.value>0);

  if (loading && !data) return <div className="p-10 text-slate-500">Loading Dashboard...</div>;

  return (
    <div className="animate-fade-in pb-10">
        <header className="flex justify-between items-center mb-8">
          <div><h1 className={`text-2xl font-bold ${textHeadClass}`}>Game Analytics Dashboard</h1><p className="text-sm text-slate-500 mt-1">Real-time Data Visualization</p></div>
          <button onClick={() => fetchDashboardData(true)} className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded-lg shadow-lg shadow-blue-500/30 transition-all flex items-center gap-2"><RotateCcw size={16}/> Refresh Data</button>
        </header>

        {/* KPI CARDS */}
        <div className="grid grid-cols-5 gap-4 mb-6">
          {/* Thẻ 1: Giữ nguyên (Vì hàm formatCurrency đã an toàn) */}
          <KpiCard 
            title="Coin Revenue" 
            value={formatCurrency(data?.summary?.total_revenue)} 
            icon={<DollarSign size={16}/>} 
            color="text-yellow-500" 
            cardClass={cardClass} 
            textHead={textHeadClass} 
          />
          
          {/* Thẻ 2: Sửa nhẹ thêm || 0 để đỡ hiện chữ 'undefined%' nếu chưa có số liệu */}
          <KpiCard 
            title="Avg Fail Rate" 
            value={`${data?.summary?.avg_fail_rate || 0}%`} 
            icon={<Activity size={16}/>} 
            color="text-red-500" 
            cardClass={cardClass} 
            textHead={textHeadClass}
          />

          {/* Thẻ 3: SỬA LỖI (Thêm || 0) */}
          <KpiCard 
            title="Active Players" 
            value={(data?.summary?.active_players || 0).toLocaleString()} 
            icon={<PlayCircle size={16}/>} 
            color="text-blue-500" 
            cardClass={cardClass} 
            textHead={textHeadClass}
          />

          {/* Thẻ 4: SỬA LỖI (Thêm || 0) */}
          <KpiCard 
            title="Ads Watched" 
            value={(data?.summary?.total_ads || 0).toLocaleString()} 
            icon={<Video size={16}/>} 
            color="text-green-500" 
            cardClass={cardClass} 
            textHead={textHeadClass}
          />

          {/* Thẻ 5: SỬA LỖI (Thêm || 0) */}
          <KpiCard 
            title="IAP Purchases" 
            value={(data?.summary?.total_iap || 0).toLocaleString()} 
            icon={<CreditCard size={16}/>} 
            color="text-purple-500" 
            cardClass={cardClass} 
            textHead={textHeadClass}
          />
        </div>

        {/* MAIN CHART */}
        <div className={`p-6 rounded-xl border mb-6 ${cardClass}`}>
          <h3 className="text-sm font-semibold text-slate-500 mb-6">Revenue vs Fail Rate by Level</h3>
          <div className="h-[400px] w-full overflow-x-auto custom-scrollbar">
            <div className="min-w-[2000px] h-full"> 
                <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={chartData} margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                    <defs><linearGradient id="colorBar" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#3b82f6" stopOpacity={0.8}/><stop offset="95%" stopColor="#3b82f6" stopOpacity={0.3}/></linearGradient></defs>
                    <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? "#2d3748" : "#e2e8f0"} vertical={false} />
                    <XAxis dataKey="name" interval={0} angle={-90} textAnchor="end" stroke="#94a3b8" fontSize={10} tickLine={false} axisLine={false} height={60}/>
                    <YAxis yAxisId="left" stroke="#94a3b8" fontSize={11} tickLine={false} axisLine={false} tickFormatter={formatCurrency}/>
                    <YAxis yAxisId="right" orientation="right" stroke="#ef4444" fontSize={11} tickLine={false} axisLine={false} unit="%" />
                    <Tooltip contentStyle={{ backgroundColor: darkMode ? '#1a202c' : '#fff', borderColor: darkMode ? '#2d3748' : '#e2e8f0', borderRadius: '8px' }} />
                    <Bar yAxisId="left" dataKey="total" name="Revenue" fill="url(#colorBar)" barSize={8} radius={[4, 4, 0, 0]} />
                    <Line yAxisId="right" type="monotone" dataKey="failRate" name="Fail Rate" stroke="#ef4444" strokeWidth={1} dot={false} />
                </ComposedChart>
                </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* 2 TABLES */}
        <div className="grid grid-cols-2 gap-6 mb-6">
          <div className={`p-6 rounded-xl border h-[350px] overflow-hidden flex flex-col ${cardClass}`}>
            <h3 className="text-sm font-semibold text-slate-500 mb-4">All Levels Data (Sorted by Level)</h3>
            <div className="overflow-auto flex-1 pr-2 custom-scrollbar"><TableContent data={chartData} textHead={textHeadClass} /></div>
          </div>
          <div className={`p-6 rounded-xl border h-[350px] overflow-hidden flex flex-col relative ${cardClass}`}>
            <div className="absolute top-0 left-0 w-1 h-full bg-yellow-500"></div>
            <h3 className="text-sm font-semibold text-yellow-600 mb-4 flex items-center gap-2"><DollarSign size={16}/> Top Highest Revenue</h3>
            <div className="overflow-auto flex-1 pr-2 custom-scrollbar"><TableContent data={sortedByRevenue} highlightRevenue textHead={textHeadClass} /></div>
          </div>
        </div>

        {/* 2 PIE CHARTS */}
        <div className="grid grid-cols-2 gap-6">
            <PieCard title="Ads vs IAP Distribution" data={pieData1} colors={PIE_COLORS_1} cardClass={cardClass} />
            <PieCard title="Win vs Fail Ratio" data={pieData2} colors={PIE_COLORS_2} cardClass={cardClass} />
        </div>
    </div>
  );
}

// ====================================================================================
// 2. MONITOR VIEW
// ====================================================================================
function MonitorView({ darkMode, cardClass, textHeadClass, logs, setLogs, isRunning, setIsRunning }: any) {
    const [jobHistory, setJobHistory] = useState<any[]>([]);

    const fetchHistory = async () => {
        try {
            const res = await fetch(`${API_URL}/etl/history`);
            if (res.ok) setJobHistory(await res.json());
        } catch (e) {}
    };

    useEffect(() => {
        fetchHistory();
        const interval = setInterval(fetchHistory, 5000);
        return () => clearInterval(interval);
    }, []);

    const handleRunJob = async () => {
        if (!confirm("Xác nhận chạy ETL Job ngay bây giờ?")) return;
        setIsRunning(true);
        const timeStr = new Date().toLocaleTimeString();
        setLogs((prev:any) => [`[${timeStr}] ⏳ Job Started (Manual Trigger)...`, ...prev]);
        
        try {
            const res = await fetch(`${API_URL}/etl/run/1`, { method: 'POST' });
            const data = await res.json();
            
            if (data.status === 'success') {
                setLogs((prev:any) => [`[${timeStr}] ✅ Job Finished. Saved file.`, ...prev]);
            } else {
                setLogs((prev:any) => [`[${timeStr}] ❌ Job Failed. Reason: ${data.message}`, ...prev]);
            }
            fetchHistory(); 
        } catch (error) {
            setLogs((prev:any) => [`[${timeStr}] 🚨 NETWORK ERROR: Cannot connect to Backend.`, ...prev]);
        } finally {
            setIsRunning(false);
        }
    };

    const handleDeleteAll = async () => {
        if (!confirm("CẢNH BÁO: Xóa toàn bộ lịch sử chạy?")) return;
        await fetch(`${API_URL}/etl/history/all`, { method: 'DELETE' }); fetchHistory();
    };

    return (
        <div className="animate-fade-in max-w-5xl mx-auto pb-10">
            <header className="mb-8"><h1 className={`text-2xl font-bold ${textHeadClass}`}>System Monitor</h1></header>

            <div className={`p-6 rounded-xl border mb-6 flex justify-between items-center ${cardClass}`}>
                <div><h3 className={`text-lg font-semibold ${textHeadClass}`}>Control Panel</h3><p className="text-sm text-slate-500">Manual ETL Trigger</p></div>
                <div className="flex gap-4">
                    <button onClick={handleRunJob} disabled={isRunning} className={`flex items-center gap-2 px-6 py-3 rounded-lg font-bold text-white transition-all ${isRunning ? 'bg-slate-600' : 'bg-green-600 hover:bg-green-700 shadow-lg shadow-green-500/30'}`}>
                        {isRunning ? <><Activity className="animate-spin"/> Processing...</> : <><Play size={18}/> Run Job Now</>}
                    </button>
                    <button onClick={handleDeleteAll} className="flex items-center gap-2 px-6 py-3 rounded-lg font-bold text-white bg-red-600 hover:bg-red-700 shadow-lg shadow-red-500/30"><Trash2 size={18}/> Clear Logs</button>
                </div>
            </div>

            <div className={`p-6 rounded-xl border h-[200px] flex flex-col mb-6 ${cardClass}`}>
                <h3 className="text-sm font-semibold text-slate-500 mb-2">Live Console Logs</h3>
                <div className="flex-1 bg-black/90 rounded-lg p-4 font-mono text-xs text-green-400 overflow-y-auto custom-scrollbar">
                    {logs.length === 0 ? <span className="text-slate-600 italic">System Ready. Waiting for schedule or manual run...</span> : logs.map((log:string, i:number) => <div key={i} className="border-b border-white/5 py-1">{log}</div>)}
                </div>
            </div>

            <div className={`p-6 rounded-xl border ${cardClass}`}>
                <h3 className={`text-lg font-semibold mb-4 ${textHeadClass}`}>Execution History (Recent 50)</h3>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm text-left">
                        <thead className="text-xs uppercase bg-slate-800 text-slate-400"><tr><th className="px-4 py-3">Type</th><th className="px-4 py-3">Status</th><th className="px-4 py-3">Rows</th><th className="px-4 py-3">Message</th><th className="px-4 py-3">Time</th></tr></thead>
                        <tbody className="divide-y divide-slate-700">
                            {jobHistory.map((job, index) => (
                                <tr key={job.id} className="hover:bg-slate-700/30">
                                    <td className="px-4 py-3 font-mono text-blue-400 font-bold text-xs">{job.job_code}</td>
                                    <td className="px-4 py-3">
                                        {job.status === 'success' && <span className="bg-green-500/20 text-green-400 px-2 py-1 rounded text-xs font-bold">Success</span>}
                                        {job.status === 'failed' && <span className="bg-red-500/20 text-red-400 px-2 py-1 rounded text-xs font-bold">Failed</span>}
                                        {job.status === 'running' && <span className="bg-blue-500/20 text-blue-400 px-2 py-1 rounded text-xs font-bold animate-pulse">Running</span>}
                                    </td>
                                    <td className="px-4 py-3 font-bold">{job.rows_processed}</td>
                                    <td className="px-4 py-3 text-xs opacity-80 truncate max-w-[200px]" title={job.message}>{job.message}</td>
                                    <td className="px-4 py-3 text-slate-500 text-xs">{new Date(job.start_time).toLocaleString()}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    )
}

// ====================================================================================
// 3. SETTINGS VIEW (ĐÃ BỔ SUNG CỘT SOURCE)
// ====================================================================================
function SettingsView({ darkMode, cardClass, textHeadClass }: any) {
    const [config, setConfig] = useState({ 
        app_name: '', appmetrica_app_id: '', appmetrica_token: '', 
        daily_schedule_time: '09:00', interval_minutes: 60          
    });
    const [items, setItems] = useState<any[]>([]);
    const [newItem, setNewItem] = useState({ event_param_key: '', display_name: '', price: '' });

    useEffect(() => {
        fetch(`${API_URL}/apps/`).then(res => res.json()).then(data => { if(data) setConfig(data); });
        fetchBooster();
    }, []);

    const fetchBooster = () => fetch(`${API_URL}/boosters/`).then(res => res.json()).then(data => setItems(data));

    const handleSaveConfig = async () => {
        if (!confirm("Lưu cấu hình và cập nhật lịch chạy?")) return;
        await fetch(`${API_URL}/apps/`, {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(config)
        });
        alert("Đã lưu thành công! Hệ thống sẽ chạy theo lịch mới.");
    };

    const handleAddItem = async () => {
        if(!newItem.event_param_key || !newItem.display_name) return alert("Thiếu thông tin");
        await fetch(`${API_URL}/boosters/`, {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ app_id: 1, ...newItem, price: parseFloat(newItem.price || '0') }) 
        });
        setNewItem({ event_param_key: '', display_name: '', price: '' }); fetchBooster();
    };

    const handleDeleteItem = async (id: number) => {
        if(!confirm("Xóa Item này?")) return;
        await fetch(`${API_URL}/boosters/${id}`, { method: 'DELETE' }); fetchBooster();
    };

    return (
        <div className="animate-fade-in max-w-4xl mx-auto pb-10">
            <header className="mb-8"><h1 className={`text-2xl font-bold ${textHeadClass}`}>Settings</h1></header>

            <div className={`p-6 rounded-xl border mb-8 ${cardClass}`}>
                <h3 className={`text-lg font-semibold mb-6 flex items-center gap-2 ${textHeadClass}`}><Settings size={20}/> System Configuration</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div><label className="block text-xs font-medium text-slate-500 mb-2 uppercase">App Name</label><input type="text" value={config.app_name} onChange={(e) => setConfig({...config, app_name: e.target.value})} className={`w-full p-3 rounded-lg border outline-none ${darkMode ? 'bg-[#11141d] border-slate-700 text-white' : 'bg-slate-50 border-slate-300 text-slate-900'}`}/></div>
                    <div><label className="block text-xs font-medium text-slate-500 mb-2 uppercase">AppMetrica ID</label><input type="text" value={config.appmetrica_app_id} onChange={(e) => setConfig({...config, appmetrica_app_id: e.target.value})} className={`w-full p-3 rounded-lg border outline-none ${darkMode ? 'bg-[#11141d] border-slate-700 text-white' : 'bg-slate-50 border-slate-300 text-slate-900'}`}/></div>
                    <div className="md:col-span-2"><label className="block text-xs font-medium text-slate-500 mb-2 uppercase">API Token</label><input type="password" value={config.appmetrica_token} onChange={(e) => setConfig({...config, appmetrica_token: e.target.value})} className={`w-full p-3 rounded-lg border outline-none ${darkMode ? 'bg-[#11141d] border-slate-700 text-white' : 'bg-slate-50 border-slate-300 text-slate-900'}`}/></div>
                    
                    <div className="p-4 border border-blue-500/30 rounded-lg bg-blue-500/5">
                        <label className="block text-xs font-medium text-blue-400 mb-2 uppercase flex items-center gap-2"><Clock size={14}/> Daily Schedule</label>
                        <input type="time" value={config.daily_schedule_time} onChange={(e) => setConfig({...config, daily_schedule_time: e.target.value})} className={`w-full p-3 rounded-lg border outline-none ${darkMode ? 'bg-[#11141d] border-slate-700 text-white' : 'bg-slate-50 border-slate-300 text-slate-900'}`}/>
                    </div>
                    <div className="p-4 border border-green-500/30 rounded-lg bg-green-500/5">
                        <label className="block text-xs font-medium text-green-400 mb-2 uppercase flex items-center gap-2"><RotateCcw size={14}/> Interval Cycle</label>
                        <input type="number" min="0" value={config.interval_minutes} onChange={(e) => setConfig({...config, interval_minutes: parseInt(e.target.value) || 0})} className={`w-full p-3 rounded-lg border outline-none ${darkMode ? 'bg-[#11141d] border-slate-700 text-white' : 'bg-slate-50 border-slate-300 text-slate-900'}`}/>
                    </div>
                </div>
                <div className="mt-6 flex justify-end"><button onClick={handleSaveConfig} className="flex items-center gap-2 px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-medium transition-colors"><Save size={16}/> Save Configuration</button></div>
            </div>

            <div className={`p-6 rounded-xl border ${cardClass}`}>
                <div className="flex justify-between items-center mb-6"><h3 className={`text-lg font-semibold flex items-center gap-2 ${textHeadClass}`}><DollarSign size={20}/> Item Management</h3></div>
                
                {/* FORM ADD MANUAL */}
                <div className="flex gap-4 mb-6 items-end">
                      <div className="flex-1"><input placeholder="Display Name (VD: Gold Pack)" value={newItem.display_name} onChange={e=>setNewItem({...newItem, display_name: e.target.value})} className="w-full p-2 rounded border bg-transparent text-slate-400 text-sm"/></div>
                      <div className="flex-1"><input placeholder="Key (VD: packID_Gold)" value={newItem.event_param_key} onChange={e=>setNewItem({...newItem, event_param_key: e.target.value})} className="w-full p-2 rounded border bg-transparent text-slate-400 text-sm"/></div>
                      <div className="w-24"><input type="number" placeholder="Price" value={newItem.price} onChange={e=>setNewItem({...newItem, price: e.target.value})} className="w-full p-2 rounded border bg-transparent text-slate-400 text-sm"/></div>
                      <button onClick={handleAddItem} className="px-4 py-2 bg-slate-700 hover:bg-slate-600 text-white rounded text-sm"><Plus size={16}/></button>
                </div>

                {/* TABLE CÓ CỘT SOURCE */}
                <div className="overflow-x-auto">
                    <table className="w-full text-sm text-left">
                        <thead className={`text-xs uppercase border-b ${darkMode ? 'text-slate-500 border-slate-700' : 'text-slate-400 border-slate-200'}`}>
                            <tr>
                                <th className="px-4 py-3">No.</th>
                                <th className="px-4 py-3">Name</th>
                                <th className="px-4 py-3">Key</th>
                                <th className="px-4 py-3 text-right">Price</th>
                                <th className="px-4 py-3 text-center">Source</th> {/* <--- CỘT NÀY ĐÂY */}
                                <th className="px-4 py-3 text-center">Action</th>
                            </tr>
                        </thead>
                        <tbody className={`divide-y ${darkMode ? 'divide-slate-800' : 'divide-slate-100'}`}>
                            {items.map((item, index) => (
                                <tr key={item.id} className="hover:opacity-80">
                                    <td className={`px-4 py-3 ${textHeadClass}`}>{index + 1}</td>
                                    <td className={`px-4 py-3 font-medium ${textHeadClass}`}>{item.display_name}</td>
                                    <td className="px-4 py-3 text-slate-500 font-mono">{item.event_param_key}</td>
                                    <td className="px-4 py-3 text-right text-green-500 font-bold">${item.price}</td>
                                    <td className="px-4 py-3 text-center">
                                        {item.source_type === 'AUTO' 
                                            ? <span className="bg-purple-500/20 text-purple-400 px-2 py-1 rounded text-xs font-bold border border-purple-500/30">Auto-Scan</span>
                                            : <span className="bg-slate-500/20 text-slate-400 px-2 py-1 rounded text-xs font-bold border border-slate-500/30">Manual</span>
                                        }
                                    </td>
                                    <td className="px-4 py-3 flex justify-center gap-3"><button onClick={()=>handleDeleteItem(item.id)} className="text-red-500 hover:text-red-400"><Trash2 size={16}/></button></td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                    {items.length === 0 && <div className="p-4 text-center text-slate-500 italic">Chưa có items. Hãy chạy "Run Job Now" để tự động quét từ AppMetrica!</div>}
                </div>
            </div>
        </div>
    )
}

// SHARED COMPONENTS (GIỮ NGUYÊN)
function NavItem({ icon, label, active, onClick, darkMode }: any) { return (<div onClick={onClick} className={`flex items-center gap-3 px-3 py-2 mx-2 rounded-lg cursor-pointer transition-colors ${active ? 'bg-blue-600/10 text-blue-600' : darkMode ? 'text-slate-400 hover:text-white hover:bg-slate-800' : 'text-slate-500 hover:text-slate-900 hover:bg-slate-100'}`}>{icon} <span className="text-sm font-medium">{label}</span></div>); }
function KpiCard({ title, value, icon, color, cardClass, textHead }: any) { return (<div className={`p-4 rounded-xl border flex flex-col justify-between ${cardClass}`}><div className="flex justify-between items-start mb-2"><span className="text-slate-500 text-xs font-medium uppercase">{title}</span><div className={color}>{icon}</div></div><div className={`text-xl font-bold ${textHead}`}>{value}</div></div>); }
function TableContent({ data, highlightRevenue, textHead }: any) { return (<table className="w-full text-xs text-left text-slate-500"><thead className="uppercase sticky top-0 opacity-80 backdrop-blur-sm"><tr><th className="px-3 py-2">Level</th><th className="px-3 py-2 text-right">Rev</th><th className="px-3 py-2 text-center">Ads</th><th className="px-3 py-2 text-center">IAP</th><th className="px-3 py-2 text-right">Fail</th></tr></thead><tbody>{data.map((row: any, i: number) => (<tr key={i} className="border-b border-dashed border-slate-700/30 hover:bg-slate-500/5"><td className={`px-3 py-2 font-medium ${textHead}`}>{row.name}</td><td className={`px-3 py-2 text-right font-mono ${highlightRevenue ? 'text-yellow-500 font-bold' : textHead}`}>{formatCurrency(row.total)}</td><td className="px-3 py-2 text-center">{row.ads}</td><td className="px-3 py-2 text-center">{row.iap}</td><td className="px-3 py-2 text-right">{row.failRate}%</td></tr>))}</tbody></table>); }
function PieCard({title, data, colors, cardClass}: any) { return (<div className={`p-6 rounded-xl border flex flex-col items-center ${cardClass}`}><h3 className="text-sm font-semibold text-slate-500 mb-4 w-full text-left">{title}</h3><div className="h-[250px] w-full flex justify-center"><ResponsiveContainer width="100%" height="100%"><PieChart><Pie data={data} cx="50%" cy="50%" innerRadius={60} outerRadius={80} paddingAngle={5} dataKey="value" label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>{data.map((entry: any, index: any) => (<Cell key={`cell-${index}`} fill={colors[index % colors.length]} stroke="none" />))}</Pie><Tooltip /> <Legend verticalAlign="bottom" height={36}/></PieChart></ResponsiveContainer></div></div>); }