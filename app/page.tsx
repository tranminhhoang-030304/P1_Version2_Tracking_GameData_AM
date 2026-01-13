'use client';
import React, { useState, useEffect } from 'react';
import LevelDetailTab from './components/LevelDetailTab';
import DataExplorer from './components/DataExplorer';
import {
  LayoutDashboard, Settings, Activity, Server,
  Play, CheckCircle, Save, Plus, BarChart3, List,
  Calendar, Clock, AlertCircle, X, RotateCcw, FileText, Trash2, StopCircle, RefreshCw,
  Bot, Zap, FlaskConical, Filter, PieChart as PieIcon, Coins, Gamepad2, Database,
  ChevronsLeft, ChevronsRight, ChevronLeft, ChevronRight, TrendingUp, Users
} from 'lucide-react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, ResponsiveContainer, BarChart, Bar,
  PieChart, Pie, Cell, Legend
} from 'recharts';

// --- TYPE DEFINITIONS ---
interface AppConfig {
  id: number;
  name: string;
  app_id: string;
  api_token: string;
  is_active: boolean;
  schedule_time?: string;
  interval_minutes?: number;
}

interface DashboardData {
  overview: {
    cards: { revenue: number; active_users: number; fail_rate_avg: number };
    chart_main: any[];
  };
  detailed: {
    level_stats: any[];
    booster_stats: any[];
    raw_table: any[];
    event_dictionary: string[];
  };
}

const API_URL = 'http://127.0.0.1:8080';
const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d'];

const StatCard = ({ title, value, icon: Icon, color }: any) => (
  <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm hover:shadow-md transition-shadow">
    <div className="flex justify-between items-start">
      <div>
        <p className="text-slate-500 dark:text-slate-400 text-sm font-medium">{title}</p>
        <h3 className="text-2xl font-bold text-slate-800 dark:text-white mt-2">{value}</h3>
      </div>
      <div className={`p-3 rounded-lg ${color} bg-opacity-10`}>
        <Icon size={20} className={color.replace('bg-', 'text-')} />
      </div>
    </div>
  </div>
);

const NavItem = ({ icon: Icon, label, active, onClick }: any) => (
  <button
    onClick={onClick}
    className={`flex items-center gap-3 px-4 py-3 rounded-lg w-full transition-all ${active
      ? 'bg-blue-600 text-white shadow-md shadow-blue-500/20'
      : 'text-slate-500 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800'
      }`}
  >
    <Icon size={20} />
    <span className="font-medium">{label}</span>
  </button>
);

export default function GameAnalyticsApp() {
  const [activeTab, setActiveTab] = useState('settings');
  const [apps, setApps] = useState<AppConfig[]>([]);
  const [selectedApp, setSelectedApp] = useState<AppConfig | null>(null);
  const [dashboardData, setDashboardData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(false);

  // Load danh sách App
  const fetchApps = () => {
    fetch(`${API_URL}/apps`)
      .then(res => res.json())
      .then(data => {
        setApps(data);
        if (selectedApp) {
          const updatedCurrentApp = data.find((a: AppConfig) => a.id === selectedApp.id);
          if (updatedCurrentApp) setSelectedApp(updatedCurrentApp);
        } else if (data.length > 0) {
          setSelectedApp(data[0]);
        }
      })
      .catch(err => console.error("Load apps error:", err));
  };

  useEffect(() => { fetchApps(); }, []);

  useEffect(() => {
    if (activeTab === 'dashboard' && selectedApp) {
      setLoading(true);
      fetch(`${API_URL}/dashboard/${selectedApp.id}`)
        .then(res => res.json())
        .then(data => {
          if (data.success) setDashboardData(data);
          else setDashboardData(null);
        })
        .catch(err => setDashboardData(null))
        .finally(() => setLoading(false));
    }
  }, [activeTab, selectedApp]);

  return (
    <div className="flex h-screen bg-slate-50 dark:bg-slate-900 font-sans text-slate-800 dark:text-slate-200">
      {/* SIDEBAR */}
      <aside className="w-64 bg-white dark:bg-slate-800 border-r border-slate-200 dark:border-slate-700 flex flex-col z-10 shrink-0">
        <div className="p-6 h-full flex flex-col">
          <div className="flex items-center gap-2 text-blue-600 dark:text-blue-400 mb-8">
            <Activity size={28} strokeWidth={2.5} />
            <span className="text-xl font-bold tracking-tight">GameStats</span>
          </div>

          <div className="mb-6 p-4 bg-slate-50 dark:bg-slate-900 rounded-xl border border-slate-100 dark:border-slate-700">
            <p className="text-xs text-slate-400 font-semibold uppercase tracking-wider mb-2">Active Project</p>
            <div className="font-medium text-slate-700 dark:text-slate-200 truncate" title={selectedApp?.name}>
              {selectedApp ? selectedApp.name : 'Select a Game'}
            </div>
          </div>

          <nav className="space-y-2 flex-1">
            <NavItem icon={Settings} label="Settings" active={activeTab === 'settings'} onClick={() => setActiveTab('settings')} />
            <NavItem icon={Server} label="Monitor" active={activeTab === 'monitor'} onClick={() => setActiveTab('monitor')} />
            <NavItem icon={LayoutDashboard} label="Dashboard" active={activeTab === 'dashboard'} onClick={() => setActiveTab('dashboard')} />
            <NavItem icon={Database} label="Data Explorer" active={activeTab === 'data'} onClick={() => setActiveTab('data')} />
          </nav>

          <div className="text-xs text-center text-slate-400 mt-4">v1.5.0 Analytics Config</div>
        </div>
      </aside>

      {/* MAIN CONTENT */}
      <main className="flex-1 overflow-y-auto p-8 relative">
        {activeTab === 'settings' && (
          <SettingsView
            apps={apps}
            selectedApp={selectedApp}
            onSelectApp={setSelectedApp}
            onRefresh={fetchApps}
          />
        )}

        {activeTab === 'monitor' && (
          <MonitorView selectedApp={selectedApp} />
        )}

        {activeTab === 'dashboard' && (
          dashboardData ? (
            <DashboardView data={dashboardData} loading={loading} appId={selectedApp!.id} />
          ) : (
            <div className="flex h-full items-center justify-center text-slate-400 flex-col gap-4">
              <BarChart3 size={48} className="opacity-20" />
              {selectedApp ? "No data available. Run ETL in Monitor tab." : "Please select a game in Settings first."}
            </div>
          )
        )}

        {activeTab === 'data' && (
          selectedApp ? (
            <DataExplorer appId={selectedApp.id} />
          ) : (
            <div className="flex h-full items-center justify-center text-slate-400 flex-col gap-4">
              <Database size={48} className="opacity-20" />
              Please select a game first.
            </div>
          )
        )}
      </main>
    </div>
  );
}

// --- VIEW: SETTINGS (UPDATED) ---
function SettingsView({ apps, selectedApp, onSelectApp, onRefresh }: any) {
  const [isAdding, setIsAdding] = useState(false);
  const [showAnalyticsConfig, setShowAnalyticsConfig] = useState(false); // Modal state

  // Form chính (Giữ nguyên logic cũ)
  const [formData, setFormData] = useState({
    name: '', app_id: '', api_token: '',
    schedule_time: '12:00', interval_minutes: 60, is_active: true
  });

  // Form Analytics (Mới thêm)
  const [analyticsData, setAnalyticsData] = useState<any>({
    events: { level_start: '', level_win: '', level_fail: '' },
    boosters: []
  });
  const [previewData, setPreviewData] = useState<any>(null);

  // Effect: Sync dữ liệu khi chọn App
  useEffect(() => {
    if (isAdding) {
      setFormData({ name: '', app_id: '', api_token: '', schedule_time: '00:00', interval_minutes: 60, is_active: true });
    } else if (selectedApp) {
      setFormData({
        name: selectedApp.name,
        app_id: selectedApp.app_id || '',
        api_token: selectedApp.api_token || '',
        schedule_time: selectedApp.schedule_time || '12:00',
        interval_minutes: selectedApp.interval_minutes || 60,
        is_active: selectedApp.is_active
      });
    }
  }, [selectedApp, isAdding]);

  // --- THÊM EFFECT MỚI: Load Preview Data khi chọn App ---
  useEffect(() => {
    if (selectedApp?.id) {
      fetch(`${API_URL}/apps/${selectedApp.id}/analytics-config`)
        .then(res => {
          if (res.ok) return res.json();
          return null;
        })
        .then(data => {
          // Kiểm tra data có hợp lệ không trước khi set
          if (data && (data.events || (data.boosters && data.boosters.length > 0))) {
            setPreviewData(data);
          } else {
            setPreviewData(null);
          }
        })
        .catch(() => setPreviewData(null));
    } else {
      setPreviewData(null);
    }
  }, [selectedApp]);

  // --- LOGIC MỚI: ANALYTICS CONFIG ---

  // Load Config khi mở modal
  const handleOpenAnalytics = async () => {
    if (!selectedApp) return;
    try {
      const res = await fetch(`${API_URL}/apps/${selectedApp.id}/analytics-config`);
      const data = await res.json();
      // Nếu server trả về rỗng hoặc lỗi, dùng default
      setAnalyticsData(data.events ? data : {
        events: { level_start: '', level_win: '', level_fail: '' },
        boosters: []
      });
      setShowAnalyticsConfig(true);
    } catch (e) {
      // Fallback nếu chưa có config
      setAnalyticsData({
        events: { level_start: '', level_win: '', level_fail: '' },
        boosters: []
      });
      setShowAnalyticsConfig(true);
    }
  }

  const handleSaveAnalytics = async () => {
    if (!selectedApp) return;
    try {
      await fetch(`${API_URL}/apps/${selectedApp.id}/analytics-config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(analyticsData)
      });
      alert("Analytics Configuration Saved!");
      setShowAnalyticsConfig(false);

      onRefresh();
    } catch (e) { alert("Error saving config"); }
  }

  // Quản lý list booster
  const addBooster = () => {
    setAnalyticsData({
      ...analyticsData,
      boosters: [...(analyticsData.boosters || []), { event_name: '', display_name: '', coin_cost: 0 }]
    });
  };

  const removeBooster = (index: number) => {
    const newBoosters = [...analyticsData.boosters];
    newBoosters.splice(index, 1);
    setAnalyticsData({ ...analyticsData, boosters: newBoosters });
  }

  const updateBooster = (index: number, field: string, value: any) => {
    const newBoosters = [...analyticsData.boosters];
    newBoosters[index] = { ...newBoosters[index], [field]: value };
    setAnalyticsData({ ...analyticsData, boosters: newBoosters });
  }

  // --- LOGIC CŨ: MAIN APP SAVE/DELETE ---

  const handleSubmit = async () => {
    const url = isAdding ? `${API_URL}/apps` : `${API_URL}/apps/${selectedApp.id}`;
    const method = isAdding ? 'POST' : 'PUT';
    try {
      const res = await fetch(url, {
        method: method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData)
      });
      if (res.ok) {
        alert(isAdding ? "Created New Game!" : "Saved Changes!");
        setIsAdding(false);
        onRefresh();
      } else {
        const err = await res.json();
        alert("Failed: " + err.error);
      }
    } catch (e) { alert("Error connecting to server!"); }
  };

  const handleDelete = async (e: any, appToDelete: any) => {
    e.stopPropagation();
    if (!confirm(`Delete ${appToDelete.name}?`)) return;
    try {
      const res = await fetch(`${API_URL}/apps/${appToDelete.id}`, { method: 'DELETE' });
      if (res.ok) {
        if (selectedApp?.id === appToDelete.id) onSelectApp(null);
        onRefresh();
      }
    } catch (err) { alert("Error!"); }
  };

  return (
    <div className="space-y-8 animate-in fade-in zoom-in duration-300 relative">
      {/* 1. LIST APPS GRID */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {apps.map((app: any) => (
          <div
            key={app.id}
            onClick={() => { setIsAdding(false); onSelectApp(app); }}
            className={`group cursor-pointer p-4 rounded-xl border-2 transition-all relative flex items-center justify-between ${!isAdding && selectedApp && selectedApp.id === app.id
              ? 'border-blue-500 bg-blue-50' : 'border-slate-200 hover:border-blue-300'
              }`}
          >
            <div className="flex-1 min-w-0 pr-2">
              <h3 className="font-bold truncate" title={app.name}>{app.name}</h3>
            </div>

            <div className="flex items-center gap-2 shrink-0">
              {(!isAdding && selectedApp?.id === app.id) && (
                <CheckCircle size={20} className="text-blue-500" />
              )}
              <button
                onClick={(e) => handleDelete(e, app)}
                className="p-1.5 text-red-400 hover:text-red-600 hover:bg-red-100 rounded-md opacity-0 group-hover:opacity-100 transition-opacity"
              >
                <Trash2 size={18} />
              </button>
            </div>
          </div>
        ))}

        <div onClick={() => { setIsAdding(true); onSelectApp(null); }} className={`p-4 rounded-xl border-2 border-dashed flex items-center justify-center cursor-pointer transition-all ${isAdding ? 'border-blue-500 bg-blue-50 text-blue-600' : 'border-slate-300 hover:border-blue-400 text-slate-400'}`}>
          <div className="flex flex-col items-center"><Plus size={24} /><span className="font-medium">Add New Game</span></div>
        </div>
      </div>

      {/* 2. MAIN CONFIG FORM */}
      <div className="bg-white dark:bg-slate-800 p-8 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
        <div className="flex justify-between items-start mb-6">
          <h3 className="text-lg font-bold flex items-center gap-2">
            <Settings size={20} />
            {isAdding ? "Create New Game Config" : `Configuration: ${formData.name}`}
          </h3>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="space-y-2">
            <label className="text-sm font-medium">Game Name</label>
            <input type="text" className="w-full px-4 py-2 rounded border" value={formData.name} onChange={e => setFormData({ ...formData, name: e.target.value })} />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium">AppMetrica App ID</label>
            <input type="text" className="w-full px-4 py-2 rounded border" value={formData.app_id} onChange={e => setFormData({ ...formData, app_id: e.target.value })} />
          </div>
          <div className="space-y-2 col-span-2">
            <label className="text-sm font-medium">OAuth Token</label>
            <input type="password" className="w-full px-4 py-2 rounded border font-mono" value={formData.api_token} onChange={e => setFormData({ ...formData, api_token: e.target.value })} />
          </div>
          <div className="space-y-2"><label className="text-sm font-medium">Schedule Time</label><input type="time" className="w-full px-4 py-2 rounded border" value={formData.schedule_time} onChange={e => setFormData({ ...formData, schedule_time: e.target.value })} /></div>
          <div className="space-y-2"><label className="text-sm font-medium">Interval (Min)</label><input type="number" className="w-full px-4 py-2 rounded border" value={formData.interval_minutes} onChange={e => setFormData({ ...formData, interval_minutes: parseInt(e.target.value) })} /></div>
          <div className="flex items-center gap-3 mt-4"><input type="checkbox" className="w-5 h-5" checked={formData.is_active} onChange={e => setFormData({ ...formData, is_active: e.target.checked })} /><label className="text-sm font-medium">Active (Enable Auto-Sync)</label></div>
        </div>

        {/* --- PHẦN MỚI: BẢNG PREVIEW CONFIG --- */}
        {/* --- HIỂN THỊ PREVIEW DỰA TRÊN STATE MỚI --- */}
        {previewData && (
          <div className="mt-6 bg-slate-50 dark:bg-slate-900/50 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden animate-in fade-in slide-in-from-top-2">
            {/* Header nhỏ */}
            <div className="px-4 py-2 bg-slate-100 dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700 flex justify-between items-center">
              <span className="text-xs font-bold uppercase text-slate-500 tracking-wider">Analytics Map Preview</span>
              <span className="text-xs text-blue-600 italic">Active Configuration</span>
            </div>

            <div className="p-4 grid grid-cols-1 md:grid-cols-2 gap-6">
              {/* Cột 1: Level Events */}
              <div>
                <h4 className="text-xs font-bold text-slate-400 uppercase mb-2 flex items-center gap-1"><Gamepad2 size={12} /> Level Events</h4>
                <div className="space-y-1 text-sm border-l-2 border-slate-200 pl-3">
                  <div className="flex justify-between items-center gap-2">
                    <span className="text-slate-500 text-xs">Start:</span>
                    <code className="bg-white dark:bg-slate-800 px-1.5 py-0.5 rounded border border-slate-200 dark:border-slate-600 text-blue-600 font-mono text-xs truncate max-w-[150px]">{previewData.events?.level_start || '-'}</code>
                  </div>
                  <div className="flex justify-between items-center gap-2">
                    <span className="text-slate-500 text-xs">Win:</span>
                    <code className="bg-white dark:bg-slate-800 px-1.5 py-0.5 rounded border border-slate-200 dark:border-slate-600 text-emerald-600 font-mono text-xs truncate max-w-[150px]">{previewData.events?.level_win || '-'}</code>
                  </div>
                  <div className="flex justify-between items-center gap-2">
                    <span className="text-slate-500 text-xs">Fail:</span>
                    <code className="bg-white dark:bg-slate-800 px-1.5 py-0.5 rounded border border-slate-200 dark:border-slate-600 text-red-500 font-mono text-xs truncate max-w-[150px]">{previewData.events?.level_fail || '-'}</code>
                  </div>
                </div>
              </div>

              {/* Cột 2: Boosters List */}
              <div>
                <h4 className="text-xs font-bold text-slate-400 uppercase mb-2 flex items-center gap-1"><Coins size={12} /> Boosters ({previewData.boosters?.length || 0})</h4>
                {previewData.boosters && previewData.boosters.length > 0 ? (
                  <div className="max-h-32 overflow-y-auto custom-scrollbar border border-slate-200 dark:border-slate-700 rounded bg-white dark:bg-slate-800">
                    <table className="w-full text-xs text-left">
                      {/* --- ĐOẠN CẦN SỬA LẠI Ở ĐÂY --- */}
                      <thead className="bg-slate-50 dark:bg-slate-900 sticky top-0 z-10">
                        <tr>
                          <th className="p-2 border-b font-medium text-slate-500">Log Event</th>
                          {/* BẠN ĐANG THIẾU DÒNG NÀY: */}
                          <th className="p-2 border-b font-medium text-slate-500">Display</th>
                          <th className="p-2 border-b font-medium text-slate-500 text-right">Cost</th>
                        </tr>
                      </thead>
                      <tbody>
                        {previewData.boosters.map((b: any, i: number) => (
                          <tr key={i} className="border-b last:border-0 hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors">
                            <td className="p-2 font-mono text-slate-600 dark:text-slate-300 truncate max-w-[120px]" title={b.event_name}>{b.event_name}</td>

                            {/* BẠN ĐANG THIẾU DÒNG NÀY: */}
                            <td className="p-2 text-slate-700 dark:text-slate-200 font-medium truncate max-w-[100px]">{b.display_name || '-'}</td>

                            <td className="p-2 text-right font-mono text-orange-600 font-bold">{b.coin_cost}</td>
                          </tr>
                        ))}
                      </tbody>
                      {/* -------------------------------- */}
                    </table>
                  </div>
                ) : (
                  <div className="text-xs text-slate-400 italic border border-dashed border-slate-300 rounded p-2 text-center">No boosters mapped yet.</div>
                )}
              </div>
            </div>
          </div>
        )}
        <div className="mt-8 flex justify-end gap-3">
          {/* Nút Advanced Analytics (MỚI) */}
          {!isAdding && selectedApp && (
            <button onClick={handleOpenAnalytics} className="bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-lg font-bold flex items-center gap-2 shadow-md shadow-indigo-200">
              <Database size={18} /> Advanced Analytics
            </button>
          )}

          <button onClick={handleSubmit} className="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg font-bold flex items-center gap-2">
            <Save size={18} /> {isAdding ? "Create Game" : "Save Changes"}
          </button>
        </div>
      </div>

      {/* 3. MODAL CONFIG ANALYTICS (MỚI - POPUP) */}
      {showAnalyticsConfig && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm animate-in fade-in">
          <div className="bg-white dark:bg-slate-800 w-full max-w-3xl rounded-xl shadow-2xl flex flex-col max-h-[90vh] animate-in zoom-in-95">
            <div className="p-4 border-b dark:border-slate-700 flex justify-between items-center bg-slate-50 dark:bg-slate-900 rounded-t-xl">
              <div className="flex items-center gap-3">
                <Database size={20} className="text-indigo-600" />
                <h3 className="text-lg font-bold">Event Mapping: {selectedApp?.name}</h3>
              </div>
              <button onClick={() => setShowAnalyticsConfig(false)} className="p-2 hover:bg-slate-200 dark:hover:bg-slate-700 rounded-full"><X size={20} /></button>
            </div>

            <div className="p-6 overflow-y-auto space-y-6">
              {/* Event Mapping Section */}
              <div className="bg-slate-50 dark:bg-slate-900 p-4 rounded-lg border border-slate-200 dark:border-slate-700">
                <h4 className="font-bold text-slate-700 dark:text-slate-300 mb-4 flex items-center gap-2"><Gamepad2 size={18} /> Level Events Definition</h4>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div>
                    <label className="text-xs font-semibold uppercase text-slate-500 mb-1 block">Start Level Event</label>
                    <input type="text" placeholder="e.g. level_start" className="w-full p-2 text-sm border rounded"
                      value={analyticsData.events.level_start}
                      onChange={e => setAnalyticsData({ ...analyticsData, events: { ...analyticsData.events, level_start: e.target.value } })}
                    />
                  </div>
                  <div>
                    <label className="text-xs font-semibold uppercase text-slate-500 mb-1 block">Win Level Event</label>
                    <input type="text" placeholder="e.g. mission_completed" className="w-full p-2 text-sm border rounded"
                      value={analyticsData.events.level_win}
                      onChange={e => setAnalyticsData({ ...analyticsData, events: { ...analyticsData.events, level_win: e.target.value } })}
                    />
                  </div>
                  <div>
                    <label className="text-xs font-semibold uppercase text-slate-500 mb-1 block">Fail Level Event</label>
                    <input type="text" placeholder="e.g. mission_failed" className="w-full p-2 text-sm border rounded"
                      value={analyticsData.events.level_fail}
                      onChange={e => setAnalyticsData({ ...analyticsData, events: { ...analyticsData.events, level_fail: e.target.value } })}
                    />
                  </div>
                </div>
                <p className="text-xs text-slate-400 mt-2 italic">* Nhập chính xác tên sự kiện (Raw Event Name) mà game bắn lên server.</p>
              </div>

              {/* Booster Config Section */}
              <div>
                <div className="flex justify-between items-center mb-3">
                  <h4 className="font-bold text-slate-700 dark:text-slate-300 flex items-center gap-2"><Coins size={18} /> Booster & Economy Config</h4>
                  <button onClick={addBooster} className="text-sm bg-blue-50 text-blue-600 px-3 py-1 rounded font-bold border border-blue-100 hover:bg-blue-100 flex items-center gap-1"><Plus size={14} /> Add Booster</button>
                </div>

                <div className="border rounded-lg overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-slate-100 dark:bg-slate-900 text-slate-500">
                      <tr>
                        <th className="p-3 text-left">Event Name (Log)</th>
                        <th className="p-3 text-left">Display Name</th>
                        <th className="p-3 text-left">Cost (Coin)</th>
                        <th className="p-3 w-10"></th>
                      </tr>
                    </thead>
                    <tbody className="divide-y">
                      {analyticsData.boosters?.map((b: any, idx: number) => (
                        <tr key={idx} className="bg-white dark:bg-slate-800">
                          <td className="p-2"><input type="text" className="w-full p-1 border rounded" placeholder="e.g. use_hammer" value={b.event_name} onChange={e => updateBooster(idx, 'event_name', e.target.value)} /></td>
                          <td className="p-2"><input type="text" className="w-full p-1 border rounded" placeholder="e.g. Hammer" value={b.display_name} onChange={e => updateBooster(idx, 'display_name', e.target.value)} /></td>
                          <td className="p-2"><input type="number" className="w-full p-1 border rounded" placeholder="500" value={b.coin_cost} onChange={e => updateBooster(idx, 'coin_cost', e.target.value)} /></td>
                          <td className="p-2 text-center"><button onClick={() => removeBooster(idx)} className="text-red-400 hover:text-red-600"><Trash2 size={16} /></button></td>
                        </tr>
                      ))}
                      {(!analyticsData.boosters || analyticsData.boosters.length === 0) && (
                        <tr><td colSpan={4} className="p-4 text-center text-slate-400 italic">No boosters configured. Click "Add Booster" to start.</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div className="p-4 border-t bg-slate-50 dark:bg-slate-900 rounded-b-xl flex justify-end gap-3">
              <button onClick={() => setShowAnalyticsConfig(false)} className="px-4 py-2 border rounded-lg font-medium hover:bg-slate-200">Cancel</button>
              <button onClick={handleSaveAnalytics} className="px-4 py-2 bg-indigo-600 text-white hover:bg-indigo-700 rounded-lg font-bold flex items-center gap-2"><Save size={18} /> Save Configuration</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// --- DASHBOARD VIEW (V2.0: BOOSTER ECONOMY & DATE FILTER) ---
function DashboardView({ selectedApp }: any) {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  // State lọc ngày (Mặc định rỗng = Lấy tất cả)
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');

  const fetchDashboard = () => {
    if (!selectedApp) return;
    setLoading(true);

    // Xây dựng URL với bộ lọc ngày
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);

    fetch(`${API_URL}/dashboard/${selectedApp.id}?${params.toString()}`)
      .then(res => res.json())
      .then(json => {
        if (json.success) setData(json);
      })
      .catch(err => console.error(err))
      .finally(() => setLoading(false));
  };

  // Tự động fetch khi đổi App hoặc đổi Ngày
  useEffect(() => {
    fetchDashboard();
  }, [selectedApp, startDate, endDate]);

  if (!data) return <div className="p-10 text-center text-slate-400">Loading Dashboard...</div>;

  const { overview } = data;
  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8'];

  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4">

      {/* 1. FILTER BAR & TITLE */}
      <div className="flex flex-col md:flex-row justify-between items-center gap-4 bg-white dark:bg-slate-800 p-4 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
        <div>
          <h3 className="font-bold text-lg flex items-center gap-2">
            <LayoutDashboard className="text-blue-600" />
            Strategic Overview
          </h3>
          <p className="text-xs text-slate-500">Business Metrics & Game Economy</p>
        </div>

        <div className="flex items-center gap-2 bg-slate-50 dark:bg-slate-900 p-1 rounded-lg border border-slate-200 dark:border-slate-700">
          <Calendar size={16} className="text-slate-400 ml-2" />
          <input
            type="date"
            className="bg-transparent border-none text-sm outline-none w-32 text-slate-600 dark:text-slate-300"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
          />
          <span className="text-slate-400">-</span>
          <input
            type="date"
            className="bg-transparent border-none text-sm outline-none w-32 text-slate-600 dark:text-slate-300"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
          />
          {(startDate || endDate) && (
            <button onClick={() => { setStartDate(''); setEndDate('') }} className="text-red-500 hover:bg-red-50 p-1 rounded-md transition-colors"><X size={14} /></button>
          )}
        </div>
      </div>

      {/* 2. KPI CARDS (UPDATED) */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* CARD 1: BOOSTER REVENUE */}
        <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm relative overflow-hidden group hover:shadow-md transition-all">
          <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity"><Coins size={64} /></div>
          <h4 className="text-slate-500 text-sm font-medium uppercase tracking-wider">Booster Revenue</h4>
          <div className="text-3xl font-bold text-emerald-600 mt-2 flex items-baseline gap-1">
            {overview.cards.revenue.toLocaleString()} <span className="text-sm font-normal text-slate-400">Coins</span>
          </div>
          <div className="mt-2 text-xs text-emerald-600 bg-emerald-50 w-fit px-2 py-1 rounded-full flex items-center gap-1">
            <TrendingUp size={12} /> Based on config price
          </div>
        </div>

        {/* CARD 2: ACTIVE USERS */}
        <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm relative overflow-hidden group hover:shadow-md transition-all">
          <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity"><Users size={64} /></div>
          <h4 className="text-slate-500 text-sm font-medium uppercase tracking-wider">Active Users</h4>
          <div className="text-3xl font-bold text-blue-600 mt-2">
            {overview.cards.active_users.toLocaleString()}
          </div>
          <div className="mt-2 text-xs text-blue-600 bg-blue-50 w-fit px-2 py-1 rounded-full">
            Unique UserIDs
          </div>
        </div>

        {/* CARD 3: TOTAL EVENTS */}
        <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm relative overflow-hidden group hover:shadow-md transition-all">
          <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity"><Activity size={64} /></div>
          <h4 className="text-slate-500 text-sm font-medium uppercase tracking-wider">Total Data Points</h4>
          <div className="text-3xl font-bold text-purple-600 mt-2">
            {overview.cards.total_events.toLocaleString()}
          </div>
          <div className="mt-2 text-xs text-purple-600 bg-purple-50 w-fit px-2 py-1 rounded-full">
            Raw Events Processed
          </div>
        </div>
      </div>

      {/* 3. CHARTS ROW */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* CHART 1: TOP BOOSTER ECONOMY (NEW!) */}
        <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
          <h4 className="font-bold text-slate-700 dark:text-slate-200 mb-4 flex items-center gap-2">
            <Zap className="text-amber-500" size={18} /> Top Selling Boosters (By Revenue)
          </h4>
          <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={overview.booster_chart} layout="vertical" margin={{ top: 5, right: 30, left: 40, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
                <XAxis type="number" hide />
                <YAxis dataKey="name" type="category" width={100} tick={{ fontSize: 12 }} />
                <RechartsTooltip
                  contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                  formatter={(value: any) => [`${value.toLocaleString()} Coins`, 'Revenue']}
                />
                <Bar dataKey="total_spent" fill="#f59e0b" radius={[0, 4, 4, 0]} barSize={20}>
                  {overview.booster_chart.map((entry: any, index: number) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* CHART 2: EVENT FREQUENCY (EXISTING) */}
        <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
          <h4 className="font-bold text-slate-700 dark:text-slate-200 mb-4 flex items-center gap-2">
            <BarChart3 className="text-blue-500" size={18} /> Event Frequency Overview
          </h4>
          <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={overview.chart_main} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} interval={0} angle={-15} textAnchor="end" height={60} />
                <YAxis tick={{ fontSize: 12 }} />
                <RechartsTooltip contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }} />
                <Bar dataKey="value" fill="#3b82f6" radius={[4, 4, 0, 0]} barSize={40} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

      </div>
    </div>
  );
}

function MonitorView({ selectedApp }: any) {
  const [history, setHistory] = useState<any[]>([]);
  const [selectedJob, setSelectedJob] = useState<any>(null);
  const [filterStartDate, setFilterStartDate] = useState('');
  const [filterEndDate, setFilterEndDate] = useState('');

  // State phân trang
  const [pagination, setPagination] = useState({
    current_page: 1,
    total_pages: 1,
    total_records: 0
  });

  // Hàm "ép" kiểu ngày tháng an toàn
  const parseSafeDate = (dateStr: any) => {
    if (!dateStr) return null;
    let d = new Date(dateStr);
    if (!isNaN(d.getTime())) return d;
    if (typeof dateStr === 'string') {
      const parts = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s*(\d{0,2}):?(\d{0,2}):?(\d{0,2})/);
      if (parts) {
        return new Date(
          parseInt(parts[3]), parseInt(parts[2]) - 1, parseInt(parts[1]),
          parseInt(parts[4]) || 0, parseInt(parts[5]) || 0, parseInt(parts[6]) || 0
        );
      }
    }
    return null;
  };

  const fetchHistory = (page = 1) => {
    if (!selectedApp) return;

    const params = new URLSearchParams({
      app_id: selectedApp.id,
      page: page.toString(),
      limit: '30'
    });

    if (filterStartDate) params.append('start_date', filterStartDate);
    if (filterEndDate) params.append('end_date', filterEndDate);

    fetch(`${API_URL}/monitor/history?${params.toString()}`)
      .then(res => res.json())
      .then(data => {
        if (data.data && Array.isArray(data.data)) {
          setHistory(data.data);
          if (data.pagination) setPagination(data.pagination);

          if (selectedJob) {
            const updatedJob = data.data.find((job: any) => job.id === selectedJob.id);
            if (updatedJob) setSelectedJob(updatedJob);
          }
        } else if (Array.isArray(data)) {
          setHistory(data);
        }
      })
      .catch(err => console.error(err));
  };

  useEffect(() => {
    fetchHistory(1);
    const interval = setInterval(() => {
      setPagination(prev => {
        fetchHistory(prev.current_page);
        return prev;
      });
    }, 3000);
    return () => clearInterval(interval);
  }, [selectedApp, filterStartDate, filterEndDate]);

  const handlePageChange = (newPage: number) => {
    if (newPage >= 1 && newPage <= pagination.total_pages) {
      fetchHistory(newPage);
    }
  };

  const handleRun = async (type: string, retryJobId?: number) => {
    if (!selectedApp) return;
    const body: any = { run_type: type };
    if (retryJobId) body.retry_job_id = retryJobId;
    try {
      await fetch(`${API_URL}/etl/run/${selectedApp.id}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      fetchHistory(pagination.current_page);
    } catch (e) { alert("Error trigger job"); }
  };

  const handleStop = async (jobId: number) => {
    if (!confirm(`Stop Job #${jobId}?`)) return;
    await fetch(`${API_URL}/etl/stop/${jobId}`, { method: 'POST' });
    fetchHistory(pagination.current_page);
  };

  const handleDeleteAll = async () => {
    if (!confirm("DELETE ALL HISTORY?")) return;
    await fetch(`${API_URL}/monitor/purge?app_id=${selectedApp.id}`, { method: 'DELETE' });
    fetchHistory(1);
  }

  const handleDeleteSingle = async (e: React.MouseEvent, jobId: number) => {
    e.stopPropagation();
    if (!confirm(`Delete record #${jobId}?`)) return;
    await fetch(`${API_URL}/monitor/history/${jobId}`, { method: 'DELETE' });
    fetchHistory(pagination.current_page);
  };

  const isRunningState = (status: string) => ['Running', 'Processing'].includes(status);

  const formatRawTime = (dateStr: any) => {
    const date = parseSafeDate(dateStr);
    if (!date) return String(dateStr);
    return date.toLocaleString('en-GB', { hour12: false });
  };

  const calculateDuration = (startStr: any, endStr: any) => {
    const startDate = parseSafeDate(startStr);
    const endDate = parseSafeDate(endStr);
    if (!startDate) return '-';
    const endMs = endDate ? endDate.getTime() : (!endStr ? Date.now() : null);
    if (!endMs) return '-';
    const diffMs = endMs - startDate.getTime();
    const diffSec = Math.floor(diffMs / 1000);
    if (diffSec < 0) return '0s';
    const hours = Math.floor(diffSec / 3600);
    const minutes = Math.floor((diffSec % 3600) / 60);
    const seconds = diffSec % 60;
    const parts = [];
    if (hours > 0) parts.push(`${hours}h`);
    if (minutes > 0) parts.push(`${minutes}m`);
    parts.push(`${seconds}s`);
    return parts.join(' ');
  };

  const getRunTypeBadge = (type: string) => {
    let colorClass = "bg-slate-100 text-slate-600";
    switch (type?.toLowerCase()) {
      case 'manual': colorClass = "bg-blue-100 text-blue-700"; break;
      case 'schedule': colorClass = "bg-purple-100 text-purple-700"; break;
      case 'retry': colorClass = "bg-orange-100 text-orange-700"; break;
      case 'demo': colorClass = "bg-yellow-100 text-yellow-700"; break;
      default: colorClass = "bg-slate-100 text-slate-600";
    }
    return <span className={`px-2 py-1 rounded-md text-xs font-bold uppercase tracking-wider ${colorClass}`}>{type}</span>;
  };

  return (
    <div className="space-y-6 relative">
      <div className="bg-white dark:bg-slate-800 p-4 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div className="flex flex-col sm:flex-row sm:items-center gap-4 w-full md:w-auto">
          <h3 className="font-bold text-lg whitespace-nowrap">Job History Monitor</h3>
          <div className="flex items-center gap-2 bg-slate-50 dark:bg-slate-900 px-2 py-1 rounded-lg border border-slate-200 dark:border-slate-700">
            <Filter size={16} className="text-slate-400" />
            <input type="date" className="text-sm bg-transparent border-none outline-none text-slate-600 dark:text-slate-300 focus:ring-0 w-32" value={filterStartDate} onChange={(e) => setFilterStartDate(e.target.value)} />
            <span className="text-slate-400">-</span>
            <input type="date" className="text-sm bg-transparent border-none outline-none text-slate-600 dark:text-slate-300 focus:ring-0 w-32" value={filterEndDate} onChange={(e) => setFilterEndDate(e.target.value)} />
            {(filterStartDate || filterEndDate) && (<button onClick={() => { setFilterStartDate(''); setFilterEndDate('') }} className="text-red-500 hover:text-red-700 px-1 font-bold">✕</button>)}
          </div>
        </div>
        <div className="flex gap-2 w-full md:w-auto justify-end">
          <button onClick={() => handleRun('demo')} className="bg-yellow-500 hover:bg-yellow-600 text-white px-4 py-2 rounded-lg font-bold flex items-center gap-2 text-sm whitespace-nowrap"><Play size={16} /> Test Demo</button>
          <button onClick={() => handleRun('manual')} className="bg-emerald-600 hover:bg-emerald-700 text-white px-4 py-2 rounded-lg font-bold flex items-center gap-2 text-sm whitespace-nowrap"><RotateCcw size={16} /> Run ETL Now</button>
          <button onClick={handleDeleteAll} className="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg font-bold flex items-center gap-2 text-sm whitespace-nowrap"><Trash2 size={16} /> Delete All</button>
        </div>
      </div>

      <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-col">
        <div className="overflow-y-auto max-h-[600px] rounded-xl scroll-smooth custom-scrollbar">
          <table className="w-full text-left text-sm relative border-collapse">
            <thead className="bg-slate-100 dark:bg-slate-900 sticky top-0 z-10 shadow-sm">
              <tr>
                <th className="px-6 py-3">ID</th>
                <th className="px-6 py-3">Type</th>
                <th className="px-6 py-3">Start Time</th>
                <th className="px-6 py-3">End Time</th>
                <th className="px-6 py-3">Duration</th>
                <th className="px-6 py-3">Status</th>
                <th className="px-6 py-3 text-center">Events</th>
                <th className="px-6 py-3 text-right">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-700">
              {/* [FIX 2] Đổi History (viết hoa) thành history (viết thường) */}
              {history.length === 0 ? (
                <tr><td colSpan={8} className="p-10 text-center text-slate-400">No history available.</td></tr>
              ) : history.map((job: any) => (
                <tr key={job.id} className="hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors">
                  <td className="px-6 py-3 font-mono text-slate-500">#{job.id}</td>
                  <td className="px-6 py-3">{getRunTypeBadge(job.run_type)}</td>
                  <td className="px-6 py-3 font-mono text-slate-600 dark:text-slate-300 whitespace-nowrap">{formatRawTime(job.start_time)}</td>
                  <td className="px-6 py-3 font-mono text-slate-600 dark:text-slate-300 whitespace-nowrap">
                    {job.end_time ? formatRawTime(job.end_time) : <span className="text-blue-500 italic">Running...</span>}
                  </td>
                  <td className="px-6 py-3 font-mono text-slate-800 dark:text-slate-200 font-medium">
                    {calculateDuration(job.start_time, job.end_time)}
                  </td>
                  <td className="px-6 py-3"><span className={`px-2 py-1 rounded-full text-xs font-bold flex w-fit items-center gap-1 ${job.status === 'Success' ? 'bg-emerald-100 text-emerald-700' : isRunningState(job.status) ? 'bg-blue-100 text-blue-700 animate-pulse' : job.status === 'Cancelled' ? 'bg-slate-200 text-slate-700' : 'bg-red-100 text-red-700'}`}>{isRunningState(job.status) && <RefreshCw size={12} className="animate-spin" />}{job.status}</span></td>
                  <td className="px-6 py-3 font-bold text-center text-blue-600">{job.total_events}</td>
                  <td className="px-6 py-3 text-right">
                    <div className="flex justify-end gap-2">
                      {isRunningState(job.status) && (
                        <button onClick={() => handleStop(job.id)} className="p-2 text-red-500 hover:bg-red-50 rounded-lg border border-red-200 hover:border-red-400 transition-all flex items-center gap-1" title="Stop Job">
                          <StopCircle size={16} className="animate-pulse" /> STOP
                        </button>
                      )}
                      {['Failed', 'Cancelled', 'Success', 'Skipped'].includes(job.status) && (
                        <button onClick={() => handleRun('retry', job.id)} className="p-2 text-blue-600 hover:bg-blue-50 rounded-lg flex items-center gap-1 transition-all border border-transparent hover:border-blue-200" title="Retry">
                          <RotateCcw size={16} /> {job.status === 'Cancelled' ? 'Resume' : 'Retry'}
                        </button>
                      )}
                      <button onClick={() => setSelectedJob(job)} className="p-2 text-blue-600 hover:bg-blue-50 rounded-lg flex items-center gap-1 transition-all border border-transparent hover:border-blue-200" title="View Logs Detail"><FileText size={16} /></button>
                      <button onClick={(e) => handleDeleteSingle(e, job.id)} className="p-2 text-slate-400 hover:text-red-600 hover:bg-slate-100 rounded-lg transition-all" title="Delete Record"><Trash2 size={16} /></button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="p-3 border-t dark:border-slate-700 bg-slate-50 dark:bg-slate-900 rounded-b-xl flex flex-col sm:flex-row justify-between items-center gap-4">
          <div className="flex items-center gap-4 text-xs text-slate-500">
            <span>Showing <b>{history.length}</b> jobs</span>
            <span className="hidden sm:inline">|</span>
            <span>Total: <b>{pagination.total_records.toLocaleString()}</b> jobs</span>
          </div>

          <div className="flex items-center gap-1 bg-white dark:bg-slate-800 p-1 rounded-lg border border-slate-200 dark:border-slate-700 shadow-sm">
            <button
              onClick={() => handlePageChange(1)}
              disabled={pagination.current_page <= 1}
              className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed text-slate-600 dark:text-slate-300"
            >
              <ChevronsLeft size={16} />
            </button>
            <button
              onClick={() => handlePageChange(pagination.current_page - 1)}
              disabled={pagination.current_page <= 1}
              className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed text-slate-600 dark:text-slate-300 border-r dark:border-slate-700 mr-2"
            >
              <ChevronLeft size={16} />
            </button>

            <span className="text-xs font-bold px-3 text-slate-700 dark:text-slate-200 min-w-[80px] text-center">
              Page {pagination.current_page} / {pagination.total_pages}
            </span>

            <button
              onClick={() => handlePageChange(pagination.current_page + 1)}
              disabled={pagination.current_page >= pagination.total_pages}
              className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed text-slate-600 dark:text-slate-300 border-l dark:border-slate-700 ml-2"
            >
              <ChevronRight size={16} />
            </button>
            <button
              onClick={() => handlePageChange(pagination.total_pages)}
              disabled={pagination.current_page >= pagination.total_pages}
              className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed text-slate-600 dark:text-slate-300"
            >
              <ChevronsRight size={16} />
            </button>
          </div>
        </div>
      </div>

      {/* [FIX 3] Đặt Modal vào TRONG thẻ div cha, không để rơi ra ngoài */}
      {selectedJob && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm animate-in fade-in">
          <div className="bg-white dark:bg-slate-800 w-full max-w-4xl rounded-xl shadow-2xl flex flex-col max-h-[90vh] animate-in zoom-in-95">
            <div className="p-4 border-b dark:border-slate-700 flex justify-between items-center bg-slate-50 dark:bg-slate-900 rounded-t-xl">
              <div className="flex items-center gap-3"><FileText size={20} className="text-blue-600" /><div><h3 className="text-lg font-bold">Log Details #{selectedJob.id}</h3><p className="text-xs text-slate-500">Started: {formatRawTime(selectedJob.start_time)}</p></div></div>
              <button onClick={() => setSelectedJob(null)} className="p-2 hover:bg-slate-200 dark:hover:bg-slate-700 rounded-full transition-colors"><X size={20} /></button>
            </div>
            <div className="p-6 overflow-y-auto bg-slate-900 text-green-400 font-mono text-xs whitespace-pre-wrap flex-1 min-h-[400px] shadow-inner">{selectedJob.logs || "No logs recorded."}</div>
            <div className="p-4 border-t dark:border-slate-700 bg-slate-50 dark:bg-slate-900 rounded-b-xl flex justify-end gap-3">
              {isRunningState(selectedJob.status) && (<button onClick={() => handleStop(selectedJob.id)} className="px-4 py-2 bg-red-100 text-red-600 hover:bg-red-200 rounded-lg font-bold flex items-center gap-2"><StopCircle size={18} /> Stop</button>)}
              <button onClick={() => handleRun('retry', selectedJob.id)} className="px-4 py-2 bg-blue-100 text-blue-600 hover:bg-blue-200 rounded-lg font-bold flex items-center gap-2"><RotateCcw size={18} /> Retry</button>
              <button onClick={() => setSelectedJob(null)} className="px-4 py-2 border rounded-lg font-medium hover:bg-slate-100 dark:hover:bg-slate-700">Close</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}