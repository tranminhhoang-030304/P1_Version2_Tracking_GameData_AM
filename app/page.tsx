'use client';
import React, { useState, useEffect } from 'react';
import LevelDetailTab from './components/LevelDetailTab';
import DataExplorer from './components/DataExplorer';
import {
  LayoutDashboard, Settings, Activity, Server,
  Play, CheckCircle, Save, Plus, BarChart3, List,
  Calendar, Clock, AlertCircle, X, RotateCcw, FileText, Trash2, StopCircle, RefreshCw,
  Bot, Zap, FlaskConical, Filter, PieChart as PieIcon, Coins, Gamepad2, Database,
  ChevronsLeft, ChevronsRight, ChevronLeft, ChevronRight, TrendingUp, Users, ChevronDown, Banknote
} from 'lucide-react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, ResponsiveContainer, BarChart, Bar,
  PieChart, Pie, Cell, Legend, Area, ComposedChart,
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
          selectedApp ? (
            // [FIX]: Chỉ cần truyền selectedApp, để api con tự lo việc fetch data
            <DashboardView selectedApp={selectedApp} />
          ) : (
            <div className="flex h-full items-center justify-center text-slate-400 flex-col gap-4">
              <BarChart3 size={48} className="opacity-20" />
              Please select a game in Settings first.
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

function DashboardView({ selectedApp }: any) {
  const [activeSubTab, setActiveSubTab] = useState<'overview' | 'level'>('overview');

  const [data, setData] = useState<any>(null);
  const [strategicData, setStrategicData] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');

  // State lọc Level (Có Fallback array)
  const [levels, setLevels] = useState<string[]>([]);
  const [selectedOverviewLevel, setSelectedOverviewLevel] = useState<string>("");

  // 1. Fetch Levels (Xóa Fallback)
  useEffect(() => {
    if (selectedApp) {
      fetch(`${API_URL}/api/levels/${selectedApp.id}`)
        .then(res => res.json())
        .then(arr => {
          if (Array.isArray(arr) && arr.length > 0) {
            setLevels(arr);
          } else {
            setLevels([]); // Không bịa ra level 1,2,3,4,5 nữa
          }
        })
        .catch(() => setLevels([]));
    }
  }, [selectedApp]);

  // 2. Fetch Dashboard Overview
  const fetchDashboard = () => {
    if (!selectedApp) return;
    setLoading(true);
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    if (selectedOverviewLevel) params.append('level_id', selectedOverviewLevel);

    fetch(`${API_URL}/dashboard/${selectedApp.id}?${params.toString()}`)
      .then(res => res.json())
      .then(json => { if (json.success) setData(json); })
      .finally(() => setLoading(false));
  };

  // 3. Fetch Strategic Chart
  useEffect(() => {
    if (selectedApp) {
      fetch(`${API_URL}/dashboard/${selectedApp.id}/strategic`)
        .then(res => res.json())
        .then(data => { if (data.success) setStrategicData(data.balance_chart); });
    }
  }, [selectedApp]);

  useEffect(() => {
    if (activeSubTab === 'overview') fetchDashboard();
  }, [selectedApp, startDate, endDate, selectedOverviewLevel, activeSubTab]);

  // [MỚI] Hàm tính Avg Fail Rate từ biểu đồ để hiển thị lên KPI Card
  const calculateAvgFailRate = () => {
    if (!strategicData || strategicData.length === 0) return 0;
    // Nếu đang lọc level cụ thể
    if (selectedOverviewLevel) {
      const lvl = strategicData.find((d: any) => d.name === `Lv.${selectedOverviewLevel}`);
      return lvl ? lvl.fail_rate : 0;
    }
    // Nếu không lọc, tính trung bình
    const total = strategicData.reduce((acc: any, curr: any) => acc + curr.fail_rate, 0);
    return (total / strategicData.length).toFixed(1);
  }

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8'];

  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4">
      {/* TAB NAVIGATION */}
      <div className="flex items-center gap-6 border-b border-slate-200 dark:border-slate-700 mb-2">
        <button onClick={() => setActiveSubTab('overview')} className={`pb-3 text-sm font-bold border-b-2 transition-all flex items-center gap-2 ${activeSubTab === 'overview' ? 'border-blue-600 text-blue-600' : 'border-transparent text-slate-500 hover:text-slate-700'}`}>
          <LayoutDashboard size={16} /> Strategic Overview
        </button>
        <button onClick={() => setActiveSubTab('level')} className={`pb-3 text-sm font-bold border-b-2 transition-all flex items-center gap-2 ${activeSubTab === 'level' ? 'border-blue-600 text-blue-600' : 'border-transparent text-slate-500 hover:text-slate-700'}`}>
          <Gamepad2 size={16} /> Level Inspector (Deep Dive)
        </button>
      </div>

      {activeSubTab === 'overview' ? (
        !data ? <div className="p-10 text-center text-slate-400 flex flex-col items-center gap-2"><RefreshCw className="animate-spin" /> Loading Overview...</div> : (
          <>
            {/* [FIX] HEADER & FILTER BAR MỚI - ĐỒNG BỘ VỚI TAB KIA */}
            <div className="bg-white dark:bg-slate-800 p-4 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-col md:flex-row md:items-center justify-between gap-4">
              <div className="flex flex-wrap items-center gap-4">
                {/* 1. LEVEL FILTER (STYLE: CLEAN DROPDOWN) */}
                <div className="relative group">
                  <div className="absolute left-3 top-1/2 -translate-y-1/2 text-blue-600"><Filter size={18} /></div>
                  <select
                    value={selectedOverviewLevel}
                    onChange={(e) => setSelectedOverviewLevel(e.target.value)}
                    className="appearance-none bg-slate-50 border border-slate-200 text-slate-700 py-2 pl-10 pr-10 rounded-lg font-bold outline-none cursor-pointer min-w-[180px] hover:bg-slate-100 transition-colors"
                  >
                    {levels.map(lvl => (
                      <option key={lvl} value={lvl}>
                        {lvl === '0' ? '🏠 Lobby / Tutorial' : `Level ${lvl}`}
                      </option>
                    ))}
                  </select>
                  <ChevronDown size={16} className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
                </div>

                <div className="h-6 w-px bg-slate-200 hidden md:block"></div>

                {/* 2. DATE FILTER (STYLE: TIME RANGE BOX - GIỐNG INSPECTOR) */}
                <div className="flex items-center gap-2">
                  <div className="flex items-center gap-2 bg-slate-100 text-slate-600 px-3 py-2 rounded-lg border border-slate-200">
                    <Calendar size={18} />
                    <span className="font-bold text-sm">Time Range</span>
                  </div>
                  <input
                    type="date"
                    className="bg-white border rounded-lg px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-blue-500"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                  />
                  <span className="text-slate-400 font-bold">-</span>
                  <input
                    type="date"
                    className="bg-white border rounded-lg px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-blue-500"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                  />
                  {(startDate || endDate) && (
                    <button onClick={() => { setStartDate(''); setEndDate('') }} className="text-red-500 hover:bg-red-50 p-2 rounded-full transition-colors">✕</button>
                  )}
                </div>
              </div>

              {loading && <span className="text-sm text-blue-500 font-medium animate-pulse flex items-center gap-2">Syncing...</span>}
            </div>

            {/* KPI CARDS - LAYOUT 4 THẺ */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">

              {/* 1. IAP Revenue */}
              <StatCard
                title="IAP Revenue"
                value={`${(data?.overview?.cards?.revenue || 0).toLocaleString()} currency`}
                icon={Banknote}
                color="bg-emerald-100 text-emerald-600"
              />

              {/* 2. Total Plays (Đã fix lỗi crash active_users) */}
              <StatCard
                title="Total Plays"
                value={`${(data?.overview?.cards?.active_users || 0).toLocaleString()} sessions`}
                icon={Users}
                color="bg-blue-100 text-blue-600"
              />

              {/* 3. COIN SINK (Tiền ảo) */}
              <StatCard
                title="Coins Spent" // Đổi tên: Tiêu Coin
                value={`${(data?.overview?.cards?.total_spent || 0).toLocaleString()} coin`}
                icon={Coins} // Icon tiền xu vàng
                color="bg-amber-100 text-amber-600" // Màu vàng cho Coin
              />

              {/* 4. Avg Fail Rate (Ưu tiên lấy từ Backend cho chính xác) */}
              <StatCard
                title="Avg Fail Rate"
                value={`${data?.overview?.cards?.avg_fail_rate || 0} %`}
                icon={Activity}
                color="bg-red-100 text-red-600"
              />

            </div>

            {/* CHARTS */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 shadow-sm">
                <div className="flex justify-between items-center mb-6">
                  <div><h4 className="font-bold text-lg text-slate-800 flex items-center gap-2"><Activity className="text-red-500" size={20} /> Game Balance</h4><p className="text-xs text-slate-500">Fail Rate vs Revenue Correlation</p></div>
                  <div className="flex gap-3 text-[10px] font-bold uppercase"><div className="flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-amber-400"></div> Revenue</div><div className="flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-red-500"></div> Fail Rate</div></div>
                </div>
                <div className="h-[350px] w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={strategicData}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                      <XAxis dataKey="name" tick={{ fontSize: 10 }} height={50} angle={-30} textAnchor="end" />
                      <YAxis yAxisId="left" orientation="left" stroke="#d97706" tick={{ fontSize: 10 }} tickFormatter={(val) => val >= 1000 ? `${val / 1000}k` : val} />
                      <YAxis yAxisId="right" orientation="right" stroke="#ef4444" domain={[0, 100]} tick={{ fontSize: 10 }} unit="%" />
                      <RechartsTooltip contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }} />
                      <Bar yAxisId="left" dataKey="revenue" fill="#fbbf24" barSize={12} radius={[4, 4, 0, 0]} name="Revenue" />
                      <Line yAxisId="right" type="monotone" dataKey="fail_rate" stroke="#ef4444" strokeWidth={2} dot={{ r: 2 }} name="Fail Rate" />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="bg-white dark:bg-slate-800 p-6 rounded-xl border border-slate-200 shadow-sm">
                <h4 className="font-bold text-slate-700 mb-2 flex items-center gap-2"><BarChart3 className="text-blue-500" size={18} /> Full Event Distribution</h4>
                <p className="text-xs text-slate-500 mb-4">Frequency of all recorded events</p>
                <div className="h-[350px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={data.overview.chart_main} layout="vertical" margin={{ left: 20 }}>
                      <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                      <XAxis type="number" tick={{ fontSize: 10 }} />
                      <YAxis dataKey="name" type="category" width={110} tick={{ fontSize: 10 }} interval={0} />
                      <RechartsTooltip cursor={{ fill: 'transparent' }} contentStyle={{ borderRadius: '8px' }} />
                      <Bar dataKey="value" fill="#3b82f6" radius={[0, 4, 4, 0]} barSize={12} name="Count" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          </>
        )
      ) : (
        <LevelDetailTab appId={selectedApp.id} />
      )}
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