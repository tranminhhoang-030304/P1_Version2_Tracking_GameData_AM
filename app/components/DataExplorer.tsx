'use client';

import React, { useState, useEffect } from 'react';
import {
    Search, Calendar, FileJson, ChevronLeft, ChevronRight,
    X, Copy, Check, Filter, Terminal, ChevronsLeft, ChevronsRight, Layers,
    UserMinus, AlertCircle, ClipboardList, Activity 
} from 'lucide-react';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://127.0.0.1:8080';
interface DataExplorerProps {
    appId: number;
}

export default function DataExplorer({ appId }: DataExplorerProps) {
    const getDefaultRange = (days = 1) => {
        const end = new Date();
        const start = new Date();
        start.setDate(end.getDate() - days);
        const formatLocal = (d: Date) => {
            const year = d.getFullYear();
            const month = String(d.getMonth() + 1).padStart(2, '0');
            const day = String(d.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        };
        return {
            start: formatLocal(start),
            end: formatLocal(end)
        };
    };

    const [dates, setDates] = useState(getDefaultRange(1));
    const [logs, setLogs] = useState<any[]>([]);
    const [loading, setLoading] = useState(false);
    
    const [levelList, setLevelList] = useState<string[]>([]);
    const [selectedLevel, setSelectedLevel] = useState<string>("");

    const [pagination, setPagination] = useState({ page: 1, total_pages: 1, total_records: 0 });

    const [filters, setFilters] = useState({
        keyword: '',
        event_name: '',
        start_date: '',
        end_date: ''
    });

    const [selectedLog, setSelectedLog] = useState<any>(null);

    const [dropModal, setDropModal] = useState({
        isOpen: false,
        level: "",
        loading: false,
        result: null as any
    });

    useEffect(() => {
        const fetchLevels = async () => {
            if (!appId) return;
            try {
                const res = await fetch(`${API_URL}/api/levels/${appId}`);
                const json = await res.json();
                if (Array.isArray(json)) {
                    setLevelList(json);
                }
            } catch (e) {
                console.error("Error loading levels", e);
            }
        };

        fetchLevels();
        setSelectedLevel(""); 
        setFilters(prev => ({ ...prev, keyword: '', event_name: '' })); 
    }, [appId]);

    // --- [SỬA LẠI] Cập nhật hàm fetchLogs để nhận tham số ghi đè (override) ---
    // Giúp tìm kiếm ngay lập tức khi click vào UUID mà không phải đợi React cập nhật State
    const fetchLogs = async (page = 1, customOverrides?: { keyword?: string, event_name?: string, level?: string }) => {
        setLoading(true);
        try {
            const params = new URLSearchParams({
                app_id: appId.toString(),
                page: page.toString(),
                limit: '50',
            });

            // Ưu tiên dùng tham số truyền vào, nếu không có thì dùng state hiện tại
            const kw = customOverrides?.keyword !== undefined ? customOverrides.keyword : filters.keyword;
            const ev = customOverrides?.event_name !== undefined ? customOverrides.event_name : filters.event_name;
            const lvl = customOverrides?.level !== undefined ? customOverrides.level : selectedLevel;

            if (kw) params.append('keyword', kw);
            if (ev) params.append('event_name', ev);
            
            const sDate = filters.start_date || dates.start;
            const eDate = filters.end_date || dates.end;
            
            if (sDate) params.append('start_date', sDate);
            if (eDate) params.append('end_date', eDate);

            if (lvl) params.append('level', lvl);

            const res = await fetch(`${API_URL}/events/search?${params.toString()}`);
            const json = await res.json();

            if (json.success) {
                setLogs(json.data);
                setPagination({
                    page: json.pagination.current_page,
                    total_pages: json.pagination.total_pages,
                    total_records: json.pagination.total_records
                });
            }
        } catch (error) {
            console.error("Lỗi tìm kiếm:", error);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchLogs(1);
    }, [appId]); 

    const handlePageChange = (newPage: number) => {
        if (newPage >= 1 && newPage <= pagination.total_pages) {
            fetchLogs(newPage);
        }
    };

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (filters.start_date) setDates(prev => ({...prev, start: filters.start_date}));
        if (filters.end_date) setDates(prev => ({...prev, end: filters.end_date}));
        fetchLogs(1);
    };

    const handleCheckDroppedUsers = async () => {
        if (!dropModal.level) {
            alert("Nhập Level!");
            return;
        }

        setDropModal(prev => ({ ...prev, loading: true, result: null }));
        try {
            const sDate = filters.start_date || dates.start;
            const eDate = filters.end_date || dates.end;
            const url = `${API_URL}/api/dropped-users/${appId}?level=${dropModal.level}&start_date=${sDate}&end_date=${eDate}`;

            const res = await fetch(url);
            const json = await res.json();

            if (json.success) {
                setDropModal(prev => ({ ...prev, loading: false, result: json }));
            } else {
                alert("Lỗi từ server: " + json.error);
                setDropModal(prev => ({ ...prev, loading: false }));
            }
        } catch (error) {
            console.error("Lỗi gọi API Dropped Users:", error);
            alert("Lỗi kết nối đến Backend!");
            setDropModal(prev => ({ ...prev, loading: false }));
        }
    };

    // --- [NEW] HÀM XỬ LÝ KHI CLICK VÀO UUID ---
    const handleUuidClick = (uid: string) => {
        // 1. Đóng modal tìm kiếm
        setDropModal(prev => ({ ...prev, isOpen: false }));
        
        // 2. Cập nhật state UI để người dùng nhìn thấy ô tìm kiếm đã thay đổi
        setFilters(prev => ({ ...prev, keyword: uid, event_name: '' }));
        setSelectedLevel(''); // Xoá filter level để xem toàn bộ hành trình
        
        // 3. Gọi fetchLogs ngay lập tức với custom param (bỏ qua độ trễ của React State)
        fetchLogs(1, { keyword: uid, event_name: '', level: '' });
    };

    return (
        <div className="space-y-4 animate-in fade-in duration-500 h-full flex flex-col">
            
            <form onSubmit={handleSearch} className="bg-white dark:bg-slate-800 p-4 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-wrap gap-4 items-end">
                <div className="flex-1 min-w-[180px]">
                    <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">Keyword / User ID</label>
                    <div className="relative">
                        <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                        <input
                            type="text"
                            placeholder="Search Context..."
                            className="w-full pl-9 pr-4 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                            value={filters.keyword}
                            onChange={e => setFilters({ ...filters, keyword: e.target.value })}
                        />
                    </div>
                </div>

                <div className="w-[160px]">
                    <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">Event Name</label>
                    <input
                        type="text"
                        placeholder="e.g. level_win"
                        className="w-full px-3 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                        value={filters.event_name}
                        onChange={e => setFilters({ ...filters, event_name: e.target.value })}
                    />
                </div>

                <div className="w-[140px]">
                    <label className="text-xs font-bold text-slate-500 uppercase mb-1 block flex items-center gap-1">
                        <Layers size={12}/> Level Filter
                    </label>
                    <select 
                        className="w-full px-3 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none cursor-pointer"
                        value={selectedLevel}
                        onChange={(e) => setSelectedLevel(e.target.value)}
                    >
                        <option value="">All Levels</option>
                        {levelList.map(lvl => (
                            <option key={lvl} value={lvl}>Level {lvl}</option>
                        ))}
                    </select>
                </div>

                <div className="flex items-center gap-2">
                    <div>
                        <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">From</label>
                        <input type="date" className="px-3 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg text-sm outline-none"
                            value={filters.start_date || dates.start} 
                            onChange={e => setFilters({ ...filters, start_date: e.target.value })} />
                    </div>
                    <span className="mb-2 text-slate-400">-</span>
                    <div>
                        <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">To</label>
                        <input type="date" className="px-3 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg text-sm outline-none"
                            value={filters.end_date || dates.end} 
                            onChange={e => setFilters({ ...filters, end_date: e.target.value })} />
                    </div>
                </div>

                <div className="flex items-center gap-2">
                    <button type="submit" className="bg-blue-600 hover:bg-blue-700 text-white px-5 py-2 rounded-lg font-bold text-sm flex items-center gap-2 h-[38px] transition-all shadow-md hover:shadow-lg active:scale-95">
                        {loading ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" /> : <Search size={16} />}
                        Search
                    </button>
                    
                    <button 
                        type="button" 
                        onClick={() => setDropModal({ isOpen: true, level: selectedLevel, loading: false, result: null })}
                        className="bg-orange-500 hover:bg-orange-600 text-white px-4 py-2 rounded-lg font-bold text-sm flex items-center gap-2 h-[38px] transition-all shadow-md hover:shadow-lg active:scale-95"
                        title="Tìm danh sách User Start nhưng không Win"
                    >
                        <UserMinus size={16} />
                        Tìm DropUser
                    </button>

                    {(filters.keyword || filters.event_name || selectedLevel) && (
                        <button type="button" onClick={() => { 
                            setFilters({ keyword: '', event_name: '', start_date: '', end_date: '' }); 
                            setSelectedLevel(''); 
                            setTimeout(() => fetchLogs(1), 50); 
                        }} className="text-red-500 hover:bg-red-50 px-3 py-2 rounded-lg text-sm font-bold h-[38px] border border-transparent hover:border-red-100 transition-all">
                            Clear
                        </button>
                    )}
                </div>
            </form>

            <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex-1 flex flex-col min-h-0">
                <div className="overflow-auto flex-1 custom-scrollbar">
                    <table className="w-full text-sm text-left border-collapse">
                        <thead className="bg-slate-100 dark:bg-slate-900 text-slate-500 sticky top-0 z-10 shadow-sm">
                            <tr>
                                <th className="p-3 w-20">ID</th>
                                <th className="p-3 w-40">Time</th>
                                <th className="p-3 w-48">Event Name</th>
                                <th className="p-3 w-64 bg-indigo-50/50 text-indigo-700 border-x border-indigo-100">Key Info (Context)</th>
                                <th className="p-3">Raw Data Check</th>
                                <th className="p-3 w-20 text-right">Action</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100 dark:divide-slate-700">
                            {logs.length === 0 ? (
                                <tr><td colSpan={6} className="p-10 text-center text-slate-400 italic">No logs found matching your criteria.</td></tr>
                            ) : logs.map((log) => (
                                <tr key={log.id} className="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors group">
                                    <td className="p-3 font-mono text-slate-400 text-xs">#{log.id}</td>
                                    <td className="p-3 font-mono text-slate-600 dark:text-slate-300 whitespace-nowrap text-xs">{log.created_at}</td>
                                    <td className="p-3"><span className="px-2 py-1 bg-white text-slate-700 rounded text-xs font-bold border border-slate-200 shadow-sm">{log.event_name}</span></td>
                                    <td className="p-3 font-medium text-xs text-indigo-700 bg-indigo-50/30 border-x border-indigo-50 truncate max-w-[200px]" title={log.key_info}>
                                        {log.key_info !== '-' ? log.key_info : <span className="text-slate-300">-</span>}
                                    </td>
                                    <td className="p-3 font-mono text-[10px] text-slate-400 truncate max-w-[300px] opacity-70 group-hover:opacity-100">{JSON.stringify(log.event_json)}</td>
                                    <td className="p-3 text-right">
                                        <button onClick={() => setSelectedLog(log)} className="text-slate-400 hover:text-blue-600 p-1.5 hover:bg-blue-50 rounded transition-colors" title="View Full JSON">
                                            <FileJson size={18} />
                                        </button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>

                <div className="p-3 border-t dark:border-slate-700 bg-slate-50 dark:bg-slate-900 rounded-b-xl flex flex-col sm:flex-row justify-between items-center gap-4">
                    <div className="flex items-center gap-4 text-xs text-slate-500">
                        <span>Showing <b>{logs.length}</b> records</span>
                        <span className="hidden sm:inline">|</span>
                        <span>Total: <b>{pagination.total_records.toLocaleString()}</b> records</span>
                    </div>
                    <div className="flex items-center gap-1 bg-white dark:bg-slate-800 p-1 rounded-lg border border-slate-200 dark:border-slate-700 shadow-sm">
                        <button onClick={() => handlePageChange(1)} disabled={pagination.page <= 1} className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-slate-600 dark:text-slate-300">
                            <ChevronsLeft size={16} />
                        </button>
                        <button onClick={() => handlePageChange(pagination.page - 1)} disabled={pagination.page <= 1} className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-slate-600 dark:text-slate-300 border-r dark:border-slate-700 mr-2">
                            <ChevronLeft size={16} />
                        </button>
                        <span className="text-xs font-bold px-3 text-slate-700 dark:text-slate-200 min-w-[100px] text-center">
                            Page {pagination.page.toLocaleString()} / {pagination.total_pages.toLocaleString()}
                        </span>
                        <button onClick={() => handlePageChange(pagination.page + 1)} disabled={pagination.page >= pagination.total_pages} className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-slate-600 dark:text-slate-300 border-l dark:border-slate-700 ml-2">
                            <ChevronRight size={16} />
                        </button>
                        <button onClick={() => handlePageChange(pagination.total_pages)} disabled={pagination.page >= pagination.total_pages} className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-slate-600 dark:text-slate-300">
                            <ChevronsRight size={16} />
                        </button>
                    </div>
                </div>
            </div>

            {selectedLog && (
                <JsonModal data={selectedLog} onClose={() => setSelectedLog(null)} />
            )}

            {/* --- MODAL TÌM USER RỚT ĐÀI CẢI TIẾN --- */}
            {dropModal.isOpen && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4 backdrop-blur-sm animate-in fade-in">
                    <div className="bg-white dark:bg-slate-800 w-full max-w-2xl rounded-xl shadow-2xl flex flex-col animate-in zoom-in-95 border dark:border-slate-700 overflow-hidden">
                        
                        <div className="p-4 border-b dark:border-slate-700 flex justify-between items-center bg-orange-50 dark:bg-slate-900">
                            <div className="flex items-center gap-3">
                                <div className="p-2 bg-orange-100 text-orange-600 rounded-lg"><UserMinus size={20} /></div>
                                <div>
                                    <h3 className="text-lg font-bold text-slate-800 dark:text-white">Tìm DropUser</h3>
                                    <p className="text-xs text-slate-500">
                                        Thời gian: {filters.start_date || dates.start} đến {filters.end_date || dates.end}
                                    </p>
                                </div>
                            </div>
                            <button onClick={() => setDropModal({ ...dropModal, isOpen: false })} className="p-2 hover:bg-orange-100 dark:hover:bg-slate-700 rounded-full text-slate-500 transition-colors"><X size={20} /></button>
                        </div>

                        <div className="p-5 flex flex-col gap-5">
                            <div className="flex items-end gap-3">
                                <div className="flex-1">
                                    <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">Check Level</label>
                                    <input 
                                        type="number" 
                                        placeholder="Nhập số level (VD: 0)" 
                                        className="w-full px-4 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg outline-none focus:ring-2 focus:ring-orange-500 font-bold"
                                        value={dropModal.level}
                                        onChange={(e) => setDropModal({ ...dropModal, level: e.target.value })}
                                        onKeyDown={(e) => e.key === 'Enter' && handleCheckDroppedUsers()}
                                    />
                                </div>
                                <button 
                                    onClick={handleCheckDroppedUsers}
                                    disabled={dropModal.loading}
                                    className="bg-slate-800 hover:bg-slate-900 dark:bg-blue-600 dark:hover:bg-blue-700 text-white px-6 py-2 rounded-lg font-bold h-[42px] transition-all disabled:opacity-70 flex items-center justify-center min-w-[120px]"
                                >
                                    {dropModal.loading ? <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin" /> : 'Search'}
                                </button>
                            </div>

                            {dropModal.result && (
                                <div className="animate-in slide-in-from-bottom-2 space-y-4">
                                    
                                    <div className="grid grid-cols-3 gap-3">
                                        <div className="bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 p-3 rounded-lg text-center">
                                            <div className="text-xs text-slate-500 font-bold mb-1 uppercase">User Start</div>
                                            <div className="text-2xl font-black text-slate-700 dark:text-white">{dropModal.result.total_start}</div>
                                        </div>
                                        <div className="bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 p-3 rounded-lg text-center">
                                            <div className="text-xs text-slate-500 font-bold mb-1 uppercase">User Win</div>
                                            <div className="text-2xl font-black text-blue-600">{dropModal.result.total_win}</div>
                                        </div>
                                        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 p-3 rounded-lg text-center">
                                            <div className="text-xs text-red-500 font-bold mb-1 uppercase flex items-center justify-center gap-1"><AlertCircle size={12}/> Đã Rớt</div>
                                            <div className="text-2xl font-black text-red-600">{dropModal.result.dropped_count}</div>
                                        </div>
                                    </div>

                                    <div className="space-y-2">
                                        <div className="flex justify-between items-center">
                                            <label className="text-xs font-bold text-slate-500 uppercase flex items-center gap-1">
                                                <ClipboardList size={14}/> List UUID (user_id) Drop
                                            </label>
                                            
                                            {dropModal.result.dropped_count > 0 && (
                                                <button 
                                                    onClick={() => {
                                                        navigator.clipboard.writeText(dropModal.result.dropped_uuids.join('\n'));
                                                        alert("Copy danh sách UUID thành công!");
                                                    }}
                                                    className="text-xs font-bold text-blue-600 hover:text-blue-800 bg-blue-50 hover:bg-blue-100 px-3 py-1 rounded-md transition-colors"
                                                >
                                                    Copy List
                                                </button>
                                            )}
                                        </div>
                                        
                                        {/* [ĐÃ SỬA]: BIẾN CÁC UUID THÀNH NÚT BẤM (BUTTON) */}
                                        <div className="bg-slate-50 dark:bg-[#1e1e1e] border border-slate-200 dark:border-slate-700 rounded-lg p-2 h-[200px] overflow-auto custom-scrollbar">
                                            {dropModal.result.dropped_count === 0 ? (
                                                <div className="h-full flex items-center justify-center text-slate-400 text-sm font-medium italic">
                                                    Không có uuid (uuser_id) drop level này 🎉
                                                </div>
                                            ) : (
                                                <div className="flex flex-col gap-1">
                                                    {dropModal.result.dropped_uuids.map((uid: string) => (
                                                        <button 
                                                            key={uid}
                                                            onClick={() => handleUuidClick(uid)}
                                                            className="flex items-center gap-2 text-left text-xs font-mono text-slate-700 dark:text-slate-300 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 px-3 py-2 rounded-md transition-all group"
                                                            title="Click để theo dấu chân (User Journey) của người này"
                                                        >
                                                            <Activity size={14} className="text-slate-400 group-hover:text-blue-500" />
                                                            {uid}
                                                        </button>
                                                    ))}
                                                </div>
                                            )}
                                        </div>
                                    </div>

                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}

        </div>
    );
}

// --- SUB COMPONENT: JSON MODAL (Giữ nguyên) ---
function JsonModal({ data, onClose }: { data: any, onClose: () => void }) {
    const [copied, setCopied] = useState(false);
    let jsonString = "{}";
    try {
        jsonString = JSON.stringify(data.event_json, null, 2);
    } catch(e) { jsonString = String(data.event_json); }

    const handleCopy = () => {
        navigator.clipboard.writeText(jsonString);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    }

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm animate-in fade-in">
            <div className="bg-white dark:bg-slate-800 w-full max-w-4xl rounded-xl shadow-2xl flex flex-col max-h-[90vh] animate-in zoom-in-95 border dark:border-slate-700">
                <div className="p-4 border-b dark:border-slate-700 flex justify-between items-center bg-slate-50 dark:bg-slate-900 rounded-t-xl">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-indigo-100 text-indigo-600 rounded-lg"><Terminal size={20} /></div>
                        <div>
                            <h3 className="text-lg font-bold">Event Detail #{data.id}</h3>
                            <p className="text-xs text-slate-500 font-mono">{data.event_name} • {data.created_at}</p>
                        </div>
                    </div>
                    <div className="flex items-center gap-2">
                        <button onClick={handleCopy} className={`flex items-center gap-1 text-xs font-bold px-3 py-1.5 rounded border transition-all ${copied ? 'bg-green-50 text-green-600 border-green-200' : 'hover:bg-white'}`}>
                            {copied ? <Check size={14} /> : <Copy size={14} />} {copied ? 'Copied!' : 'Copy JSON'}
                        </button>
                        <button onClick={onClose} className="p-2 hover:bg-slate-200 dark:hover:bg-slate-700 rounded-full transition-colors"><X size={20} /></button>
                    </div>
                </div>
                <div className="flex-1 overflow-auto bg-[#1e1e1e] p-4 custom-scrollbar">
                    <pre className="font-mono text-xs leading-relaxed text-[#d4d4d4]">
                        {jsonString.split('\n').map((line, i) => (
                            <div key={i} className="hover:bg-[#2a2a2a] px-2 rounded-sm">{line}</div>
                        ))}
                    </pre>
                </div>
            </div>
        </div>
    );
}