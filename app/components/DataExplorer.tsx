'use client';

import React, { useState, useEffect } from 'react';
import {
    Search, Calendar, FileJson, ChevronLeft, ChevronRight,
    X, Copy, Check, Filter, Terminal, ChevronsLeft, ChevronsRight
} from 'lucide-react';

interface DataExplorerProps {
    appId: number;
}

export default function DataExplorer({ appId }: DataExplorerProps) {
    // State dữ liệu
    const [logs, setLogs] = useState<any[]>([]);
    const [loading, setLoading] = useState(false);

    // State phân trang
    const [pagination, setPagination] = useState({ page: 1, total_pages: 1, total_records: 0 });

    // State bộ lọc
    const [filters, setFilters] = useState({
        keyword: '',      // Tìm UserID, Error Msg...
        event_name: '',   // Tìm tên event cụ thể
        start_date: '',
        end_date: ''
    });

    // State Modal xem chi tiết
    const [selectedLog, setSelectedLog] = useState<any>(null);

    // Hàm gọi API tìm kiếm
    const fetchLogs = async (page = 1) => {
        setLoading(true);
        try {
            // Xây dựng URL với các tham số query
            const params = new URLSearchParams({
                app_id: appId.toString(),
                page: page.toString(),
                limit: '50', // Lấy 50 dòng mỗi trang
            });

            if (filters.keyword) params.append('keyword', filters.keyword);
            if (filters.event_name) params.append('event_name', filters.event_name);
            if (filters.start_date) params.append('start_date', filters.start_date);
            if (filters.end_date) params.append('end_date', filters.end_date);

            const res = await fetch(`http://127.0.0.1:8080/events/search?${params.toString()}`);
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

    // Gọi API khi appId thay đổi hoặc user bấm nút chuyển trang
    useEffect(() => {
        fetchLogs(1);
    }, [appId]);

    // Xử lý chuyển trang
    const handlePageChange = (newPage: number) => {
        if (newPage >= 1 && newPage <= pagination.total_pages) {
            fetchLogs(newPage);
        }
    };

    // Xử lý nút Search (Về trang 1)
    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        fetchLogs(1);
    };

    return (
        <div className="space-y-4 animate-in fade-in duration-500 h-full flex flex-col">
            {/* 1. THANH TÌM KIẾM (FILTER BAR) */}
            <form onSubmit={handleSearch} className="bg-white dark:bg-slate-800 p-4 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-wrap gap-4 items-end">

                {/* Keyword Search */}
                <div className="flex-1 min-w-[200px]">
                    <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">Keyword / User ID</label>
                    <div className="relative">
                        <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                        <input
                            type="text"
                            placeholder="Search User ID, Error..."
                            className="w-full pl-9 pr-4 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                            value={filters.keyword}
                            onChange={e => setFilters({ ...filters, keyword: e.target.value })}
                        />
                    </div>
                </div>

                {/* Event Name */}
                <div className="w-[200px]">
                    <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">Event Name</label>
                    <input
                        type="text"
                        placeholder="e.g. priceSpendLevel"
                        className="w-full px-3 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                        value={filters.event_name}
                        onChange={e => setFilters({ ...filters, event_name: e.target.value })}
                    />
                </div>

                {/* Date Range */}
                <div className="flex items-center gap-2">
                    <div>
                        <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">From Date</label>
                        <input type="date" className="px-3 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg text-sm outline-none"
                            value={filters.start_date} onChange={e => setFilters({ ...filters, start_date: e.target.value })} />
                    </div>
                    <span className="mb-2 text-slate-400">-</span>
                    <div>
                        <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">To Date</label>
                        <input type="date" className="px-3 py-2 bg-slate-50 dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg text-sm outline-none"
                            value={filters.end_date} onChange={e => setFilters({ ...filters, end_date: e.target.value })} />
                    </div>
                </div>

                {/* Action Buttons */}
                <button type="submit" className="bg-blue-600 hover:bg-blue-700 text-white px-5 py-2 rounded-lg font-bold text-sm flex items-center gap-2 h-[38px]">
                    {loading ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" /> : <Search size={16} />}
                    Search
                </button>
                {(filters.keyword || filters.event_name || filters.start_date) && (
                    <button type="button" onClick={() => { setFilters({ keyword: '', event_name: '', start_date: '', end_date: '' }); fetchLogs(1); }} className="text-red-500 hover:bg-red-50 px-3 py-2 rounded-lg text-sm font-bold h-[38px]">Clear</button>
                )}
            </form>

            {/* 2. BẢNG DỮ LIỆU (DATA TABLE) */}
            <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex-1 flex flex-col min-h-0">
                <div className="overflow-auto flex-1 custom-scrollbar">
                    <table className="w-full text-sm text-left border-collapse">
                        <thead className="bg-slate-100 dark:bg-slate-900 text-slate-500 sticky top-0 z-10 shadow-sm">
                            <tr>
                                <th className="p-3 w-20">ID</th>
                                <th className="p-3 w-40">Time</th>
                                <th className="p-3 w-48">Event Name</th>
                                <th className="p-3 w-48">Key Info (Context)</th>
                                <th className="p-3">Raw Data Check</th>
                                <th className="p-3 w-20 text-right">Action</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100 dark:divide-slate-700">
                            {logs.length === 0 ? (
                                <tr><td colSpan={6} className="p-10 text-center text-slate-400 italic">No logs found matching your criteria.</td></tr>
                            ) : logs.map((log) => (
                                <tr key={log.id} className="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors group">
                                    <td className="p-3 font-mono text-slate-400">#{log.id}</td>
                                    <td className="p-3 font-mono text-slate-600 dark:text-slate-300 whitespace-nowrap">{log.created_at}</td>
                                    <td className="p-3"><span className="px-2 py-1 bg-blue-50 text-blue-700 rounded text-xs font-bold border border-blue-100">{log.event_name}</span></td>
                                    <td className="p-3 font-mono text-xs text-indigo-600 truncate max-w-[150px]" title={log.key_info}>{log.key_info}</td>
                                    <td className="p-3 font-mono text-[10px] text-slate-400 truncate max-w-[200px]">{JSON.stringify(log.event_json)}</td>
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

                {/* 3. THANH PHÂN TRANG (PAGINATION BAR - FIXED) */}
                <div className="p-3 border-t dark:border-slate-700 bg-slate-50 dark:bg-slate-900 rounded-b-xl flex flex-col sm:flex-row justify-between items-center gap-4">

                    {/* Hiển thị thông tin số lượng */}
                    <div className="flex items-center gap-4 text-xs text-slate-500">
                        <span>Showing <b>{logs.length}</b> records</span>
                        <span className="hidden sm:inline">|</span>
                        <span>Total: <b>{pagination.total_records.toLocaleString()}</b> records</span>
                    </div>

                    {/* Bộ nút điều hướng */}
                    <div className="flex items-center gap-1 bg-white dark:bg-slate-800 p-1 rounded-lg border border-slate-200 dark:border-slate-700 shadow-sm">

                        {/* Nút First Page (<<) */}
                        <button
                            onClick={() => handlePageChange(1)}
                            disabled={pagination.page <= 1}
                            className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-slate-600 dark:text-slate-300"
                            title="First Page"
                        >
                            <ChevronsLeft size={16} />
                        </button>

                        {/* Nút Prev Page (<) */}
                        <button
                            onClick={() => handlePageChange(pagination.page - 1)}
                            disabled={pagination.page <= 1}
                            className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-slate-600 dark:text-slate-300 border-r dark:border-slate-700 mr-2"
                            title="Previous Page"
                        >
                            <ChevronLeft size={16} />
                        </button>

                        {/* Hiển thị số trang */}
                        <span className="text-xs font-bold px-3 text-slate-700 dark:text-slate-200 min-w-[100px] text-center">
                            Page {pagination.page.toLocaleString()} / {pagination.total_pages.toLocaleString()}
                        </span>

                        {/* Nút Next Page (>) */}
                        <button
                            onClick={() => handlePageChange(pagination.page + 1)}
                            disabled={pagination.page >= pagination.total_pages}
                            className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-slate-600 dark:text-slate-300 border-l dark:border-slate-700 ml-2"
                            title="Next Page"
                        >
                            <ChevronRight size={16} />
                        </button>

                        {/* Nút Last Page (>>) */}
                        <button
                            onClick={() => handlePageChange(pagination.total_pages)}
                            disabled={pagination.page >= pagination.total_pages}
                            className="p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-slate-600 dark:text-slate-300"
                            title="Last Page"
                        >
                            <ChevronsRight size={16} />
                        </button>
                    </div>
                </div>
            </div>

            {/* 4. MODAL JSON VIEWER (SOI CHI TIẾT) */}
            {selectedLog && (
                <JsonModal data={selectedLog} onClose={() => setSelectedLog(null)} />
            )}
        </div>
    );
}

// --- SUB COMPONENT: JSON MODAL ---
function JsonModal({ data, onClose }: { data: any, onClose: () => void }) {
    const [copied, setCopied] = useState(false);
    const jsonString = JSON.stringify(data.event_json, null, 2);

    const handleCopy = () => {
        navigator.clipboard.writeText(jsonString);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    }

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm animate-in fade-in">
            <div className="bg-white dark:bg-slate-800 w-full max-w-4xl rounded-xl shadow-2xl flex flex-col max-h-[90vh] animate-in zoom-in-95 border dark:border-slate-700">
                {/* Header */}
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

                {/* Body (JSON View) */}
                <div className="flex-1 overflow-auto bg-[#1e1e1e] p-4 custom-scrollbar">
                    <pre className="font-mono text-xs leading-relaxed text-[#d4d4d4]">
                        {/* Highlight cú pháp JSON đơn giản */}
                        {jsonString.split('\n').map((line, i) => (
                            <div key={i} className="hover:bg-[#2a2a2a] px-2 rounded-sm">
                                {line}
                            </div>
                        ))}
                    </pre>
                </div>

                {/* Footer */}
                <div className="p-3 bg-slate-50 dark:bg-slate-900 border-t dark:border-slate-700 text-xs text-slate-400 text-center rounded-b-xl">
                    User ID: <span className="font-mono text-slate-600 dark:text-slate-300 select-all">{data.user_preview}</span>
                </div>
            </div>
        </div>
    );
}