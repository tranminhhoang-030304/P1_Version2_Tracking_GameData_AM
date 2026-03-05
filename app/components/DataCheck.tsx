'use client';

import React, { useState, useEffect, useMemo } from 'react';
import { Search, Calendar, FileSpreadsheet, Loader2, Smartphone, Globe, X } from 'lucide-react'; 
import { API_URL } from '@/lib/api-config'; 

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

interface DataCheckRow {
    level: string | number;
    user_start: number;
    user_win: number;
    level_drop: number;
    next_drop: number | string;
    play_count: number;
    
    boosters: Record<string, number>; 
    total_booster: number;
    avg_booster: number;
    
    unlock?: number;
    revive_full?: number;
    revive_moves?: number;
    total_revive?: number;
    
    avg_coin: number;
    avg_time: number;
    
    level_drop_diff?: number;
    coin_spend_diff?: number;
}

export default function DataCheckTab({ appId }: { appId: number }) {
    const [data, setData] = useState<DataCheckRow[]>([]);
    const [loading, setLoading] = useState(false);
    const [isExporting, setIsExporting] = useState(false);
    
    const [dates, setDates] = useState(getDefaultRange(1));
    const [version, setVersion] = useState('All'); 
    const [geo, setGeo] = useState('All');

    const [filterOptions, setFilterOptions] = useState<{versions: string[], geos: string[]}>({
        versions: ['All'],
        geos: ['All']
    });

    useEffect(() => {
        const fetchFilterOptions = async () => {
            if (!appId) return;
            try {
                const res = await fetch(`${API_URL}/api/filters/options/${appId}`);
                const json = await res.json();
                if (json.versions && json.geos) {
                    setFilterOptions(json);
                }
            } catch (e) {
                console.error("Filter options error:", e);
            }
        };
        fetchFilterOptions();
    }, [appId]);

    const fetchData = async () => {
        setLoading(true);
        try {
            const params = new URLSearchParams();
            if (dates.start) params.append('start_date', dates.start);
            if (dates.end) params.append('end_date', dates.end);
            
            if (version && version.toLowerCase() !== 'all') params.append('version', version);
            if (geo && geo.toLowerCase() !== 'all') params.append('geo', geo);

            const res = await fetch(`${API_URL}/api/data-check/${appId}?${params.toString()}`);
            const json = await res.json();
            if (json.success) setData(json.data);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => { fetchData(); }, [appId]);

    const boosterKeys = useMemo(() => {
        const keys = new Set<string>();
        data.forEach(row => {
            if (row.boosters) Object.keys(row.boosters).forEach(k => keys.add(k));
        });
        return Array.from(keys).sort();
    }, [data]);

    const handleExportExcel = async () => {
        if (!appId) return;
        setIsExporting(true);
        try {
            const params = new URLSearchParams();
            if (dates.start) params.append('start_date', dates.start);
            if (dates.end) params.append('end_date', dates.end);
            if (version && version.toLowerCase() !== 'all') params.append('version', version);
            if (geo && geo.toLowerCase() !== 'all') params.append('geo', geo);

            const response = await fetch(`${API_URL}/api/datacheck/export/${appId}?${params.toString()}`);
            
            if (!response.ok) throw new Error("Export failed");
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `App_${appId}_DataCheck.xlsx`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (e) {
            alert("Export failed!");
        } finally {
            setIsExporting(false);
        }
    };

    const getDropColor = (rate: number) => {
        if (rate > 50) return "text-red-600 font-bold bg-red-50";
        if (rate > 30) return "text-orange-600 font-bold bg-orange-50";
        return "text-slate-600 font-medium";
    };

    // [BÁC SĨ FIX]: Đổi logic renderCell thành hiện số 0 nhạt màu thay vì gạch ngang
    const renderCell = (val: number | undefined | null, suffix = '') => {
        const finalVal = (val === undefined || val === null) ? 0 : val;
        if (finalVal === 0) return <span className="text-slate-400">0{suffix}</span>;
        return <span className="font-medium text-slate-700">{finalVal.toLocaleString()}{suffix}</span>;
    };

    const formatBoosterName = (key: string) => {
        const clean = key.replace('booster_', '');
        return `Booster ${clean.charAt(0).toUpperCase() + clean.slice(1)}`;
    };

    const clearDateFilter = () => {
        setDates({ start: '', end: '' });
    };

    return (
        <div className="space-y-4 animate-in fade-in h-full flex flex-col">
            {/* --- FILTER BAR --- */}
            <div className="bg-white dark:bg-slate-800 p-4 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-wrap gap-4 items-end justify-between">
                <div className="flex flex-wrap items-end gap-4">
                    {/* Date Picker */}
                    <div>
                        <label className="text-xs font-bold text-slate-500 uppercase mb-1 flex items-center gap-1"><Calendar size={12}/> Time Range</label>
                        <div className="flex items-center gap-2 bg-slate-50 dark:bg-slate-900 border rounded-lg p-1 pr-2">
                            <input type="date" className="px-2 py-1 bg-transparent text-sm outline-none"
                                value={dates.start} onChange={e => setDates({ ...dates, start: e.target.value })} />
                            <span className="text-slate-400">-</span>
                            <input type="date" className="px-2 py-1 bg-transparent text-sm outline-none"
                                value={dates.end} onChange={e => setDates({ ...dates, end: e.target.value })} />
                            
                            {(dates.start || dates.end) && (
                                <button onClick={clearDateFilter} className="ml-2 text-slate-400 hover:text-red-500 transition-colors" title="Clear Date Filter">
                                    <X size={14} />
                                </button>
                            )}
                        </div>
                    </div>

                    <div>
                        <label className="text-xs font-bold text-slate-500 uppercase mb-1 flex items-center gap-1"><Smartphone size={12}/> Version</label>
                        <select className="px-3 py-2 bg-slate-50 dark:bg-slate-900 border rounded-lg text-sm min-w-[100px] max-w-[150px]"
                            value={version} onChange={e => setVersion(e.target.value)}>
                            {filterOptions.versions.map((ver) => (
                                <option key={ver} value={ver}>{ver}</option>
                            ))}
                        </select>
                    </div>

                    <div>
                        <label className="text-xs font-bold text-slate-500 uppercase mb-1 flex items-center gap-1"><Globe size={12}/> Geo</label>
                        <select className="px-3 py-2 bg-slate-50 dark:bg-slate-900 border rounded-lg text-sm min-w-[100px] max-w-[150px]"
                            value={geo} onChange={e => setGeo(e.target.value)}>
                            {filterOptions.geos.map((g) => (
                                <option key={g} value={g}>{g}</option>
                            ))}
                        </select>
                    </div>

                    <button onClick={fetchData} className="bg-blue-600 hover:bg-blue-700 text-white px-5 py-2 rounded-lg font-bold text-sm h-[38px] flex items-center gap-2 shadow-sm">
                        {loading ? <Loader2 size={16} className="animate-spin"/> : <Search size={16} />} Check
                    </button>
                </div>
                <button onClick={handleExportExcel} disabled={isExporting} className="bg-emerald-600 hover:bg-emerald-700 text-white px-5 py-2 rounded-lg font-bold text-sm h-[38px] flex items-center gap-2 shadow-sm">
                    {isExporting ? <Loader2 size={18} className="animate-spin" /> : <FileSpreadsheet size={18} />} Export
                </button>
            </div>

            {/* --- DATA TABLE --- */}
            <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex-1 flex flex-col min-h-0 overflow-hidden">
                <div className="overflow-auto custom-scrollbar flex-1 pb-2">
                    <table className="min-w-[2800px] text-sm text-left border-collapse relative">
                        <thead className="bg-slate-100 dark:bg-slate-900 text-slate-500 sticky top-0 z-10 font-bold text-xs uppercase tracking-wider shadow-sm">
                            <tr>
                                {/* --- NHÓM 1: CƠ BẢN --- */}
                                <th className="p-3 w-20 border-b dark:border-slate-700 sticky left-0 bg-slate-100 dark:bg-slate-900 z-20 text-center shadow-[2px_0_5px_rgba(0,0,0,0.05)]">Level</th>
                                <th className="p-3 border-b text-center bg-gray-50/50">User Start Level</th>
                                <th className="p-3 border-b text-center bg-gray-50/50 border-r">User Win Level</th>
                                <th className="p-3 border-b text-center text-red-600 bg-red-50/30">Level Drop</th>
                                <th className="p-3 border-b text-center text-red-600 bg-red-50/30">Drop giữa 2 level</th>
                                <th className="p-3 border-b text-center text-purple-600 bg-purple-50/30 border-r">Level Drop Change (vs Prev Ver)</th>
                                <th className="p-3 border-b text-center border-r">Play Count</th>
                                
                                {/* --- NHÓM 2: UNLOCK & BOOSTERS --- */}
                                <th className="p-3 border-b text-center bg-yellow-50/20 text-amber-700 border-l">Unlock</th>
                                
                                {boosterKeys.map(key => (
                                    <th key={key} className="p-3 border-b text-center min-w-[100px] text-slate-600 bg-blue-50/10">
                                        {formatBoosterName(key)}
                                    </th>
                                ))}

                                <th className="p-3 border-b text-center bg-blue-100/50 text-blue-800 font-bold border-l border-r">Total Booster Used</th>

                                {/* --- NHÓM 3: REVIVE --- */}
                                <th className="p-3 border-b text-center bg-purple-50/20 text-purple-700">Revive do full người</th>
                                <th className="p-3 border-b text-center bg-purple-50/20 text-purple-700">Revive do hết lượt</th>
                                <th className="p-3 border-b text-center bg-purple-50/40 text-purple-800 font-bold border-r">Total Revive</th>

                                {/* --- NHÓM 4: TỔNG HỢP & TIỀN --- */}
                                <th className="p-3 border-b text-center bg-slate-100 text-slate-700 border-r">Avg Booster & Revive/User</th>
                                <th className="p-3 border-b text-center">Avg Timeplay</th>
                                <th className="p-3 border-b text-center text-purple-600 bg-purple-50/30 border-l border-r">Coin Spend (vs Prev Ver)</th>
                                <th className="p-3 border-b text-center bg-amber-100/50 text-amber-800 font-bold">Avg Coin Spend/User/Level</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100 dark:divide-slate-700">
                            {data.map((row) => (
                                <tr key={row.level} className="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
                                    {/* CỘT LEVEL STICKY */}
                                    <td className="p-3 font-bold text-slate-700 border-r sticky left-0 bg-white dark:bg-slate-800 z-10 text-center shadow-[2px_0_5px_rgba(0,0,0,0.05)]">
                                        {row.level}
                                    </td>
                                    
                                    <td className="p-3 text-center">{renderCell(row.user_start)}</td>
                                    <td className="p-3 text-center border-r border-dashed">{renderCell(row.user_win)}</td>
                                    <td className={`p-3 text-center ${getDropColor(row.level_drop)}`}>{renderCell(row.level_drop, '%')}</td>
                                    
                                    {/* [BÁC SĨ FIX]: Đổi '-' thành '0%' */}
                                    <td className="p-3 text-center text-slate-400">
                                        {row.next_drop !== "-" && row.next_drop != null && row.next_drop !== 0 ? <span className="font-medium text-slate-700">{row.next_drop}%</span> : '0%'}
                                    </td>
                                    
                                    {/* [BÁC SĨ FIX]: Đổi '-' thành '0%' */}
                                    <td className="p-3 text-center text-slate-400 border-r border-dashed">0%</td>
                                    
                                    <td className="p-3 text-center border-r border-dashed">{renderCell(row.play_count)}</td>
                                    <td className="p-3 text-center border-l border-dashed">{renderCell(row.unlock)}</td>

                                    {/* [BÁC SĨ FIX]: Thay '-' thành số 0 nhạt màu cho các ô Booster không ai xài */}
                                    {boosterKeys.map(key => (
                                        <td key={key} className="p-3 text-center">
                                            {row.boosters[key] > 0 ? <span className="font-medium text-slate-700">{row.boosters[key]}</span> : <span className="text-slate-400">0</span>}
                                        </td>
                                    ))}

                                    <td className="p-3 text-center text-blue-700 bg-blue-50/30 border-l border-r border-blue-100">
                                        {renderCell(row.total_booster)}
                                    </td>

                                    <td className="p-3 text-center">{renderCell(row.revive_full)}</td>
                                    <td className="p-3 text-center">{renderCell(row.revive_moves)}</td>
                                    <td className="p-3 text-center text-purple-700 bg-purple-50/20 border-r border-dashed">
                                        {renderCell(row.total_revive)}
                                    </td>

                                    <td className="p-3 text-center border-r border-dashed">
                                        {renderCell(row.avg_booster)}
                                    </td>

                                    <td className="p-3 text-center">{renderCell(row.avg_time, 's')}</td>
                                    
                                    {/* [BÁC SĨ FIX]: Đổi '-' thành '0' */}
                                    <td className="p-3 text-center text-slate-400 border-l border-r border-dashed">0</td>

                                    {/* [BÁC SĨ FIX]: Đổi '-' thành '0' */}
                                    <td className="p-3 text-right font-mono bg-amber-50/50">
                                        {row.avg_coin > 0 ? <span className="font-bold text-amber-600">{row.avg_coin.toLocaleString()}</span> : <span className="text-slate-400">0</span>}
                                    </td>
                                </tr>
                            ))}
                            {data.length === 0 && !loading && (
                                <tr><td colSpan={30} className="p-10 text-center text-slate-400">No data found for this filter.</td></tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}