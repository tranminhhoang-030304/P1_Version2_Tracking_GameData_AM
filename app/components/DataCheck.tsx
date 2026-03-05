'use client';

import React, { useState, useEffect, useMemo } from 'react';
import { Search, Calendar, Download, Table as TableIcon, AlertCircle, FileSpreadsheet } from 'lucide-react';

interface DataCheckRow {
    level: string;
    _sort: number;
    difficulty: string;
    user_complete: number;
    win_rate: number;
    play_count_avg: number;
    avg_timeplay: number;
    avg_fail_process: number | null;
    coin_spent: number;
    boosters: Record<string, number>; // { Hammer: 10, Magnet: 5 }
}

export default function DataCheckTab({ appId }: { appId: number }) {
    const [data, setData] = useState<DataCheckRow[]>([]);
    const [loading, setLoading] = useState(false);
    const [dates, setDates] = useState({ start: '', end: '' });

    // 1. Gọi API Data Check
    const fetchData = async () => {
        setLoading(true);
        try {
            const params = new URLSearchParams();
            if (dates.start) params.append('start_date', dates.start); // Định dạng YYYY-MM-DD từ input date là chuẩn rồi
            if (dates.end) params.append('end_date', dates.end);

            const res = await fetch(`http://127.0.0.1:8080/api/data-check/${appId}?${params.toString()}`);
            const json = await res.json();
            
            if (json.success) {
                setData(json.data);
            }
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    // Gọi lần đầu
    useEffect(() => { fetchData(); }, [appId]);

    // 2. Tự động lấy danh sách tên các Booster (Cột động)
    const boosterKeys = useMemo(() => {
        const keys = new Set<string>();
        data.forEach(row => {
            if (row.boosters) {
                Object.keys(row.boosters).forEach(k => keys.add(k));
            }
        });
        return Array.from(keys).sort();
    }, [data]);

    // 3. Hàm Xuất CSV (Export Excel)
    const exportCSV = () => {
        if (data.length === 0) return;

        // Header
        const headers = [
            "Level", "Users Win", "Win Rate (%)", "Avg Retry", "Avg Time (s)", 
            "Fail Process (%)", "Total Coin Spent", 
            ...boosterKeys.map(k => `Booster ${k}`)
        ];

        // Rows
        const csvRows = data.map(row => {
            const basic = [
                row.level,
                row.user_complete,
                row.win_rate,
                row.play_count_avg,
                row.avg_timeplay,
                row.avg_fail_process !== null ? row.avg_fail_process : "",
                row.coin_spent
            ];
            // Map booster values
            const boosterVals = boosterKeys.map(k => row.boosters[k] || 0);
            return [...basic, ...boosterVals].join(",");
        });

        const csvContent = "data:text/csv;charset=utf-8," 
            + headers.join(",") + "\n" 
            + csvRows.join("\n");

        const encodedUri = encodeURI(csvContent);
        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", `DataCheck_App${appId}_${new Date().toISOString().slice(0,10)}.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    // Helper tô màu Win Rate
    const getWinRateColor = (rate: number) => {
        if (rate < 20) return "text-red-600 font-bold bg-red-50";
        if (rate < 40) return "text-orange-600 font-bold bg-orange-50";
        if (rate > 80) return "text-green-600 font-bold bg-green-50";
        return "text-slate-700";
    };

    return (
        <div className="space-y-4 animate-in fade-in h-full flex flex-col">
            {/* FILTER BAR */}
            <div className="bg-white dark:bg-slate-800 p-4 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-wrap gap-4 items-end justify-between">
                <div className="flex items-end gap-4">
                    <div>
                        <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">Time Range</label>
                        <div className="flex items-center gap-2">
                            <input type="date" className="px-3 py-2 bg-slate-50 dark:bg-slate-900 border rounded-lg text-sm"
                                value={dates.start} onChange={e => setDates({ ...dates, start: e.target.value })} />
                            <span className="text-slate-400">-</span>
                            <input type="date" className="px-3 py-2 bg-slate-50 dark:bg-slate-900 border rounded-lg text-sm"
                                value={dates.end} onChange={e => setDates({ ...dates, end: e.target.value })} />
                        </div>
                    </div>
                    <button onClick={fetchData} className="bg-blue-600 hover:bg-blue-700 text-white px-5 py-2 rounded-lg font-bold text-sm h-[38px] flex items-center gap-2">
                        {loading ? "..." : <Search size={16} />} Check
                    </button>
                </div>

                <button onClick={exportCSV} disabled={data.length === 0} 
                    className="bg-emerald-600 hover:bg-emerald-700 text-white px-5 py-2 rounded-lg font-bold text-sm h-[38px] flex items-center gap-2 disabled:opacity-50">
                    <FileSpreadsheet size={18} /> Export CSV
                </button>
            </div>

            {/* DATA TABLE */}
            <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex-1 flex flex-col min-h-0 overflow-hidden">
                <div className="overflow-auto custom-scrollbar flex-1">
                    <table className="w-full text-sm text-left border-collapse relative">
                        <thead className="bg-slate-100 dark:bg-slate-900 text-slate-500 sticky top-0 z-10 font-bold text-xs uppercase tracking-wider shadow-sm">
                            <tr>
                                <th className="p-3 w-20 border-b dark:border-slate-700 sticky left-0 bg-slate-100 dark:bg-slate-900 z-20">Level</th>
                                <th className="p-3 border-b dark:border-slate-700 text-center">User Win (DAU)</th>
                                <th className="p-3 border-b dark:border-slate-700 text-center">Win Rate</th>
                                <th className="p-3 border-b dark:border-slate-700 text-center">Avg Retry</th>
                                <th className="p-3 border-b dark:border-slate-700 text-center">Avg Time (s)</th>
                                <th className="p-3 border-b dark:border-slate-700 text-center">Fail @ (%)</th>
                                <th className="p-3 border-b dark:border-slate-700 text-right bg-amber-50 dark:bg-amber-900/10 text-amber-700 border-l border-r">Coin Spent</th>
                                
                                {/* Dynamic Booster Columns */}
                                {boosterKeys.map(key => (
                                    <th key={key} className="p-3 border-b dark:border-slate-700 text-center min-w-[80px]">
                                        {key}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100 dark:divide-slate-700">
                            {data.length === 0 && !loading ? (
                                <tr><td colSpan={10} className="p-10 text-center text-slate-400">No data found. Select date range and click Check.</td></tr>
                            ) : data.map((row) => (
                                <tr key={row.level} className="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
                                    <td className="p-3 font-bold text-slate-700 dark:text-slate-200 border-r dark:border-slate-700 sticky left-0 bg-white dark:bg-slate-800">
                                        Lv.{row.level}
                                    </td>
                                    <td className="p-3 text-center">{row.user_complete}</td>
                                    <td className={`p-3 text-center ${getWinRateColor(row.win_rate)}`}>
                                        {row.win_rate}%
                                    </td>
                                    <td className="p-3 text-center text-slate-500">{row.play_count_avg}</td>
                                    <td className="p-3 text-center">{row.avg_timeplay}s</td>
                                    <td className="p-3 text-center text-slate-500">
                                        {row.avg_fail_process !== null ? `${Math.round(row.avg_fail_process)}%` : '-'}
                                    </td>
                                    <td className="p-3 text-right font-mono text-amber-600 bg-amber-50/50 border-l border-r dark:border-slate-700">
                                        {row.coin_spent.toLocaleString()}
                                    </td>

                                    {/* Booster Values */}
                                    {boosterKeys.map(key => (
                                        <td key={key} className="p-3 text-center text-slate-500">
                                            {row.boosters[key] > 0 ? (
                                                <span className="font-bold text-slate-800 dark:text-slate-200">{row.boosters[key]}</span>
                                            ) : (
                                                <span className="text-slate-300">-</span>
                                            )}
                                        </td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}