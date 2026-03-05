import { useEffect, useState } from 'react';
import { Tag, Copy, RefreshCw, ListFilter } from 'lucide-react';

// --- SUB-COMPONENT: EVENT DICTIONARY ---
export function EventDictionary({ appId }: { appId: number }) {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const fetchEvents = async () => {
    setLoading(true);
    try {
      const res = await fetch(`http://127.0.0.1:8080/api/events/dictionary/${appId}`);
      const json = await res.json();
      if (json.success) setData(json);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchEvents(); }, [appId]);

  if (!data) return null;

  // Map màu sắc cho từng nhóm
  const getColor = (group: string) => {
    if (group.includes("Progression")) return "bg-blue-50 text-blue-700 border-blue-200";
    if (group.includes("Economy")) return "bg-emerald-50 text-emerald-700 border-emerald-200";
    if (group.includes("Ads")) return "bg-amber-50 text-amber-700 border-amber-200";
    if (group.includes("System")) return "bg-slate-100 text-slate-600 border-slate-200";
    return "bg-purple-50 text-purple-700 border-purple-200";
  };

  return (
    <div className="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm mt-6 animate-in slide-in-from-bottom-4">
      {/* Header */}
      <div className="p-4 border-b border-slate-100 dark:border-slate-700 flex justify-between items-center bg-slate-50/50 dark:bg-slate-900/50 rounded-t-xl">
        <div className="flex items-center gap-2">
          <ListFilter size={20} className="text-blue-600" />
          <h3 className="font-bold text-slate-800 dark:text-slate-200">Captured Events Dictionary</h3>
          <span className="px-2 py-0.5 bg-slate-200 dark:bg-slate-700 text-slate-600 dark:text-slate-300 text-xs rounded-full font-bold">
            {data.total_count}
          </span>
        </div>
        <button onClick={fetchEvents} className="p-2 hover:bg-white dark:hover:bg-slate-700 rounded-lg text-slate-400 hover:text-blue-600 transition-all">
          <RefreshCw size={16} className={loading ? "animate-spin" : ""} />
        </button>
      </div>

      {/* Body: Danh sách phân loại */}
      <div className="p-6 grid grid-cols-1 md:grid-cols-2 gap-6">
        {Object.entries(data.groups).map(([groupName, events]: [string, any]) => (
          <div key={groupName} className="space-y-3">
            <h4 className="text-xs font-bold uppercase tracking-wider text-slate-400 flex items-center gap-2 border-b border-slate-100 dark:border-slate-700 pb-1">
              {groupName} <span className="text-[10px] bg-slate-100 dark:bg-slate-800 px-1.5 rounded text-slate-500">{events.length}</span>
            </h4>
            
            <div className="flex flex-wrap gap-2">
              {events.map((evt: string) => (
                <div 
                  key={evt} 
                  className={`px-2.5 py-1 rounded text-xs font-medium border flex items-center gap-1.5 cursor-default group transition-all hover:scale-105 ${getColor(groupName)}`}
                  title="Click to copy"
                  onClick={() => navigator.clipboard.writeText(evt)}
                >
                  {evt}
                  {/* Icon copy ẩn, hiện khi hover */}
                  <Copy size={10} className="opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer" />
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}