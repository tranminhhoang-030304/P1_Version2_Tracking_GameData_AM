import { useState, useEffect } from 'react';

// Định nghĩa kiểu dữ liệu (nếu dùng TypeScript)
interface LogItem {
  id: number;
  start_time: string;
  status: string;
  records_fetched: number;
  file_path: string;
  message: string;
}

export default function RawDataMonitor() {
  const [logs, setLogs] = useState<LogItem[]>([]);

  const fetchLogs = async () => {
    try {
      const res = await fetch('http://localhost:8080/api/history/raw-data');
      if (res.ok) {
        const data = await res.json();
        setLogs(data);
      }
    } catch (err) {
      console.error("Không lấy được lịch sử Raw Data");
    }
  };

  useEffect(() => {
    fetchLogs();
    // Tự động refresh mỗi 5 giây
    const interval = setInterval(fetchLogs, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 mt-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-bold text-gray-800 flex items-center gap-2">
          📂 Lịch sử Cào Dữ Liệu Thô (Raw Data History)
        </h3>
        <button onClick={fetchLogs} className="text-sm text-blue-600 hover:underline">
          Làm mới
        </button>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-gray-500 uppercase bg-gray-50 border-b">
            <tr>
              <th className="px-4 py-3">Thời gian</th>
              <th className="px-4 py-3">Trạng thái</th>
              <th className="px-4 py-3">Số lượng</th>
              <th className="px-4 py-3">File đã lưu</th>
              <th className="px-4 py-3">Ghi chú</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {logs.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-4 py-4 text-center text-gray-500">
                  Chưa có lịch sử nào (Hoặc chưa bật Backend).
                </td>
              </tr>
            ) : (
              logs.map((log) => (
                <tr key={log.id} className="hover:bg-gray-50 transition-colors">
                  <td className="px-4 py-3 text-gray-600">
                    {log.start_time ? new Date(log.start_time).toLocaleString('vi-VN') : 'N/A'}
                  </td>
                  <td className="px-4 py-3">
                    <span className={`px-2 py-1 rounded text-xs font-semibold ${
                      log.status === 'SUCCESS' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                    }`}>
                      {log.status}
                    </span>
                  </td>
                  <td className="px-4 py-3 font-medium">
                    {log.records_fetched > 0 ? (
                      <span className="text-blue-600">+{log.records_fetched} records</span>
                    ) : (
                      <span className="text-gray-400">0</span>
                    )}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-500 truncate max-w-[200px]" title={log.file_path}>
                    {log.file_path ? '.../' + log.file_path.split(/[/\\]/).pop() : '-'}
                  </td>
                  <td className="px-4 py-3 text-gray-600 max-w-xs truncate" title={log.message}>
                    {log.message}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}