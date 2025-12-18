import { useState, useEffect } from 'react';

export default function SchedulerConfig() {
  const [interval, setIntervalVal] = useState(60);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState('');

  // 1. Load cấu hình hiện tại khi mở web
  useEffect(() => {
    fetch('http://localhost:8080/api/system/config')
      .then(res => res.json())
      .then(data => setIntervalVal(data.interval_minutes))
      .catch(err => console.error("Lỗi load config:", err));
  }, []);

  // 2. Hàm lưu cấu hình mới
  const handleSave = async () => {
    setLoading(true);
    setStatus('');
    try {
      const res = await fetch('http://localhost:8080/api/system/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ interval_minutes: Number(interval) })
      });
      const data = await res.json();
      
      if (data.status === 'success') {
        setStatus('✅ Đã cập nhật và áp dụng lịch mới ngay lập tức!');
      } else {
        setStatus('❌ Lỗi: ' + data.message);
      }
    } catch (err) {
      setStatus('❌ Lỗi kết nối server (Backend chưa chạy?).');
    }
    setLoading(false);
  };

  return (
    <div className="bg-white p-6 rounded-lg shadow-sm border border-blue-100 mt-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-bold text-blue-800 flex items-center gap-2">
          ⏰ Cấu hình Tự động Cào Dữ liệu
        </h3>
        <span className="text-xs bg-blue-100 text-blue-800 px-2 py-1 rounded">System Scheduler</span>
      </div>
      
      <p className="text-sm text-gray-600 mb-4">
        Hệ thống sẽ tự động cào dữ liệu thô từ AppMetrica và lưu thành file JSON.
      </p>

      <div className="flex items-end gap-4">
        <div className="w-full max-w-xs">
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Chu kỳ chạy (phút)
          </label>
          <input
            type="number"
            min="1"
            value={interval}
            onChange={(e) => setIntervalVal(Number(e.target.value))}
            className="w-full p-2 border border-gray-300 rounded focus:ring-2 focus:ring-blue-500 focus:outline-none"
          />
        </div>
        
        <button
          onClick={handleSave}
          disabled={loading}
          className={`px-4 py-2 rounded font-medium text-white transition-colors ${
            loading ? 'bg-gray-400 cursor-not-allowed' : 'bg-blue-600 hover:bg-blue-700'
          }`}
        >
          {loading ? 'Đang lưu...' : 'Lưu & Áp dụng'}
        </button>
      </div>

      {status && (
        <div className={`mt-3 p-3 rounded text-sm ${status.includes('✅') ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'}`}>
          {status}
        </div>
      )}
    </div>
  );
}