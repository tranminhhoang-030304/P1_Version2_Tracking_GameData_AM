// API Configuration
export const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080';
// Các cấu hình khác nếu cần
export const API_TIMEOUT = 30000;
export const DEFAULT_HEADERS = {
    'Content-Type': 'application/json',
};