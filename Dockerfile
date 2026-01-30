# Chọn version tương ứng bạn vừa check được (ví dụ 3.10)
FROM python:3.14-slim

WORKDIR /app

# Thiết lập biến môi trường để log hiện ngay lập tức và không tạo file .pyc rác
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Cài đặt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn uvicorn

# Copy toàn bộ code vào container (bao gồm cả thư mục api/)
COPY . .

# Lệnh chạy Production:
# api.index:app nghĩa là: vào thư mục api -> file index.py -> tìm biến app
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "api.index:app", "--bind", "0.0.0.0:8000"]