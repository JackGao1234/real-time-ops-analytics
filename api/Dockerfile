# 使用 Python 基礎映像
FROM python:3.11-slim

# 設置工作目錄
WORKDIR /app

# 複製依賴文件並安裝
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製整個應用程式（這裡假設 api/main.py 在根目錄下）
COPY api/ /app/api/

# 設置入口點
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
