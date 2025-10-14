# 使用一個輕量級的 Python 基礎鏡像
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 將 requirements.txt 複製到工作目錄
# (假設您會創建一個包含所有依賴的 requirements.txt)
COPY requirements.txt .

# 安裝依賴
RUN pip install --no-cache-dir -r requirements.txt

# 複製所有應用程式代碼
COPY . .

# 運行主程式 (我們將在 docker-compose 中覆蓋 entrypoint 來運行 dotenv)
CMD ["python3", "main.py"]
