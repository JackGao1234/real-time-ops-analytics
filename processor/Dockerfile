# 使用一個輕量級的 Python 基礎鏡像
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 將 requirements.txt 複製到容器中
COPY processor/requirements.txt .

# 安裝所有 Python 依賴
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式代碼
COPY . /app

# 運行主程式 (我們將在 docker-compose 中覆蓋 entrypoint 來運行 dotenv)
CMD ["python3", "main.py"]
