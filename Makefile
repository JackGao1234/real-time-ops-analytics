# Makefile
.PHONY: all up down clean logs shell producer chclient

# 預設目標：啟動所有服務
all: up

# 啟動服務 (Daemon mode)
up:
	@echo "=> 啟動服務: Redpanda, ClickHouse..."
	docker compose up -d

# 停止並移除容器與網路 (保留數據)
down:
	@echo "=> 停止並移除容器..."
	docker compose down

# 清理所有數據、卷宗和容器 (用於完全重置環境)
clean: down
	@echo "=> 清理所有數據卷宗..."
	docker compose down -v
	@echo "環境已完全清理，請重新運行 make up 啟動。"

# 查看所有服務日誌
logs:
	docker compose logs -f

# 進入 Redpanda 容器 Shell
shell-redpanda:
	@echo "=> 進入 Redpanda Shell..."
	docker exec -it redpanda bash

# 運行數據生產者 (Producer) 腳本
producer:
	@echo "=> 運行數據生產者..."
	python3 sample-data/generate_samples.py

# 讀取 .env 檔案中的變數
CH_USER := $(shell grep CLICKHOUSE_USER .env | cut -d '=' -f2)
CH_PASS := $(shell grep CLICKHOUSE_PASSWORD .env | cut -d '=' -f2)
CH_DB := $(shell grep CLICKHOUSE_DB .env | cut -d '=' -f2)

# 連線 ClickHouse 客戶端 (使用 .env 配置)
chclient:
	@echo "=> 連線 ClickHouse 客戶端 (User: $(CH_USER), DB: $(CH_DB))..."
	docker exec -it clickhouse clickhouse-client --host 127.0.0.1 --port 9000 --user "$(CH_USER)" --password "$(CH_PASS)" --database "$(CH_DB)"

# 回放腳本 (Reprocessing) - 骨架，用於未來實現
reprocess:
	@echo "=> 運行數據回放 (Reprocessing) 腳本 - 待實現"
	# 範例：./reprocess.sh

# 運行數據處理器 (Processor)
processor-run:
	@echo "=> 啟動實時數據處理器 (Docker 容器內)..."
	# 使用 --force-recreate 確保代碼有更新時能重新啟動
	docker compose up -d processor
