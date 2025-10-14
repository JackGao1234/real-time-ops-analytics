.PHONY: all up down clean logs shell-redpanda producer producer-high chclient ddl reprocess processor-run topic-check

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

# 清理所有數據、Data Volume和容器 (用於完全重置環境)
clean: down
	@echo "=> 清理所有 Data Volume..."
	docker compose down -v
	@echo "環境已完全清理，請重新運行 make up 啟動。"

# 查看所有服務日誌
logs:
	docker compose logs -f

# 進入 Redpanda 容器 Shell
shell-redpanda:
	@echo "=> 進入 Redpanda Shell..."
	docker exec -it redpanda bash

# 讀取 .env 檔案中的變數 (使用 sed 提取值並去除 Windows 換行符)
CH_USER := $(shell sed -n 's/CLICKHOUSE_USER=//p' .env | tr -d '\r')
CH_PASS := $(shell sed -n 's/CLICKHOUSE_PASSWORD=//p' .env | tr -d '\r')
CH_DB := $(shell sed -n 's/CLICKHOUSE_DB=//p' .env | tr -d '\r')
ORDER_TOPIC := $(shell sed -n 's/ORDER_TOPIC=//p' .env | tr -d '\r')
PAYMENT_TOPIC := $(shell sed -n 's/PAYMENT_TOPIC=//p' .env | tr -d '\r')

# 檢查 Kafka Topic 中的數據流 (預設檢查 order_topic)
topic-check:
	@echo "=> 正在檢查 $(ORDER_TOPIC) Topic 的最新數據 (Ctrl+C 停止)..."
	docker exec redpanda rpk topic consume $(ORDER_TOPIC) -f "%v\n"

# 運行低負載數據生產者 (Producer) 腳本 - 每秒約 1 筆
producer:
	@echo "=> 運行低負載數據生產者 (約 1 筆/秒)..."
	python3 sample-data/generate_samples_low_load.py

# 運行高負載數據生產者 (High-Producer) 腳本 - 每秒 100 筆
producer-high:
	@echo "=> 運行高負載數據生產者 (約 100 筆/秒)..."
	python3 sample-data/generate_samples_high_load.py

# 連線 ClickHouse 客戶端 (使用 .env 配置)
chclient:
	@echo "=> 連線 ClickHouse 客戶端 (User: $(CH_USER), DB: $(CH_DB))..."
	docker exec -it clickhouse clickhouse-client --host 127.0.0.1 --port 9000 --user "$(CH_USER)" --password "$(CH_PASS)" --database "$(CH_DB)"

# DDL 執行目標
ddl:
	@echo "=> 執行 DDL 腳本..."
	# 遍歷 db/ 目錄下所有 .sql 檔案並依次執行
	find db -name "*.sql" | sort | while read file; do \
		echo "-> 執行 $$file..."; \
		docker exec -i clickhouse clickhouse-client \
			--user "$(CH_USER)" \
			--password "$(CH_PASS)" \
			--database "$(CH_DB)" \
			--multiquery < "$$file"; \
	done
	@echo "✅ DDL 執行完成。"

# 回放腳本 (Reprocessing) - 骨架，用於未來實現
reprocess:
	@echo "=> 運行數據回放 (Reprocessing) 腳本 - 待實現"
	# 範例：./reprocess.sh

# 運行數據處理器 (Processor)
processor-run:
	@echo "=> 啟動實時數據處理器 (Docker 容器內)..."
	# 使用 --force-recreate 確保代碼有更新時能重新啟動
	docker compose up -d processor
