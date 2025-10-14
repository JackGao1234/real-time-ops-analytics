# ⚡️ 實時營運分析平台 (Real-Time Operations Analytics Platform)

## 🚀 快速啟動手冊 (Quick Start Guide)

本手冊指導新進人員在專案環境中快速部署、執行數據流，並驗證結果。

### 1. 前置準備

* **基礎工具:** 確保系統已安裝 **Docker Compose** 和 **Python 3.x**。
* **配置檔案:** 在專案根目錄創建 `.env`，並依據 `.env_template` 填寫配置。

### 2. 環境啟動 (核心 Make 流程)

所有操作集中於 `Makefile`。請按以下**順序**執行：

| 步驟 | Command | 目的 | 備註 |
| :--- | :--- | :--- | :--- |
| **I. 啟動基礎設施** | `make up` | 啟動 Redpanda & ClickHouse 容器。 | 一次性啟動。 |
| **II. 創建資料庫結構** | `make ddl` | 執行所有 `db/*.sql` 腳本。 | **必須執行**，用於創建表格。 |
| **III. 啟動處理器** | `make processor-run` | 啟動 Python 實時數據處理服務。 | 處理器開始監聽 Kafka。 |
| **IV. 啟動數據流** | `make producer` | 啟動低負載模擬數據生產者。 | 在**另一個終端**運行。 |

### 3. QA 驗證手冊

使用 `make chclient` 進入 ClickHouse 客戶端，執行以下查詢驗證數據：

| QA 項目 | 查詢語句 (範例) | 預期結果 |
| :--- | :--- | :--- |
| **原始數據驗證** | `SELECT count() FROM raw_events;` | 數量持續增加。 |
| **原始數據驗證** | `SELECT dimension_channel, event_source FROM raw_events LIMIT 5;` | `dimension_channel` 有值 (Web/Mobile/...)。 |
| **聚合指標驗證** | `SELECT toStartOfMinute(time_window) AS minute_window, uniqMerge(unique_users_state) AS unique_users_count FROM metrics_minutely FINAL GROUP BY minute_window ORDER BY minute_window DESC;` | 每個分鐘窗的獨立用戶數（`uniq(user_id)`）持續增加。 |

---

## 🎯 專案核心設計與目標

### 1. 服務級別目標 (SLO)

| 指標 | 目標值 (SLO) | 備註 |
| :--- | :--- | :--- |
| **端到端延遲 (Latency)** | **< 5 秒** | 從 Producer 發送到指標可查詢的時間。 |
| **數據完整性 (Integrity)** | **高** (最終一致性) | 依賴 ClickHouse 的 `ReplacingMergeTree` 實現 `event_id` 去重。 |
| **吞吐量 (Throughput)** | **100+ TPS** | 高負載模式 (使用 `make producer-high`) 下需維持穩定。 |
<!-- 需求是每日數百萬筆, 保守估計1000萬筆, 約116筆/秒 -->

### 2. 關鍵技術棧

| 技術 | 目的/優勢 |
| :--- | :--- |
| **Redpanda** | 替代 Kafka，協議兼容，適用於容器環境，高性能。 |
| **ClickHouse** | OLAP 數據庫，專注於實時聚合與超高速查詢。 |
| **Python Processor** | 負責業務邏輯、時間窗劃分、利用 ClickHouse 函數（`uniqState`）進行高效聚合。 |

### 3. 決策與技術權衡 (Trade-offs)

| 決策點 | 選擇方案 | 權衡考量 (Why) |
| :--- | :--- | :--- |
| **實時聚合方式** | **Python 處理器 + ClickHouse `AggregatingMergeTree`** | 將聚合邏輯分散，Python 進行批次內聚合，ClickHouse 負責跨批次合併狀態，效率最高。 |
| **數據去重** | **ClickHouse `ReplacingMergeTree`** | 簡化處理器邏輯，將 `event_id` 去重交給 ClickHouse 異步處理，犧牲即時性（Eventually Consistent）換取吞吐量。 |
| **DDL 執行** | **`make ddl` 腳本化** | 確保環境在任何時候都能通過腳本（非手動）初始化和修復資料庫結構。 |

---

## 🗑️ 環境維護指令

| Command | 描述 |
| :--- | :--- |
| `make down` | 停止並移除所有容器。 |
| `make clean` | 停止、移除容器並**刪除所有 Data Volume** (用於環境重置，將遺失所有數據)。 |
| `make logs` | 實時追蹤所有服務的日誌。 |

---

## ♻️ 數據回補 (Reprocessing)

**狀態：** 待實現。

**思路：** 為了處理歷史數據或修復錯誤，未來可實現一個 Reprocessor，將特定時間範圍的數據重新從 Kafka 讀取（或從備份存儲）並重新寫入 ClickHouse。由於 ClickHouse 的聚合和去重機制，直接重寫數據即可實現修正。
