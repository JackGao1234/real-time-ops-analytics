# 實時營運分析平台 (Real-Time Operations Analytics Platform)

## 🎯 專案總覽與目標
本專案旨在建立一個低延遲、高吞吐量的數據管道，從交易事件流中實時計算核心業務指標（如 GMV、訂單數、退款率），並將結果提供給下游的儀表板或 API 服務。

專案目標是提供 **< 5 秒**的端到端數據延遲，並確保數據的**最終一致性 (Eventual Consistency)**，以支持營運和商業決策。

---

## 🛠️ 技術棧 (Stack)
| 組件 | 技術 | 目的 |
| :--- | :--- | :--- |
| **消息隊列** | Redpanda (Kafka Protocol) | 高性能、低延遲的消息Broker，用於事件分發。 |
| **數據庫** | ClickHouse | 實時 OLAP 分析資料庫，用於指標聚合與高速查詢。 |
| **處理器** | Python + `kafka-python` + `clickhouse-connect` | 核心業務邏輯，負責從 Kafka 讀取、去重、聚合及寫入。 |
| **API** | Python + FastAPI (待開發) | 提供對 ClickHouse 指標的即時查詢服務。 |
| **基礎設施** | Docker Compose | 開發環境的快速部署與服務協調。 |

---

## 🚀 快速啟動指南

### 1. 前置準備
確保系統安裝以下工具：
1.  **Docker** 與 **Docker Compose**
2.  **Python 3.x**
3.  **ClickHouse Client** (用於從本機連線驗證)

### 2. 環境變數配置
在專案根目錄下創建 `.env` 文件(參考.env_template)，設定服務連接所需的參數：

```env
# ClickHouse 驗證與資料庫名稱
CLICKHOUSE_USER=${FOO}
CLICKHOUSE_PASSWORD=${BAR}
CLICKHOUSE_DB=analytic

# Redpanda/Kafka 外部端口
KAFKA_EXTERNAL_PORT=19092

# Kafka Topic 定義
ORDER_TOPIC=order
PAYMENT_TOPIC=payment
