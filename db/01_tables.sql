-- =========================================================
-- 1. 原始事件表 (raw_events)
-- 目的: 儲存所有進入系統的事件。
-- 引擎: ReplacingMergeTree，確保相同的 event_id 只有一筆記錄。
-- =========================================================
CREATE TABLE IF NOT EXISTS raw_events (
    -- 核心 ID 與時間戳
    event_id String,                  -- 事件的唯一 ID，用於全局去重
    event_time DateTime,              -- 事件在來源端實際發生的時間
    ingest_time DateTime DEFAULT now(), -- 事件進入ClickHouse的時間

    -- 事件屬性
    event_source LowCardinality(String), -- 事件來源 (order, payment)
    event_type LowCardinality(String),   -- 事件具體類型 (order_created, payment_success, refund, etc.)

    -- 業務 ID
    order_id String,
    user_id String,

    -- 原始資料體
    payload_json String,

    -- 邏輯刪除標記
    is_deleted UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_id, event_time)
SETTINGS index_granularity = 8192;


-- =========================================================
-- 2. 即時聚合指標表 (metrics_minutely)
-- 目的: 儲存 GMV、訂單數和退款所需組件。
-- 引擎: AggregatingMergeTree，用於合併 AggregateFunction 狀態。
-- =========================================================
CREATE TABLE IF NOT EXISTS metrics_minutely (
    -- Primary Key / 聚合維度
    time_window DateTime,
    dimension_channel LowCardinality(String),

    -- GMV 相關 (分母)
    gmv_sum SimpleAggregateFunction(sum, Float64), -- 該窗口的總銷售額 (GMV)

    -- 訂單數/計數相關
    order_count SimpleAggregateFunction(sum, UInt64), -- 總訂單數 (Order Count)
    unique_orders AggregateFunction(uniq, String),   -- 不重複訂單數
    unique_users AggregateFunction(uniq, String),    -- 不重複用戶數

    -- 退款相關
    refund_sum SimpleAggregateFunction(sum, Float64),         -- 該窗口的總退款金額 (Numerator for GMV Rate)
    refunded_order_count SimpleAggregateFunction(sum, UInt64) -- 該窗口發生的退款事件數 (Numerator for Order Rate)

) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(time_window)
ORDER BY (time_window, dimension_channel);
