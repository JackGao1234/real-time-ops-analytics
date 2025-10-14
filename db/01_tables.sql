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

    -- 事件屬性 (使用 LowCardinality 節省空間，加速 WHERE 過濾)
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
    -- Primary Key / 聚合維度 (Aggregation Keys)
    time_window DateTime,                            -- 分鐘時間窗格
    event_source LowCardinality(String),              -- 聚合鍵 1 (新增)
    event_type LowCardinality(String),                -- 聚合鍵 2 (新增)
    dimension_channel LowCardinality(String),

    -- 聚合狀態欄位 (State Columns)

    -- 1. SimpleAggregateFunction: 寫入時傳入原始值，系統自動合併 (Sum)
    total_events_state SimpleAggregateFunction(sum, UInt64),  -- 總事件數 (Count) (欄位名稱調整)
    total_gmv_state SimpleAggregateFunction(sum, Float64),      -- GMV 總和 (Sum) (欄位名稱調整)

    -- 2. AggregateFunction: 寫入時必須傳入 State 形式 (如 arrayReduce('uniqState', ...))
    unique_users_state AggregateFunction(uniq, String),        -- 不重複用戶數的 State (欄位名稱調整)
    unique_orders_state AggregateFunction(uniq, String),        -- 不重複訂單數的 State (欄位名稱調整)

    -- 3. 退款相關 (SimpleAggregateFunction)
    refund_sum_state SimpleAggregateFunction(sum, Float64),         -- 該窗口的總退款金額 (Sum) (欄位名稱調整)
    refunded_order_count_state SimpleAggregateFunction(sum, UInt64) -- 該窗口發生的退款事件數 (Sum) (欄位名稱調整)

) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(time_window)
ORDER BY (time_window, event_source, event_type, dimension_channel); -- 排序鍵新增 event_source, event_type 以匹配 Processor 邏輯
