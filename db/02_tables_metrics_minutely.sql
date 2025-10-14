-- =========================================================
-- 2. 即時聚合指標表 (metrics_minutely)
-- 目的: 儲存 GMV、訂單數和退款所需組件。
-- 引擎: AggregatingMergeTree，用於合併 AggregateFunction 狀態。
-- =========================================================
CREATE TABLE IF NOT EXISTS metrics_minutely (
    -- Primary Key / 聚合維度 (Aggregation Keys)
    time_window DateTime,                            -- 分鐘時間窗格
    event_source LowCardinality(String),              -- 聚合鍵 1
    event_type LowCardinality(String),                -- 聚合鍵 2
    dimension_channel LowCardinality(String),

    -- 聚合狀態欄位 (State Columns)

    -- 1. SimpleAggregateFunction: 寫入時傳入原始值，系統自動合併 (Sum)
    total_events_state SimpleAggregateFunction(sum, UInt64),  -- 總事件數 (Count)
    total_gmv_state SimpleAggregateFunction(sum, Float64),      -- GMV 總和 (Sum)

    -- 2. AggregateFunction: 寫入時必須傳入 State 形式 (如 arrayReduce('uniqState', ...))
    unique_users_state AggregateFunction(uniq, String),        -- 不重複用戶數的 State
    unique_orders_state AggregateFunction(uniq, String),        -- 不重複訂單數的 State

    -- 3. 退款相關 (SimpleAggregateFunction)
    refund_sum_state SimpleAggregateFunction(sum, Float64),         -- 該窗口的總退款金額 (Sum)
    refunded_order_count_state SimpleAggregateFunction(sum, UInt64) -- 該窗口發生的退款事件數 (Sum)

) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(time_window)
ORDER BY (time_window, event_source, event_type, dimension_channel); -- 排序鍵新增 event_source, event_type 以匹配 Processor 邏輯
