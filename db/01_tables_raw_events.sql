-- 假設這是您的 db/01_raw_events.sql 檔案
CREATE TABLE IF NOT EXISTS raw_events (
    -- 核心 ID 與時間戳
    event_id String,
    event_time DateTime,
    ingest_time DateTime DEFAULT now(),

    -- 事件屬性
    event_source LowCardinality(String),
    event_type LowCardinality(String),

    -- 業務 ID
    order_id String,
    user_id String,

    dimension_channel LowCardinality(String),

    -- 原始資料體
    payload_json String,

    -- 邏輯刪除標記
    is_deleted UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_id, event_time)
SETTINGS index_granularity = 8192;
