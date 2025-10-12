
-- =========================================================
-- 3. 查詢視圖 (v_metrics_minutely)
-- 目的: 簡化最終查詢，將 AggregatingMergeTree 的 State 欄位轉換為最終結果，並批次計算比率。
-- =========================================================
CREATE VIEW v_metrics_minutely AS
SELECT
    time_window,
    dimension_channel,

    -- 基礎指標 (SimpleAggregateFunction 直接作為普通欄位)
    gmv_sum,
    order_count,
    refund_sum,
    refunded_order_count,

    -- 轉換 AggregateFunction 狀態
    finalizeAggregation(unique_orders) AS unique_orders_final,
    finalizeAggregation(unique_users) AS unique_users_final,

    -- 批次/查詢時計算比率
    -- GMV 退款率: (Total Refund / Total GMV)
    if(gmv_sum > 0, refund_sum / gmv_sum, 0) AS gmv_refund_rate_ratio,

    -- 訂單退款率: (Refunded Order Count / Total Order Count)
    if(order_count > 0, refunded_order_count / order_count, 0) AS order_refund_rate_ratio

FROM metrics_minutely
ORDER BY time_window DESC;
