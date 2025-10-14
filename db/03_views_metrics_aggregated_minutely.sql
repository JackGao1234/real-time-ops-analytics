CREATE VIEW IF NOT EXISTS metrics_aggregated_minutely AS
SELECT
    -- 1. 最終輸出維度
    t1.time_window,
    t1.dimension_channel,

    -- 2. 指標聚合
    SUM(t1.total_events_count) AS total_events_count,
    SUM(t1.total_gmv_sum) AS total_gmv_sum,
    SUM(t1.total_refund_sum) AS total_refund_sum,
    SUM(t1.total_refunded_order_count) AS total_refunded_order_count,

    -- 3. 終止 AggregateFunction 指標 (近似處理)
    MAX(t1.unique_orders_count) AS unique_orders_count,
    MAX(t1.unique_users_count) AS unique_users_count,

    -- 4. 衍生比率計算
    if(SUM(t1.total_gmv_sum) > 0, SUM(t1.total_refund_sum) / SUM(t1.total_gmv_sum), 0) AS gmv_refund_rate_ratio,
    if(SUM(t1.total_events_count) > 0, SUM(t1.total_refunded_order_count) / SUM(t1.total_events_count), 0) AS order_refund_rate_ratio

FROM (
    -- 第一階段 (t1): 終止聚合狀態，保留所有維度
    SELECT
        time_window,
        event_source,
        event_type,
        dimension_channel,

        SUM(total_events_state) AS total_events_count,
        SUM(total_gmv_state)    AS total_gmv_sum,
        SUM(refund_sum_state) AS total_refund_sum,
        SUM(refunded_order_count_state) AS total_refunded_order_count,

        uniqMerge(unique_orders_state) AS unique_orders_count,
        uniqMerge(unique_users_state)  AS unique_users_count

    FROM metrics_minutely
    GROUP BY
        time_window,
        event_source,
        event_type,
        dimension_channel
) AS t1
-- 第二階段聚合：只保留 time_window 和 dimension_channel
GROUP BY
    t1.time_window,
    t1.dimension_channel

ORDER BY t1.time_window DESC;
