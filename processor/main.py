import datetime
import json
import logging
import os
import time

from clickhouse_connect import get_client
from dotenv import load_dotenv
from kafka import KafkaConsumer
from typing import List, Dict, Any, Tuple, Set


# TODO: 之後如果資料再度增長, 可考慮用Flink做即時處理data loading和metrics transformation
# Flink: 極低延遲 <-- 在資料量不到太多的情況下使用
# Spark streaming: 延遲約數秒, 但高吞吐 <-- 暫時不考量, 為更好的達到即時處理

# =========================================================
# 核心初始化函數
# =========================================================
def init_clients():
    """
    初始化 ClickHouse 客戶端和 Kafka Consumer。
    """
    print("--- 初始化連線客戶端 ---", flush=True)

    # 1. ClickHouse 配置
    CH_HOST = os.getenv("CH_HOST")
    CH_PORT = 8123
    CH_USER = os.getenv("CH_USER")
    CH_PASSWORD = os.getenv("CH_PASSWORD")
    CH_DATABASE = os.getenv("CH_DATABASE")

    # 初始化 ClickHouse 連線
    try:
        ch_client = get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE,
            connect_timeout=10,
        )
        # 簡單測試連線是否成功
        if ch_client.command("SELECT 1"):
            print(f"✅ ClickHouse 連線成功: {CH_HOST}:{CH_PORT}", flush=True)
        else:
            raise Exception("ClickHouse 連線測試失敗。")
    except Exception as e:
        print(f"❌ ClickHouse 連線失敗: {e}")
        import traceback

        traceback.print_exc()
        exit(1)

    # 2. Kafka Consumer 配置
    KAFKA_BROKER = os.getenv("KAFKA_BROKER_INTERNAL")
    KAFKA_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP")
    ORDER_TOPIC = os.getenv("ORDER_TOPIC")
    PAYMENT_TOPIC = os.getenv("PAYMENT_TOPIC")
    TOPICS = [ORDER_TOPIC, PAYMENT_TOPIC]

    # 初始化 Kafka Consumer
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=KAFKA_GROUP_ID,
            # 自動解碼 JSON 訊息
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        print(f"✅ Kafka Consumer 連線成功，訂閱主題: {TOPICS}")
    except Exception as e:
        print(f"❌ Kafka Consumer 連線失敗: {e}")
        exit(1)

    return ch_client, consumer


# =========================================================
# 3. 核心數據處理與寫入邏輯
# =========================================================

# 用於將 event_time 調整到分鐘開始的時間戳
def get_minute_start(dt: datetime.datetime) -> datetime.datetime:
    """將 datetime 物件的時間調整到該分鐘的開始（秒數歸零）"""
    return dt.replace(second=0, microsecond=0)


def format_set_to_clickhouse_array_str(data_set: Set[Any]) -> str:
    """
    將 Python Set 轉換為 ClickHouse Array 字串，
    並確保所有元素被單引號包圍。
    範例: {'u1', 'u2'} -> ['u1','u2']
    """
    if not data_set:
        return "[]"

    # 確保 set 中的每個元素都被轉換為字串並包上單引號
    elements_str = [f"'{str(item)}'" for item in data_set]

    # 使用 JOIN 將 ['u1', 'u2'] 變成 'u1','u2'
    return f"[{','.join(elements_str)}]"


def process_and_insert_batch(ch_client, batch: List[Any]) -> int:
    """
    接收一個批次的 Kafka 消息，同時寫入 raw_events 和 metrics_minutely。
    """
    raw_events_data = []
    # 使用字典進行批次內聚合：Key = (time_window, event_source, event_type, dimension_channel)
    metrics_aggregation: Dict[
        Tuple[datetime.datetime, str, str, str], Dict[str, Any]
    ] = {}

    # --- 1. 數據清洗與預處理 ---
    for message in batch:
        event_data = message.value

        event_time_str = event_data.get("event_time")
        if not event_time_str:
            print("❌ 嚴重警告：事件缺少 'event_time' 字段，跳過此消息。")
            continue

        try:
            # 處理時間解析
            event_time = datetime.datetime.fromisoformat(event_time_str)
            if event_time.tzinfo is not None:
                event_time = event_time.astimezone(datetime.timezone.utc).replace(
                    tzinfo=None
                )
        except ValueError:
            print(f"❌ 嚴重警告：無法解析事件時間 {event_time_str}，跳過此消息。")
            continue

        event_id = event_data.get("event_id")
        channel = event_data.get("dimension_channel", "Unknown")
        event_source = message.topic
        event_type = event_data.get("event_type")
        order_id = str(event_data.get("order_id", ""))

        user_id = str(event_data.get("user_id", ""))

        payload_json = json.dumps(event_data)
        is_deleted = 0

        # 寫入 raw_events 的數據格式
        raw_events_data.append(
            (
                event_id,
                event_time,
                event_source,
                event_type,
                order_id,
                user_id,
                channel,
                payload_json,
                is_deleted,
            )
        )

        time_window = get_minute_start(event_time)

        amount_value = event_data.get("amount", 0.0)

        gmv_value = 0.0
        refund_amount_value = 0.0

        # 根據事件類型分配金額
        # 這裡假設 order/payment 事件類型為 "order" 和 "payment"
        if (
            event_data.get("source") in ["LowLoadTest", "HighLoadTest"]
            and event_type == "order"
        ):
            gmv_value = amount_value  # 記為 GMV
        elif event_type == "payment_refund":
            refund_amount_value = amount_value

        agg_key = (time_window, event_source, event_type, channel)

        if agg_key not in metrics_aggregation:
            metrics_aggregation[agg_key] = {
                "count": 0,
                "sum_gmv": 0.0,
                "unique_users": set(),
                "unique_orders": set(),
                "refund_sum": 0.0,
                "refund_count": 0,
            }

        agg_state = metrics_aggregation[agg_key]
        agg_state["count"] += 1

        agg_state["sum_gmv"] += gmv_value

        agg_state["unique_users"].add(user_id)
        agg_state["unique_orders"].add(order_id)

        if event_type == "payment_refund":
            agg_state["refund_sum"] += refund_amount_value
            agg_state["refund_count"] += 1

    # --- 3. 執行 ClickHouse 寫入 ---

    # 3a. 寫入 raw_events
    if raw_events_data:
        try:
            ch_client.insert(
                "raw_events",
                raw_events_data,
                column_names=[
                    "event_id",
                    "event_time",
                    "event_source",
                    "event_type",
                    "order_id",
                    "user_id",
                    "dimension_channel",
                    "payload_json",
                    "is_deleted",
                ],
            )
            print(f"  -> 成功寫入 {len(raw_events_data)} 條 raw_events.")
        except Exception as e:
            print(f"❌ ClickHouse 寫入 raw_events 失敗: {e}")

    # 3b. 準備並寫入 metrics_minutely (AggregatingMergeTree)
    values_list = []
    if not metrics_aggregation:
        return len(raw_events_data)

    for (
        time_window,
        event_source,
        event_type,
        dimension_channel,
    ), state in metrics_aggregation.items():

        # 1. 轉換維度
        time_str = time_window.strftime("toDateTime('%Y-%m-%d %H:%M:%S')")
        source_str = f"'{event_source}'"
        type_str = f"'{event_type}'"
        channel_str = f"'{dimension_channel}'"

        # 2. 轉換 SimpleAggregateFunction 狀態 (直接使用原始值)
        count_str = str(state["count"])
        gmv_str = str(state["sum_gmv"])
        refund_sum_str = str(state["refund_sum"])
        refund_count_str = str(state["refund_count"])

        # 3. 轉換 AggregateFunction 狀態 (這是突破點！)
        # 使用輔助函數將 Set 轉為 ClickHouse 陣列字串
        user_arr_str = format_set_to_clickhouse_array_str(state["unique_users"])
        order_arr_str = format_set_to_clickhouse_array_str(state["unique_orders"])

        # 組合 ClickHouse 內建函數來產生狀態
        # 範例: arrayReduce('uniqState', ['user1','user2'])
        unique_users_state_str = f"arrayReduce('uniqState', {user_arr_str})"
        unique_orders_state_str = f"arrayReduce('uniqState', {order_arr_str})"

        # 組合完整的 VALUES 行
        row_values = [
            time_str,
            source_str,
            type_str,
            channel_str,
            count_str,
            gmv_str,
            refund_sum_str,
            refund_count_str,
            unique_users_state_str,
            unique_orders_state_str,
        ]
        values_list.append(f"({','.join(row_values)})")

    processed_metrics_count = 0
    if values_list:
        try:
            start_time = time.time()

            # 構造 INSERT INTO VALUES 語句
            insert_sql_values = f"""
                INSERT INTO metrics_minutely (
                    time_window, event_source, event_type, dimension_channel,
                    total_events_state, total_gmv_state,
                    refund_sum_state, refunded_order_count_state,
                    unique_users_state, unique_orders_state
                )
                VALUES {','.join(values_list)}
            """

            ch_client.command(insert_sql_values)

            end_time = time.time()
            processed_metrics_count = len(values_list)
            print(
                f"  -> 成功寫入 {processed_metrics_count} 條 metrics_minutely (耗時: {end_time - start_time:.3f}s)"
            )

        except Exception as e:
            print(f"❌ ClickHouse 寫入 metrics_minutely 失敗: {e}")
            import traceback

            traceback.print_exc()

    return len(raw_events_data)


# =========================================================
# 4. 主循環
# =========================================================
def main():
    ch_client, consumer = init_clients()

    POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", 1000))
    total_processed = 0

    print("--- 實時分析處理器啟動，等待事件流 ---", flush=True)

    while True:
        # 使用 consumer.poll() 進行批次拉取 (這是關鍵的性能優化)
        # timeout_ms 定義了 poll 阻塞的最大時間
        raw_batches = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=5000)

        all_messages = []

        # 將所有 Topic/Partition 的訊息收集到一個列表
        for tp, messages in raw_batches.items():
            all_messages.extend(messages)

        if all_messages:
            # 處理整個批次並寫入 ClickHouse
            processed_count = process_and_insert_batch(ch_client, all_messages)
            total_processed += processed_count

            # 提交 Kafka Offset
            consumer.commit()

            print(f"總處理量: {total_processed} 條. (當前批次: {processed_count} 條)", flush=True)
        else:
            time.sleep(0.5)  # 沒有數據時小憩


if __name__ == "__main__":
    main()
