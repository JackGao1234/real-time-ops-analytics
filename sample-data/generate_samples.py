import json
import time
import random
import os
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# 導入 Pydantic V2 相關模塊
from pydantic import BaseModel, Field, field_validator, ValidationError
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

# =========================================================
# 1. 配置與常量
# =========================================================

# Redpanda/Kafka Broker 連接點 (外部端口)
KAFKA_BROKER = f"localhost:{os.getenv('KAFKA_EXTERNAL_PORT', '19092')}"
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "order")
PAYMENT_TOPIC = os.getenv("PAYMENT_TOPIC", "payment")

CHANNELS = ["iOS", "Android", "Web", "Facebook_Ads", "Email_Marketing", "Google_Search"]

# =========================================================
# 2. Pydantic Schema (數據結構規範與驗證)
# =========================================================


class EventSchema(BaseModel):
    """定義發送到 Kafka 的事件數據結構"""

    event_id: str = Field(..., description="事件的唯一ID，用於下游去重")
    event_time: str = Field(..., description="事件發生時間 (ISO 8601 格式，含時區)")
    event_source: str = Field(..., description="事件類型：order 或 payment")
    event_type: str = Field(..., description="具體事件，如 order_created, payment_success")
    order_id: str
    user_id: str
    dimension_channel: str
    gmv: float = Field(..., description="交易金額絕對值 (GMV 或 Refund Amount)")

    # 使用 Pydantic V2 的 @field_validator 取代 @validator
    @field_validator("event_time", mode="before")
    @classmethod
    def check_event_time_format(cls, v: str) -> str:
        """驗證並確保 event_time 格式為 ISO 8601 且包含時區資訊"""
        try:
            # 嘗試解析為帶有時區的 datetime 物件
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                # 如果沒有時區，假設為 UTC (或可自行定義本地時區)
                dt = dt.replace(tzinfo=timezone.utc)

            # 統一輸出為帶有時區的 ISO 格式
            return dt.isoformat()
        except ValueError as e:
            raise ValueError(f"event_time 必須是有效的 ISO 8601 格式: {e}")


# =========================================================
# 3. 數據生成與發送邏輯
# =========================================================


def generate_sample_event(
    order_id: str,
    event_source: str,
    event_type: str,
    base_time: datetime,
    is_late: bool = False,
    is_duplicate: bool = False,
) -> tuple[str, str]:
    """生成單個事件的 JSON 字串和其 Topic 名稱。"""

    # 1. 時間戳記處理 (Time Jitter for late events)
    if is_late:
        # 模擬延遲/亂序：事件發生於 5-10 分鐘前，但現在才被發送
        event_time_dt = base_time - timedelta(minutes=random.uniform(5, 10))
    else:
        # 正常事件
        event_time_dt = base_time

    # 確保所有時間戳記帶有時區資訊 (此處統一使用 UTC)
    event_time_dt = event_time_dt.astimezone(timezone.utc)
    event_time = event_time_dt.isoformat()

    # 2. 事件 ID 處理 (Event ID for deduplication)
    if is_duplicate:
        # 重複事件使用特定的 ID，方便測試去重邏輯
        event_id = f"{order_id}-{event_type}-DUPLICATE"
    else:
        # 唯一 ID
        event_id = f"{order_id}-{event_type}-{random.randint(1000, 9999)}"

    # 3. GMV/金額處理
    # Processor 需根據 event_type 處理退款（payment_refund）的負號，此處發送絕對金額
    amount = round(random.uniform(50, 500), 2)

    # 4. 構建數據
    event_data = {
        "event_id": event_id,
        "event_time": event_time,
        "event_source": event_source,
        "event_type": event_type,
        "order_id": order_id,
        "user_id": f"user_{random.randint(100, 500)}",
        "dimension_channel": random.choice(CHANNELS),
        "gmv": amount,
    }

    # 5. 驗證並序列化為 JSON 字串
    validated_data = EventSchema(**event_data)
    # model_dump_json() 包含 Pydantic 驗證，直接輸出 JSON 字串
    return validated_data.model_dump_json(), event_source


def produce_messages(producer: KafkaProducer, num_total_orders: int = 30):
    """
    生成 num_total_orders 筆訂單的事件流。
    每個訂單至少產生 'order_created' 和 'payment_success' 兩個事件。
    """

    total_messages_sent = 0
    print(f"--- 開始生成 {num_total_orders} 筆訂單的事件流 (Broker: {KAFKA_BROKER}) ---")
    start_time = datetime.now()

    for i in range(1, num_total_orders + 1):
        order_id = f"ORDER-{100000 + i}"

        # 基礎訂單時間：模擬最近 5 分鐘內發生的事件 (作為事件時間的參考點)
        base_time = datetime.now() - timedelta(seconds=random.randint(60, 300))

        # ----------------------------------------------------
        # 1. 核心事件流：建立訂單 -> 支付成功
        # ----------------------------------------------------

        # A. 訂單創建
        msg_order_created, topic_order = generate_sample_event(
            order_id, ORDER_TOPIC, "order_created", base_time
        )
        producer.send(topic_order, msg_order_created)
        total_messages_sent += 1

        # B. 支付成功 (發生在訂單創建後 10 秒)
        msg_payment_success, topic_payment = generate_sample_event(
            order_id,
            PAYMENT_TOPIC,
            "payment_success",
            base_time + timedelta(seconds=10),
        )
        producer.send(topic_payment, msg_payment_success)
        total_messages_sent += 1

        # ----------------------------------------------------
        # 2. 測試點：重複事件 (測試去重邏輯)
        # ----------------------------------------------------
        if i % 10 == 0:
            # 重複發送支付成功事件，使用相同的 event_id 標記
            duplicate_message, topic_dup = generate_sample_event(
                order_id,
                PAYMENT_TOPIC,
                "payment_success",
                base_time + timedelta(seconds=10),
                is_duplicate=True,
            )
            producer.send(topic_dup, duplicate_message)
            print(f"  -> 測試點: 發送 重複事件 ({order_id}, payment_success)")
            total_messages_sent += 1

        # ----------------------------------------------------
        # 3. 測試點：延遲事件 (測試亂序處理邏輯)
        # ----------------------------------------------------
        if i % 5 == 0:
            # 模擬一個延遲到來的退款事件 (event_time 較早，但現在才發送)
            late_message, topic_late = generate_sample_event(
                order_id, PAYMENT_TOPIC, "payment_refund", base_time, is_late=True
            )
            producer.send(topic_late, late_message)
            print(f"  -> 測試點: 發送 延遲事件 ({order_id}, payment_refund)")
            total_messages_sent += 1

        time.sleep(random.uniform(0.01, 0.05))  # 模擬生產間隔

    producer.flush()  # 確保所有緩衝區中的訊息都被發送
    end_time = datetime.now()
    print(
        f"\n--- 成功發送 {total_messages_sent} 條訊息到 Redpanda，耗時: {(end_time - start_time).total_seconds():.2f}s ---"
    )


if __name__ == "__main__":
    try:
        print(f"連接 Broker: {KAFKA_BROKER}")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            api_version=(0, 10, 1),
            # 序列化器：將 Python 物件/字串轉為 UTF-8 bytes
            value_serializer=lambda v: v.encode("utf-8")
            if isinstance(v, str)
            else json.dumps(v).encode("utf-8"),
        )
        # 執行發送
        produce_messages(producer, num_total_orders=30)
    except Exception as e:
        print(f"\n[!] 錯誤: 與 Kafka Broker ({KAFKA_BROKER}) 連接失敗。")
        print(
            f"[!] 請檢查 Redpanda 容器 ({os.getenv('REDANDA_CONTAINER_NAME', 'redpanda')}) 是否運行在 {KAFKA_BROKER}。"
        )
        print(f"錯誤詳情: {e}")
