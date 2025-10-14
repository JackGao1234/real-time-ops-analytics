from datetime import datetime, timezone

# 導入要測試的核心函數
from processor.main import get_minute_start

# 模擬一個假的 Kafka 消息列表
# TODO: 之後可以用這個來做測試
MOCK_BATCH = [
    # 模擬 10:05:30 的訂單
    {
        "value": {
            "event_time": "2025-10-15T10:05:30Z",
            "user_id": 100,
            "order_id": "O1",
            "amount": 100.0,
            "event_type": "order",
            "dimension_channel": "Web",
        },
        "topic": "order_topic",
    },
    # 模擬 10:05:45 的訂單 (用戶和 Key 相同)
    {
        "value": {
            "event_time": "2025-10-15T10:05:45Z",
            "user_id": 100,
            "order_id": "O2",
            "amount": 200.0,
            "event_type": "order",
            "dimension_channel": "Web",
        },
        "topic": "order_topic",
    },
    # 模擬 10:06:10 的訂單 (Key 不同 - 時間窗不同)
    {
        "value": {
            "event_time": "2025-10-15T10:06:10Z",
            "user_id": 200,
            "order_id": "O3",
            "amount": 50.0,
            "event_type": "order",
            "dimension_channel": "Web",
        },
        "topic": "order_topic",
    },
]


def test_aggregation_logic():
    # 測試時間窗劃分
    dt_in = datetime(2025, 10, 15, 10, 5, 30, tzinfo=timezone.utc).replace(tzinfo=None)
    dt_out = get_minute_start(dt_in)
    assert dt_out.minute == 5
    assert dt_out.second == 0
