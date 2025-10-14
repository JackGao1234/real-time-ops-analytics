import time
import uuid
import random
import os
from datetime import datetime, timezone
from producer_base import BaseDataProducer

# 模擬低負載：每 1 秒發送 1 筆
DELAY_PER_EVENT = 1

# 新增渠道維度列表
CHANNELS = ["Web", "Mobile_App", "Retail_Store", "Partner_API"]


class LowThroughputProducer(BaseDataProducer):
    """
    產生低頻率（每秒約 1 筆）事件的數據生產者。
    """

    def generate_random_event(self, event_type: str):
        """產生單一的模擬事件數據"""
        event_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        return {
            "event_id": str(uuid.uuid4()),
            "event_time": event_time,
            "user_id": random.randint(100, 999),
            "event_type": event_type,
            "dimension_channel": random.choice(CHANNELS),
            "data": {
                "amount": round(random.uniform(10.0, 1000.0), 2)
                if event_type == "order"
                else None,
                "items_count": random.randint(1, 5) if event_type == "order" else None,
                "duration_ms": random.randint(50, 500)
                if event_type == "payment"
                else None,
            },
            "source": "LowLoadTest",
        }

    def run(self):
        """主運行迴圈，以目標速率發送數據"""
        print(f"🐌 正在以每 {DELAY_PER_EVENT} 秒 1 筆的速率啟動低負載測試...")

        try:
            while True:
                # 模擬 Order 事件
                order_data = self.generate_random_event("order")
                # print(f"SEND: {order_data}") # 可以打開這個看數據格式
                self.send_data(self.order_topic, order_data)

                # 模擬 Payment 事件
                payment_data = self.generate_random_event("payment")
                self.send_data(self.payment_topic, payment_data)

                # 等待，以控制速率
                time.sleep(DELAY_PER_EVENT)

        except KeyboardInterrupt:
            print("\n👋 停止低負載數據生產。")
        finally:
            self.close()


if __name__ == "__main__":
    # 確保當前工作目錄在 sample-data/ 之下
    if os.path.basename(os.getcwd()) != "sample-data":
        import sys

        # 確保 BaseDataProducer 可以被找到
        sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

    # 必須確保程式碼在專案根目錄執行，或正確配置 PYTHONPATH
    # 由於我們使用 make producer 在專案根目錄執行，所以這裡只需要初始化
    producer = LowThroughputProducer()
    producer.run()
