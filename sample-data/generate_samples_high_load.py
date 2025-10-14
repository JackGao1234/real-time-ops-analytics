import time
import uuid
import random
import os
from datetime import datetime, timezone
from producer_base import BaseDataProducer

# 模擬每秒 100 筆資料的目標
TARGET_RATE_PER_SECOND = 100
BATCH_SIZE = 10
DELAY_PER_BATCH = 1 / (TARGET_RATE_PER_SECOND / BATCH_SIZE)  # 0.1 秒發送 10 筆

CHANNELS = ["Web", "Mobile_App", "Retail_Store", "Partner_API"]


class HighThroughputProducer(BaseDataProducer):
    """
    產生每秒 100 筆事件的高吞吐量數據生產者。
    """

    def generate_random_event(self, event_type: str):
        """產生單一的模擬事件數據"""
        event_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        return {
            "event_id": str(uuid.uuid4()),
            "event_time": event_time,
            "user_id": random.randint(1000, 9999),
            "event_type": event_type,
            "dimension_channel": random.choice(CHANNELS),
            "data": {
                "amount": round(random.uniform(5.0, 500.0), 2)
                if event_type == "order"
                else None,
                "status": random.choice(["success", "failed", "pending"])
                if event_type == "payment"
                else None,
            },
            "source": "HighLoadTest",
        }

    def run(self):
        """主運行迴圈，以目標速率發送數據"""
        print(f"🚀 正在以每秒 {TARGET_RATE_PER_SECOND} 筆的速率啟動高負載測試...")

        try:
            while True:
                start_time = time.time()

                # 在一個批次中產生並發送多筆資料
                for _ in range(BATCH_SIZE):
                    # 模擬 Order 事件
                    order_data = self.generate_random_event("order")
                    self.send_data(self.order_topic, order_data)

                    # 模擬 Payment 事件 (簡單起見，暫時讓它佔用一個批次計數)
                    payment_data = self.generate_random_event("payment")
                    self.send_data(self.payment_topic, payment_data)

                end_time = time.time()
                time_taken = end_time - start_time

                # 計算需要等待的時間以達到目標速率
                sleep_time = DELAY_PER_BATCH - time_taken

                if sleep_time > 0:
                    time.sleep(sleep_time)

                # print(f"發送了 {BATCH_SIZE} 筆資料, 耗時 {time_taken:.4f}s")

        except KeyboardInterrupt:
            print("\n👋 停止高負載數據生產。")
        finally:
            self.close()


if __name__ == "__main__":
    # 確保當前工作目錄在 sample-data/ 之下，才能正確導入 producer_base
    if os.path.basename(os.getcwd()) != "sample-data":
        # 假設從專案根目錄執行，我們需要調整 Python 路徑
        import sys

        sys.path.append(os.path.join(os.path.dirname(__file__)))

    producer = HighThroughputProducer()
    producer.run()
