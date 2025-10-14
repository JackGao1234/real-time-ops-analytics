import json
import os
from kafka import KafkaProducer
from typing import Dict, Any


class BaseDataProducer:
    """
    Kafka 基礎資料生產者類別。
    負責初始化 Kafka 連線和數據發送的共同邏輯。
    """

    def __init__(self):
        # 從 .env 中讀取 Kafka 外部 Broker 配置
        self.kafka_broker = os.getenv("KAFKA_BROKER_EXTERNAL", "localhost:19092")
        self.order_topic = os.getenv("ORDER_TOPIC", "order_topic")
        self.payment_topic = os.getenv("PAYMENT_TOPIC", "payment_topic")
        self.producer = self._initialize_producer()

    def _initialize_producer(self):
        """初始化 Kafka Producer 連線"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # 增加請求超時時間，以應對大批量發送
                request_timeout_ms=10000,
                api_version=(0, 10, 1),
                retries=5,
            )
            print(f"✅ 成功連線 Kafka Broker: {self.kafka_broker}")
            return producer
        except Exception as e:
            print(f"❌ Kafka 連線失敗: {e}")
            raise

    def send_data(self, topic: str, data: Dict[str, Any]):
        """將字典資料發送到指定的 Kafka Topic"""
        try:
            # 異步發送，並確保數據送達
            future = self.producer.send(topic, value=data)
            # future.get(timeout=1) # 為了高吞吐量，我們不等待結果，讓它異步執行
            return future
        except Exception as e:
            print(f"❌ 發送數據到 {topic} 失敗: {e}")
            # 在高吞吐量情境下，可以選擇紀錄錯誤而不是退出

    def close(self):
        """關閉生產者連線"""
        if self.producer:
            self.producer.close()
            print("✅ Kafka Producer 連線已關閉。")
