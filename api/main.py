import os
import time
from datetime import datetime
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from clickhouse_connect import get_client
from pydantic import BaseModel, Field

# 嘗試加載 .env 文件（用於本地開發或測試）
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# =========================================================
# 數據模型定義 (Pydantic Schemas)
# =========================================================


class RawEventsCount(BaseModel):
    """原始事件總數"""

    total_count: int = Field(..., description="raw_events 表格中的總事件數量")


class RawEventsSample(BaseModel):
    """原始事件採樣記錄"""

    dimension_channel: str
    event_source: str


class UniqueUsersMetric(BaseModel):
    """分鐘級獨立用戶數指標"""

    minute_window: datetime = Field(..., description="分鐘時間窗的起始時間 (UTC)")
    unique_users_count: int = Field(..., description="該分鐘內計算出的獨立用戶總數")


# =========================================================
# 服務初始化與 ClickHouse 連線
# =========================================================

# 初始化 ClickHouse 客戶端
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = 8123
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DATABASE = os.getenv("CLICKHOUSE_DB", "analytic_db")

ch_client = None


def init_clickhouse(max_retries=10, delay_seconds=3):
    """初始化 ClickHouse 客戶端，並帶有重試機制"""
    global ch_client
    print(f"嘗試連線 ClickHouse: {CH_HOST}:{CH_PORT}", flush=True)

    for attempt in range(max_retries):
        try:
            ch_client = get_client(
                host=CH_HOST,
                port=CH_PORT,
                username=CH_USER,
                password=CH_PASSWORD,
                database=CH_DATABASE,
                connect_timeout=5,
            )
            # 測試連線
            ch_client.command("SELECT 1")
            print(f"✅ API 服務：ClickHouse 連線成功 (嘗試 {attempt + 1})", flush=True)
            return
        except Exception as e:
            print(f"❌ ClickHouse 連線失敗 (嘗試 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"等待 {delay_seconds} 秒後重試...")
                time.sleep(delay_seconds)
            else:
                print("❌ 達到最大重試次數，ClickHouse 連線失敗。", flush=True)
                ch_client = None


init_clickhouse()
app = FastAPI(
    title="Real-Time Analytics API",
    description="提供分鐘級實時 GMV、事件數和獨立用戶等核心營運指標查詢。",
    version="1.0.0",
)


# =========================================================
# API 路由定義
# =========================================================


@app.get("/", summary="健康檢查", tags=["System"])
def health_check():
    """檢查 API 服務和 ClickHouse 連線狀態"""
    if ch_client:
        try:
            ch_client.command("SELECT 1")
            return {"status": "ok", "db_status": "connected"}
        except Exception:
            return {"status": "ok", "db_status": "disconnected"}
    return {"status": "ok", "db_status": "uninitialized"}


@app.get(
    "/raw/count", response_model=RawEventsCount, summary="獲取原始事件總數", tags=["Raw Data"]
)
def get_raw_events_count():
    """對應 QA 查詢：SELECT count() FROM raw_events;"""
    if not ch_client:
        raise HTTPException(
            status_code=503, detail="Database connection is not available."
        )

    query = "SELECT count() FROM raw_events"
    result = ch_client.command(query)

    return {"total_count": int(result)}


@app.get(
    "/raw/sample",
    response_model=List[RawEventsSample],
    summary="獲取原始事件採樣 (Limit 5)",
    tags=["Raw Data"],
)
def get_raw_events_sample():
    """對應 QA 查詢：SELECT dimension_channel, event_source FROM raw_events LIMIT 5;"""
    if not ch_client:
        raise HTTPException(
            status_code=503, detail="Database connection is not available."
        )

    query = "SELECT dimension_channel, event_source FROM raw_events LIMIT 5"
    result = ch_client.query(query)

    # 手動將結果行和欄位名稱合併為字典列表
    column_names = result.column_names
    data = [dict(zip(column_names, row)) for row in result.result_rows]

    return data


@app.get(
    "/metrics/unique_users",
    response_model=List[UniqueUsersMetric],
    summary="獲取分鐘級獨立用戶數",
    tags=["Metrics"],
)
def get_unique_users(limit: int = Query(10, description="限制返回最近多少個分鐘窗的數據", ge=1)):
    """
    對應 QA 查詢：SELECT toStartOfMinute(time_window), uniqMerge(unique_users_state)
    FROM metrics_minutely FINAL GROUP BY 1 ORDER BY 1 DESC LIMIT N;
    """
    if not ch_client:
        raise HTTPException(
            status_code=503, detail="Database connection is not available."
        )

    query = f"""
        SELECT
            toStartOfMinute(time_window) AS minute_window,
            uniqMerge(unique_users_state) AS unique_users_count
        FROM metrics_minutely
        FINAL
        GROUP BY minute_window
        ORDER BY minute_window DESC
        LIMIT {limit}
    """

    result = ch_client.query(query)

    # 手動將結果行和欄位名稱合併為字典列表
    column_names = result.column_names
    data = [dict(zip(column_names, row)) for row in result.result_rows]

    return data
