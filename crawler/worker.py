from celery import Celery
from kombu import Exchange, Queue
from crawler.config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    WORKER_ACCOUNT,
    WORKER_PASSWORD,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
)

app = Celery(
    "task",
    # 只包含 tasks.py 裡面的程式, 才會成功執行
    include=[
        "crawler.tasks_etf_list_tw",
        "crawler.tasks_etf_list_us",
        "crawler.tasks_align", 
        "crawler.tasks_plan",
        "crawler.tasks_fetch",
        "crawler.tasks_tri",
        "crawler.tasks_backtests",
    ],
    # 連線到 rabbitmq,
    # pyamqp://user:password@127.0.0.1:5672/
    # 帳號密碼都是 worker
    broker=f"pyamqp://{WORKER_ACCOUNT}:{WORKER_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/",
    backend=f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"  # 使用 Redis
)

# app.conf.task_serializer = "pickle"
# app.conf.result_serializer = "pickle"
# app.conf.accept_content = ["json", "pickle"]

# ---- 佇列宣告（台股 / 美股）----
# 使用 direct exchange，routing_key 同 queue 名稱
tw_ex = Exchange("tw", type="direct")
us_ex = Exchange("us", type="direct")

app.conf.task_queues = (
    # TW queues
    Queue("tw_crawler",   exchange=tw_ex, routing_key="tw_crawler"),
    Queue("tw_align",     exchange=tw_ex, routing_key="tw_align"),
    Queue("tw_plan",      exchange=tw_ex, routing_key="tw_plan"),
    Queue("tw_fetch",     exchange=tw_ex, routing_key="tw_fetch"),
    Queue("tw_tri",       exchange=tw_ex, routing_key="tw_tri"),
    Queue("tw_backtest",  exchange=tw_ex, routing_key="tw_backtest"),

    # US queues
    Queue("us_crawler",   exchange=us_ex, routing_key="us_crawler"),
    Queue("us_align",     exchange=us_ex, routing_key="us_align"),
    Queue("us_plan",      exchange=us_ex, routing_key="us_plan"),
    Queue("us_fetch",     exchange=us_ex, routing_key="us_fetch"),
    Queue("us_tri",       exchange=us_ex, routing_key="us_tri"),
    Queue("us_backtest",  exchange=us_ex, routing_key="us_backtest"),
)

# 預設放台股爬蟲（避免未指定 queue 的任務亂飛）
app.conf.task_default_queue = "tw_crawler"
app.conf.task_default_exchange = "tw"
app.conf.task_default_exchange_type = "direct"
app.conf.task_default_routing_key = "tw_crawler"

# --- 路由規則 ---
# 只針對「不同名的 TW/US 名單任務」做固定路由；
# 其餘共用任務名稱，不在這裡固定，主程式用 apply_async(queue="tw_*/us_*") 指定即可。
app.conf.task_routes = {
    # 台股名單
    "crawler.tasks_etf_list_tw.fetch_tw_etf_list_task": {
        "queue": "tw_crawler", "routing_key": "tw_crawler"
    },
    # 美股名單（只有這個檔案跟台股不同）
    "crawler.tasks_etf_list_us.fetch_us_etf_list_task": {
        "queue": "us_crawler", "routing_key": "us_crawler"
    },
    # 其餘任務不在這裡路由，以主程式 apply_async(queue=...) 為準
}

# ---- 一些穩定性/效能建議（可依需要調整）----
app.conf.update(
    timezone="Asia/Taipei",
    enable_utc=False,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # 任務超時（硬/軟）
    task_time_limit=600,       # 硬超時：10 分鐘
    task_soft_time_limit=540,  # 軟超時：9 分鐘

    # 防止任務重取與壓爆
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    broker_heartbeat=30,
    broker_pool_limit=None,
    broker_transport_options={
        "visibility_timeout": 3600,  # 任務最多可被保留未確認 1 小時
    },
)