from celery import Celery

from crawler.config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    WORKER_ACCOUNT,
    WORKER_PASSWORD,
)

app = Celery(
    "task",
    # 只包含 tasks.py 裡面的程式, 才會成功執行
    include=[
        "crawler.tasks_etf_list_tw",
        "crawler.tasks_crawler_etf_tw", 
        "crawler.tasks_backtest_utils_tw",
        
        "crawler.tasks_etf_list_us",
        "crawler.tasks_crawler_etf_us",
        "crawler.tasks_backtest_utils_us",
        "crawler.tasks_crawler_etf_dps_us"

    ],
    # 連線到 rabbitmq,
    # pyamqp://user:password@127.0.0.1:5672/
    # 帳號密碼都是 worker
    broker=f"pyamqp://{WORKER_ACCOUNT}:{WORKER_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/",

)
