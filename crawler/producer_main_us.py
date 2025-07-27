from crawler.tasks_etf_list_us import etf_list_us  
from crawler.tasks_crawler_etf_us import crawler_etf_us
from crawler.tasks_backtest_utils_us import backtest_utils_us    
from crawler.tasks_crawler_etf_dps_us import crawler_etf_dps_us      


if __name__ == "__main__":
    us_etf_url="https://tw.tradingview.com/markets/etfs/funds-usa/"
    print("ETF 清單")
    etf_list_us = etf_list_us.apply_async(
        args=[us_etf_url], queue="crawler_us"
    )  # 使用 apply_async 發送任務到 RabbitMQ
    # etf_list_us = etf_list_us(us_etf_url)  # 直接呼叫函式

    print("歷史價格")
    crawler_etf_us = crawler_etf_us.apply_async(args=[us_etf_url], queue="crawler_us")

    print("歷史價格、技術指標與績效分析")
    backtest_utils_us = backtest_utils_us.apply_async(
        args=[us_etf_url], queue="crawler_us"
    )
    # backtest_utils_us = backtest_utils_us(us_etf_url)  # 直接呼叫函式

    print("配息資料")
    crawler_etf_dps_us = crawler_etf_dps_us.apply_async(
        args=[us_etf_url], queue="crawler_us"
    )
    # crawler_etf_dps_us = crawler_etf_dps_us(us_etf_url)  # 直接呼叫函式
