from crawler.tasks_etf_list_us import US_ETF_list  
from crawler.tasks_crawler_etf_us import get_etf_list
from crawler.tasks_backtest_utils_us import US_ETF_Yahoo_Download    
from crawler.tasks_crawler_etf__dps_us import US_ETF_Yahoo_DPS      

if __name__ == "__main__":
    print("ETF 清單")
    US_ETF_list.apply_async()

    print("歷史價格")
    get_etf_list.apply_async()
    
    print("技術指標與績效分析")
    US_ETF_Yahoo_Download.apply_async()

    print("配息資料")
    US_ETF_Yahoo_DPS.apply_async()
