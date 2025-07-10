from crawler.tasks_etf_list_us import tasks_etf_list_us  
from crawler.tasks_crawler_etf_us import tasks_crawler_etf_us
from crawler.tasks_backtest_utils_us import tasks_backtest_utils_us    
from crawler.tasks_crawler_etf__dps_us import tasks_crawler_etf__dps_us      

if __name__ == "__main__":
    print("ETF 清單")
    tasks_etf_list_us()
  
    print("歷史價格")
    tasks_crawler_etf_us()
    
    print("技術指標與績效分析")
    tasks_backtest_utils_us()

    print("配息資料")
    tasks_crawler_etf__dps_us()
