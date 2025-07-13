# 📊 ETF 分析與回測平台（台股 + 美股）

本專案是一套針對台股與美股 ETF 的 **自動化資料擷取、技術分析與回測模擬平台**。整合了 [Yahoo 台股 ETF](https://tw.stock.yahoo.com/tw-etf) 與 [Yahoo 美股 ETF](https://tw.stock.yahoo.com/) 上市標的，提供每日更新的歷史資料與策略評估，協助使用者了解長期投資的成效與風險。

---

## 🎯 專案目標

- 📈 回測「台股 vs 美股」ETF 長期投資表現
- 📉 比較「定期定額 vs 一次投入」投資策略
- 🔁 建立每日自動更新資料的系統
- 🔍 提供技術指標與績效 API 查詢接口

---

## 🧱 系統功能模組

### 1️⃣ ETF 資料蒐集爬蟲

### 📌 台股 ETF

- 從 [Yahoo 台股 ETF 清單](https://tw.stock.yahoo.com/tw-etf) 擷取所有上市 ETF 編號
- 使用 `yfinance` 擷取 2015-01-01 至今每檔 ETF 的歷史價格與配息資料：
    - 📈 **歷史價格資料**：包含每日的開盤（Open）、收盤（Close）、最高價（High）、最低價（Low）、成交量（Volume）與調整後收盤價（Adj Close，考慮股利與除權息影響）
    - 💰 **配息資料**：除息日、每單位配息金額

### 📌 美股 ETF

- 從 [TradingView 美股 ETF 清單](https://tw.tradingview.com/markets/etfs/funds-usa/) 擷取 ETF 股票代碼
- 利用 `yfinance` 抓取 2015-05-01 至今有資料的 ETF 歷史價格（與台股ETF一致）：
    - 📈 **歷史價格資料**：日期、開盤、收盤、最高、最低、成交量與調整後收盤價
    - 💰 **配息資料**：除息日、每單位配息金額

### 2️⃣ 技術指標計算模組（`pandas_ta`）

使用 `pandas_ta` 套件計算常見技術分析指標，包括：

- **RSI（相對強弱指標）**
    - 使用 14 日 RSI 衡量漲跌動能
    - 常見解讀：
        - RSI > 70：過熱（可能賣出）
        - RSI < 30：超賣（可能買入）
- **MA（移動平均線）**
    - 計算 5 日（MA5）與 20 日（MA20）均線
    - 用於觀察趨勢與轉折點（如黃金交叉、死亡交叉）
- **MACD（移動平均收斂背離指標）**
    - 預設參數：12（快線 EMA）、26（慢線 EMA）、9（訊號線）
    - 計算三項：
        - `MACD_line`：快線
        - `MACD_signal`：訊號線（慢線）
        - `MACD_hist`：柱狀圖（快線 − 慢線）
- **KD 隨機震盪指標**
    - 根據最高價、最低價與收盤價計算：
        - `%K`：快速指標
        - `%D`：%K 的移動平均
    - 解讀方式：
        - KD > 80：過熱（可能賣出）
        - KD < 20：超賣（可能買入）

### 3️⃣ 回測績效評估模組

根據 ETF 的歷史股價，計算以下四項投資績效指標：

- 📊 **總報酬率（Total Return）**
    - 公式：`(最終資產 ÷ 初始資產) − 1`
    - 衡量整段投資期間的總體漲跌幅
- 📈 **年化報酬率（CAGR）**
    - 根據起訖日期換算為年，反映資產每年穩定增長的速度
- 📉 **最大回撤（Max Drawdown）**
    - 計算歷史最高點與最低點間的最大跌幅
    - 評估資產可能面臨的最大風險
- 📐 **夏普比率（Sharpe Ratio）**
    - 衡量風險調整後的報酬率
    - 公式：`年報酬率 ÷ 年化波動率`（假設無風險利率為 0）

### 4️⃣ 策略模擬與比較

- 定期定額 vs 一次投入 vs 年投資
- 配息再投資 vs 保留現金
- 投資週期與金額可參數化查詢

---

## 🏗 技術架構圖

```
ETF 編號（台/美）
   ↓
yfinance 擷取資料
   ↓
技術指標計算（pandas_ta）
   ↓
回測績效分析
   ↓               
儲存至 MySQL
   ↓
FastAPI 讀取 MySQL → 對外提供 API / 給視覺化平台查詢   
```

---

## ⚙️ 開發技術摘要

| 模組 | 技術/工具 |
| --- | --- |
| 爬蟲 | `yfinance`, `requests`,  `selenium`(美股), `bs4`, `pandas` |
| 分析引擎 | `pandas_ta`, `pandas`, `numpy` |
| 資料庫 | `MySQL` |
| API 服務 | `FastAPI`, `Cloud Run (GCP)` |
| 排程系統 | `Airflow` |
| 非同步任務 | `Celery`, `RabbitMQ`, `Docker Compose` |
| 部署方式 | `Docker`, `Docker Hub`, `Pipenv`, `Pyenv` |

---

## 🚀 快速開始

### 1️⃣ 下載專案

```bash
git clone https://github.com/tibame05/web-crawler.git
cd web-crawler
```

### 2️⃣ 設定 Python 環境（使用 Pyenv + Pipenv）

本專案採用 Pyenv 管理 Python 版本，並使用 Pipenv 管理虛擬環境與依賴套件。

### 🐍 安裝指定 Python 版本（使用 Pyenv）

使用 `pyenv` 安裝對應的 Python 版本（例如 3.8.10）並指定為本專案使用版本：

```bash
pyenv install 3.8.10
pyenv local 3.8.10
```

### 📦 建立 Pipenv 虛擬環境

使用 `pipenv` 建立與剛剛安裝的 Python 版本綁定的虛擬環境：

```bash
pipenv --python ~/.pyenv/versions/3.8.10/bin/python
```

### 🖥️ VS Code 整合 Pipenv 虛擬環境

若 VS Code 無法自動辨識 `pipenv` 的虛擬環境，可手動設定：

1. 查詢虛擬環境位置：
    
    ```bash
    pipenv --venv
    ```
    
    輸出範例：
    
    ```swift
    /Users/joycehsu/.local/share/virtualenvs/web-crawler-v1TVI_3s
    ```
    
2. 在 VS Code 中：
    - 開啟 Command Palette（`Cmd + Shift + P`）
    - 搜尋 `Python: Select Interpreter`
    - 貼上剛剛的路徑（完整路徑至 `/bin/python`）

### 3️⃣ 安裝專案依賴 - 開發模式（已建立）

本專案已包含 `setup.py`，可直接使用 **開發模式** 安裝：

```bash
pipenv install -e .
```

這會出現🥚 `.egg-info`，代表已進入「開發模式」，任何原始碼更新都會即時生效。

**☑️ 如需手動新增特定套件（例如 `yfinance`）：**

```bash
pipenv install yfinance==0.2.63
```

### 4️⃣ 同步 Pipfile.lock（團隊協作用）

若專案已有 `Pipfile.lock` 且希望確保套件版本一致：

```bash
pipenv sync
```

這會安裝鎖定版本，適用於部署與團隊開發。

### 5️⃣ 測試與互動模式

✅ 進入 Python 互動環境：

```bash
pipenv run python
```

✅ 執行任務腳本：

```bash
pipenv run python crawler/producer_main.py
```

---

## 🐳 Docker 指令

### 打包 Image

```bash
docker build -f Dockerfile -t joycehsu65/web_crawler_tw:0.0.1 .
```

- ⚠️ 這裡的`joycehsu65`要換成自己的 Docker name

### 檢查建立的image

```bash
docker images
```

### 上傳 Image

```bash
docker push joycehsu65/web_crawler_tw:0.0.1
```

### 刪除 docker image

```bash
docker rmi joycehsu65/web_crawler_tw:0.0.1
```

---

## 🧨 部署 RabbitMQ + Celery 任務系統

### 🧱 1. 建立 Docker Network（一次即可）

```bash
docker network create etf_lib_network
```

### 2. 建立 MySQL 的 Volume（僅需一次）

```bash
docker volume create mysql
```

### ⚙️ 3. 設定 `.env` 環境變數（僅需一次）

若尚未建立 `.env` 檔案，可執行下列指令產生：

```bash
ENV=DOCKER python3 genenv.py
```

確認 `.env` 中包含：

```bash
RABBITMQ_HOST=127.0.0.1
```

### 4. 啟動 MySQL（Docker Compose）

```bash
DOCKER_IMAGE_VERSION=0.0.3.arm64 docker compose -f mysql.yml up -d
```

### 🐰 5. 啟動 RabbitMQ（Docker Compose）

```bash
docker compose -f rabbitmq-network.yml up -d
```

- 啟動 RabbitMQ container 與其 Web 管理介面
- 管理介面網址：[http://127.0.0.1:15672](http://127.0.0.1:15672/)
- 預設帳號密碼可於 `rabbitmq-network.yml` 中設定（通常為 `worker / worker`）

### 🔍 6. 檢查與除錯容器

查看目前正在運行的 container：

```bash
docker ps
```

查看 RabbitMQ container log：

```bash
docker logs web-crawler-rabbitmq-1
```

> 📝 若 container 名稱不同，可用 docker ps 確認正確名稱。
> 

### 🛠️ 7. 啟動工人（Worker）

啟動 Celery 工人來執行佇列任務：

```bash
pipenv run celery -A crawler.worker worker --loglevel=info
```

- `A crawler.worker`：指定 Celery app 的模組位置
- `-loglevel=info`：顯示詳細任務處理紀錄

### 👷‍♀️ 7.1. 啟動多個工人（多進程任務處理）

你可以同時啟動多個工人，提高任務處理效率：

```bash
pipenv run celery -A crawler.worker worker -n worker1 --loglevel=info
pipenv run celery -A crawler.worker worker -n worker2 --loglevel=info
```

- `n worker1`：指定工人名稱，便於管理

### 🚀 8. 發送任務（Producer）

執行 `producer_main.py`，將任務加入 RabbitMQ 佇列：

```bash
pipenv run python crawler/producer_main.py
```

> 任務將預設加入名為 celery 的佇列。
> 

### 🖥️ 9. Flower：監控任務狀態（Web UI）

Flower 提供 Celery 任務的監控介面，可透過瀏覽器查看：
[http://127.0.0.1:5555](http://127.0.0.1:5555/)


### 🛑 10. 關閉工人（Worker）

在 terminal 中啟動的工人，可透過 `Ctrl + C` 中斷停止。

### ❌ 11. 關閉 RabbitMQ

```bash
docker compose -f rabbitmq-network.yml down
```

## 📁 資料表總覽

| 資料表名稱 | 說明 |
|------------|------|
| `etfs` | ETF 主檔，包含 ETF 代碼、名稱、所屬地區與幣別 |
| `etf_daily_prices` | 每日價格與技術指標（每一 ETF 每日一筆） |
| `etf_backtest_results` | 每檔 ETF 的回測結果紀錄 |
| `etf_dividend` | ETF 的歷史配息資料 |

---

## 📘 `etfs` — ETF 基本資料表

| 欄位名稱 | 資料型別 | 說明 |
|----------|-----------|------|
| `etf_id` | `VARCHAR(20)` | 主鍵。ETF 代碼（如 `0050.TW`, `VOO`） |
| `etf_name` | `VARCHAR(100)` | ETF 名稱 |
| `region` | `VARCHAR(10)` | 所屬地區（如 `TW`, `US`） |
| `currency` | `VARCHAR(10)` | 幣別（如 `TWD`, `USD`） |

---

## 📗 `etf_daily_prices` — ETF 每日價格與技術指標表

| 欄位名稱 | 資料型別 | 說明 |
|----------|-----------|------|
| `etf_id` | `VARCHAR(20)` | 主鍵之一，外鍵對應 `etfs.etf_id` |
| `date` | `DATE` | 主鍵之一，價格所屬日期 |
| `adj_close` | `DECIMAL(10,4)` | 調整後收盤價 |
| `close` | `DECIMAL(10,4)` | 原始收盤價 |
| `high` | `DECIMAL(10,4)` | 當日最高價 |
| `low` | `DECIMAL(10,4)` | 當日最低價 |
| `open` | `DECIMAL(10,4)` | 開盤價 |
| `volume` | `BIGINT` | 當日成交量 |
| `rsi` | `FLOAT` | RSI 技術指標 |
| `ma5` | `FLOAT` | 5 日移動平均 |
| `ma20` | `FLOAT` | 20 日移動平均 |
| `macd_line` | `FLOAT` | MACD 主線（12 EMA - 26 EMA） |
| `macd_signal` | `FLOAT` | MACD 訊號線（MACD 之 9 EMA） |
| `macd_hist` | `FLOAT` | MACD 柱狀圖 |
| `pct_k` | `FLOAT` | KD 指標 %K |
| `pct_d` | `FLOAT` | KD 指標 %D |
| `daily_return` | `DECIMAL(8,6)` | 當日報酬率 |
| `cumulative_return` | `DECIMAL(10,6)` | 累積報酬指數（通常以 1 為基準） |

---

## 📙 `etf_backtest_results` — ETF 回測結果表

| 欄位名稱 | 資料型別 | 說明 |
|----------|-----------|------|
| `etf_id` | `VARCHAR(20)` | 主鍵，外鍵對應 `etfs.etf_id` |
| `backtest_start` | `DATE` | 回測起始日期 |
| `backtest_end` | `DATE` | 回測結束日期 |
| `total_return` | `DECIMAL(8,6)` | 總報酬率 |
| `cagr` | `DECIMAL(8,6)` | 年化報酬率 |
| `max_drawdown` | `DECIMAL(8,6)` | 最大回撤 |
| `sharpe_ratio` | `DECIMAL(8,6)` | 夏普比率（報酬 / 波動） |


---

## 🟧 `etf_dividend` — ETF 配息歷史表

| 欄位名稱 | 資料型別 | 說明 |
|----------|-----------|------|
| `etf_id` | `VARCHAR(20)` | 主鍵之一，外鍵對應 `etfs.etf_id` |
| `date` | `DATE` | 主鍵之一，配息發放日 |
| `dividend_per_unit` | `DECIMAL(10,4)` | 每單位配息金額 |
| `currency` | `VARCHAR(10)` | 配息幣別 |

---

## 🔗 資料關聯說明（ERD）

```text
etfs.etf_id
  ├── etf_daily_prices.etf_id     (1:N 每日價格)
  ├── etf_backtest_results.etf_id (1:N 回測結果)
  └── etf_dividend.etf_id         (1:N 配息紀錄)
