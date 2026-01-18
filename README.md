# 📊 ETF 分析與回測平台（台股 + 美股）

本專案是一套針對台股與美股 ETF 的 **自動化資料擷取、技術分析與回測模擬平台**。整合了 [Yahoo 台股 ETF](https://tw.stock.yahoo.com/tw-etf) 與 [tradingview 美股 ETF](https://tw.tradingview.com/markets/etfs/funds-usa/) 上市標的，提供每日更新的歷史資料與策略評估，協助使用者了解長期投資的成效與風險。

---

## 🎯 專案目標

- 📊 **ETF 標的比較**
  - 比較台股與美股 ETF 的長期投資績效與風險
  - 協助使用者選擇適合投資的 ETF

- 💰 **投資策略分析**
  - 比較一次性投入與定期定額兩種投資方式
  - 支援不同投資期間（如 1 年、3 年、10 年）

- 🔁 **資料自動化與系統建置**
  - 建立完整的 ETF 資料管線（價格、配息、TRI、回測）
  - 透過 Airflow 排程 ETL，實現每日自動更新與監控

---

## 🧱 系統功能模組

### 1️⃣ ETF 資料蒐集爬蟲

- 蒐集台股與美股 ETF 清單：
  - 台股 ETF：來自 [Yahoo 台股 ETF 清單](https://tw.stock.yahoo.com/tw-etf)
  - 美股 ETF：來自 [TradingView 美股 ETF 清單](https://tw.tradingview.com/markets/etfs/funds-usa/)
- 使用 `yfinance` 擷取 2015-01-01 至今各 ETF 的歷史資料：
  - 📈 **歷史價格資料**：
    - 日期、開盤（Open）、收盤（Close）、最高價（High）、最低價（Low）
    - 成交量（Volume）
    - 調整後收盤價（Adj Close，考慮股利與拆分影響）
  - 💰 **配息資料**：
    - 除息日
    - 每單位配息金額

### 2️⃣ 總報酬指數（TRI）計算模組

- 根據 ETF 的歷史價格與配息資料，計算 **總報酬指數（Total Return Index, TRI）**
- TRI 用來反映「**價格變動 + 股利再投資**」後的實際投資報酬
- 假設股利於 **除息日全數再投資**
- 依市場資料特性採用不同計算方式：
  - **美股 ETF**：使用 `Adj Close`（已內含股利與拆分調整）
  - **台股 ETF**：使用 `Close` 價格，並於除息日將股利納入計算
- TRI 作為後續回測與績效評估的**核心時間序列基礎**

### 3️⃣ 回測績效評估模組

根據 ETF 的 TRI 與歷史價格資料，計算以下投資績效指標：

- 📈 **年化報酬率（CAGR）**
  - 根據起訖日期換算為年
  - 反映資產每年穩定成長的速度

- 📐 **夏普比率（Sharpe Ratio）**
  - 衡量風險調整後的報酬表現
  - 公式：`年化報酬率 ÷ 年化波動度`（假設無風險利率為 0）

- 📉 **最大回撤（Max Drawdown）**
  - 計算歷史最高點至最低點間的最大跌幅
  - 評估投資期間可能承受的最大風險

- 📊 **總報酬率（Total Return）**
  - 公式：`(最終資產 ÷ 初始資產) − 1`
  - 衡量整段投資期間的總體漲跌幅

- 📐 **年化波動度（Annualized Volatility）**
  - 以報酬率的標準差年化後計算
  - 衡量資產價格波動程度，作為風險指標

---

## 🏗 技術架構（簡化流程）

```
ETF 清單（台股 / 美股）
   ↓
yfinance 擷取歷史價格與配息資料
   ↓
資料寫入 MySQL（價格、配息原始資料）
   ↓
TRI（總報酬指數）計算
   ↓
回測績效分析（CAGR、Max Drawdown、Volatility、Sharpe Ratio、Total Return）
   ↓
回測結果寫入 MySQL
   ↓
Streamlit 讀取 MySQL，提供互動式視覺化與查詢
```

---

## ⚙️ 開發技術摘要

| 模組 | 技術/工具 |
| --- | --- |
| 爬蟲 | `yfinance`, `requests`, `bs4`, `pandas` |
| 分析引擎 | `pandas_ta`, `pandas`, `numpy` |
| 資料庫 | `MySQL` |
| API 服務 | `Cloud Run (GCP)` |
| 排程系統 | `Airflow` |
| 非同步任務 | `Celery`, `RabbitMQ`, `Docker Compose` |
| 部署方式 | `Docker`, `Docker Hub`, `Pipenv`, `Pyenv` |

---

## 🚀 快速開始

### 1️⃣ 下載專案

```bash
git clone https://github.com/tibame05/web_crawler_etf.git
cd web_crawler_etf
```

### 2️⃣ 設定 Python 環境（使用 Pyenv + Pipenv）

本專案採用 Pyenv 管理 Python 版本，並使用 Pipenv 管理虛擬環境與依賴套件。

#### 🐍 安裝指定 Python 版本（使用 Pyenv）

使用 `pyenv` 安裝對應的 Python 版本（例如 3.8.10）並指定為本專案使用版本：

```bash
pyenv install 3.8.10
pyenv local 3.8.10
```

#### 📦 建立 Pipenv 虛擬環境

使用 `pipenv` 建立與剛剛安裝的 Python 版本綁定的虛擬環境：

```bash
pipenv --python ~/.pyenv/versions/3.8.10/bin/python
```

#### 🖥️ VS Code 整合 Pipenv 虛擬環境

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
pipenv install yfinance==0.2.65
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
pipenv run python crawler/producer_main_tw.py
pipenv run python crawler/producer_main_us.py
```

---

## 🐳 Docker 指令

### 打包 Image

```bash
# TW image
docker build -f Dockerfile -t winston07291/web_crawler_tw:0.0.1 .

# US image
docker build -f Dockerfile -t winston07291/web_crawler_us:0.0.1 .
```

- ⚠️ 這裡的`winston07291`要換成自己的 Docker name
- docker build -f Dockerfile -t {docker hub 名稱}/{image 名稱}:{版本號} .

### 檢查建立的image

```bash
docker images
```

### 上傳 Image 到 docker hub

```bash
# TW image
docker push winston07291/web_crawler_tw:0.0.1

# US image
docker push winston07291/web_crawler_us:0.0.1
```

- docker push {docker hub 名稱}/{image 名稱}:{版本號}

### 更改 yaml 檔的 image 名稱

開啟`worker-network.yml`與`producer-network.yml`檔案，將 image 改成輸出的 docker 名稱與版本號。

範例：
我輸出的是`joycehsu65/web_crawler_tw:0.1.2`，
從台股 services 中的`image: winston07291/web_crawler_tw:${DOCKER_IMAGE_VERSION}`，
改成`image: joycehsu65/web_crawler_tw:0.1.2`。
美股 services 也需同步更改。

### 刪除 docker image (要刪除時再執行即可)

```bash
# TW image
docker rmi winston07291/web_crawler_tw:0.0.1

# US image
docker rmi winston07291/web_crawler_us:0.0.1
```

- docker rmi {docker hub 名稱}/{image 名稱}:{版本號}

---

## 🧨 部署 MySQL + RabbitMQ + Celery 任務系統

### 建立與設定（僅需一次）

1. 建立 Docker Network（僅需一次）

    ```bash
    docker network create etf_lib_network
    ```

    - docker network create {network 名稱}

2. 建立 MySQL 的 Volume（僅需一次）

    ```bash
    docker volume create mysql
    ```

    - docker volume create {volume 名稱}

3. 設定 `.env` 環境變數（僅需一次）

    若尚未建立 `.env` 檔案，可執行下列指令產生：

    ```bash
    ENV=DOCKER python3 genenv.py

### 啟動 MySQL、RabbitMQ

1.  啟動 MySQL（Docker Compose）

    ```bash
    DOCKER_IMAGE_VERSION=0.0.3.arm64 docker compose -f mysql.yml up -d
    ```

    -  建立資料庫與與資料表（僅需一次）

    ```bash
    pipenv run python database/setup.py
    ```

2. 啟動 RabbitMQ（Docker Compose）
    
    ```bash
    docker compose -f rabbitmq-network.yml up -d
    ```

    - RabbitMQ 管理介面: [http://127.0.0.1:15672](http://127.0.0.1:15672)
    - 預設帳號密碼: `worker / worker` (可於 `rabbitmq-network.yml` 中設定)

3. 啟動 worker (Docker Compose)

    ```bash
    docker compose -f worker-network.yml up -d
    ```

    - 執行前請先確認是否有將 `worker-network.yml`檔中的 image 改成您推 docker 上的名稱與版本號
    - Flower 提供 Celery 任務的監控介面: [http://127.0.0.1:5555](http://127.0.0.1:5555/)

4. 啟動 producer

    ```bash
    docker compose -f producer-network.yml up -d
    ```
    - 執行前請先確認是否有將 `producer-network.yml`檔中的 image 改成您推 docker 上的名稱與版本號
    - producer-network.yml 中可以設定多個 producer services

### 停止並移除所有容器與網路

1. 關閉工人（Worker）

    在 terminal 中按 Ctrl + C 中斷

2. 停止並移除 producer

    ```bash
    docker compose -f producer-network.yml down
    ```

3. 停止並移除 worker

    ```bash
    docker compose -f worker-network.yml down
    ```

4. 停止並移除 RabbitMQ

    ```bash
    docker compose -f rabbitmq-network.yml down
    ```

5. 停止並移除 MySQL

    ```bash
    docker compose -f mysql.yml down
    ```

### 檢查與除錯容器

- 查看目前正在運行的 container：

    ```bash
    docker ps
    ```

- 查看容器日誌 log：

    ```bash
    # RabbitMQ
    docker logs web-crawler-rabbitmq-1
    
    # MySQL
    docker logs <mysql_container_name>
    ```

    > 📝 容器名稱可透過 `docker ps` 確認


### 啟動工人（Worker）

確認所有服務啟動後,執行以下指令啟動 Worker:

```bash
# TW Worker
pipenv run celery -A crawler.worker worker --loglevel=info --hostname=%h -Q crawler_tw

# US Worker
pipenv run celery -A crawler.worker worker --loglevel=info --hostname=%h -Q crawler_us
```

- `A crawler.worker`：指定 Celery app 的模組位置
- `-loglevel=info`：顯示詳細任務處理紀錄
- `-Q`：指定不同的 queue


### 發送任務（Producer）

直接執行 Python 腳本

```bash
# 台股任務
pipenv run python crawler/producer_main_tw.py

# 美股任務
pipenv run python crawler/producer_main_us.py
```

> 需在任務檔案中分別不同的 queue。

---

## 📁 資料表總覽（更新版本）

| 資料表名稱 | 說明 | 更新策略 |
|---|---|---|
| **etfs** | ETF 基本資料（慢變資料），包含費用率、成立日與狀態。 | 覆蓋、重寫 |
| **etf_daily_prices** | 每日價格與成交量資料（動態追加）。 | 新增，不更動舊資料 |
| **etf_dividends** | 歷史配息事件與金額（動態追加）。 | 新增，不更動舊資料 |
| **etf_tris** | 總報酬指數（TRI），用於計算含息資產曲線。 | 新增，不更動舊資料 |
| **etf_backtests** | 回測成績單，紀錄 1Y、3Y、10Y 的績效指標。 | 覆蓋、重寫 |
| **etl_sync_status** | ETL 同步狀態監控，紀錄各項資料的最後更新時間與筆數。 | 覆蓋、重寫 |

---

### 📘 `etfs` — ETF 基本資料表（慢變資料）

> 註：建議每月定期檢查 `expense_ratio`（管理費）是否更改。

| 欄位名稱 | 資料型別 | 說明 |
|---|---|---|
| `etf_id` | `VARCHAR(20)` | PK。ETF 代碼（如 `0050.TW`, `VOO`），作為各表關聯鍵。 |
| `etf_name` | `VARCHAR(100)` | ETF 名稱，顯示於排行榜與搜尋清單。 |
| `region` | `VARCHAR(10)` | 市場區域（如 `TW`, `US`），用於散點圖顏色標示。 |
| `currency` | `VARCHAR(10)` | 交易幣別，用於回測計算與說明卡。 |
| `expense_ratio` | `DECIMAL(6,4)` | 管理費（單位 %），顯示於排行榜費用率欄位。 |
| `inception_date` | `DATE` | 成立日期，用於篩選「成立 ≥ N 年」的標的。 |
| `status` | `ENUM` | 狀態：`ACTIVE`（交易中）或 `DELISTED`（已下市）。 |

---

### 📗 `etf_daily_prices` — ETF 每日價格表（動態追加）

| 欄位名稱 | 資料型別 | 說明 |
|---|---|---|
| `etf_id` | `VARCHAR(20)` | PK / FK。對應 `etfs.etf_id`。 |
| `trade_date` | `DATE` | PK。交易日期，作為更新資料時的比對依據。 |
| `open` | `DECIMAL(18,6)` | 該日開盤價。 |
| `high` | `DECIMAL(18,6)` | 該日最高價，建立技術指標或 TRI 的原始資料。 |
| `low` | `DECIMAL(18,6)` | 該日最低價。 |
| `close` | `DECIMAL(18,6)` | 該日收盤價，回測績效的基礎價。 |
| `adj_close` | `DECIMAL(18,6)` | 調整後收盤價（含息 / 拆分），若數據已含息則直接計算 TRI。 |
| `volume` | `BIGINT` | 當日成交量，用於篩選流動性較高的 ETF。 |

---

### 🟧 `etf_dividends` — ETF 配息歷史表（動態追加）

| 欄位名稱 | 資料型別 | 說明 |
|---|---|---|
| `etf_id` | `VARCHAR(20)` | PK / FK。對應 `etfs.etf_id`。 |
| `ex_date` | `DATE` | PK。除息日，計算含息報酬的生效日期。 |
| `dividend_per_unit` | `DECIMAL(10,4)` | 每單位配發的現金股利金額，用於建立 TRI。 |
| `currency` | `VARCHAR(10)` | 股利幣別，須與價格幣別對齊以便計算。 |

---

### 📈 `etf_tris` — 總報酬指數（Total Return Index）

| 欄位名稱 | 資料型別 | 說明 |
|---|---|---|
| `etf_id` | `VARCHAR(20)` | PK / FK。對應 `etfs.etf_id`。 |
| `tri_date` | `DATE` | PK。回測與績效統計的時間軸。 |
| `tri` | `DECIMAL(20,8)` | 含息累積指數，用於計算 CAGR、最大回撤與資產曲線。 |
| `currency` | `VARCHAR(10)` | 與輸入投資金額換算一致的幣別。 |

---

### 🏆 `etf_backtests` — 回測績效表（快取數據）

| 欄位名稱 | 資料型別 | 說明 |
|---|---|---|
| `etf_id` | `VARCHAR(20)` | PK / FK。對應 `etfs.etf_id`。 |
| `label` | `ENUM` | 回測年數標籤：`1y`、`3y`、`10y`。 |
| `start_date` | `DATE` | PK。回測起始日期，用於散點圖選擇區間。 |
| `end_date` | `DATE` | 回測結束日期。 |
| `cagr` | `DECIMAL(8,6)` | 年化報酬率，排行榜與散點圖 Y 軸關鍵指標。 |
| `sharpe_ratio` | `DECIMAL(8,6)` | 夏普比率，評估風險調整後的績效。 |
| `max_drawdown` | `DECIMAL(8,6)` | 最大回撤，排行榜與散點圖 X 軸風險指標。 |
| `total_return` | `DECIMAL(8,6)` | 總報酬率，投資總計績效。 |
| `volatility` | `DECIMAL(8,6)` | 年化波動度，散點圖 X 軸風險指標。 |

---

### 🔄 `etl_sync_status` — ETL 同步狀態監控表

| 欄位名稱 | 資料型別 | 說明 |
|---|---|---|
| `etf_id` | `VARCHAR(20)` | PK / FK。對應 `etfs.etf_id`。 |
| `last_price_date` | `DATE` | 價格最後日期，用於增量抓取 `etf_daily_prices`。 |
| `price_count` | `INT` | 價格資料總筆數。 |
| `last_dividend_ex_date` | `DATE` | 除息最後日期，用於增量抓取 `etf_dividends`。 |
| `dividend_count` | `INT` | 配息資料總筆數。 |
| `last_tri_date` | `DATE` | TRI 最後日期，用於增量計算 TRI 資料。 |
| `tri_count` | `INT` | TRI 資料總筆數。 |
| `updated_at` | `DATETIME` | 該筆紀錄最後更新時間（日更監控）。 |

---

## 🔗 資料關聯說明（ERD）

```text
etfs.etf_id
  ├── `etf_daily_prices`.`etf_id`   (1:N 每日價格)
  ├── `etf_dividends`.`etf_id`      (1:N 配息紀錄)
  ├── `etf_tris`.`etf_id`           (1:N TRI 時間序列)
  ├── `etf_backtests`.`etf_id`      (1:N 回測結果)
  └── `etl_sync_status`.`etf_id`    (1:1 同步狀態 / 監控快照)
