# 使用 Ubuntu 20.04 作為基礎映像檔
FROM ubuntu:20.04

# 避免 tzdata 互動式安裝卡住 + 預設環境語系
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# 安裝必要套件：
# - python3.8, python3.8-dev：Python 本體與 C 擴充編譯標頭
# - pip：Python 套件管理工具
# - build-essential：含 gcc、g++、make 等編譯工具
# - libxml2-dev, libxslt1-dev：lxml 依賴的系統函式庫
# - wget：下載 .env 時或其他自動化腳本常用
RUN apt-get update && apt-get install -y \
    python3.8 \
    python3.8-dev \
    python3-pip \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    wget \
    && pip3 install pipenv==2022.4.8

# 建立工作目錄 /crawler
RUN mkdir /crawler

# 將當前目錄（與 Dockerfile 同層）所有內容複製到容器的 /crawler 資料夾
COPY . /crawler/

# 設定容器的工作目錄為 /crawler，後續的指令都在這個目錄下執行
WORKDIR /crawler/

# 根據 Pipfile.lock 安裝所有依賴（確保環境一致性）
RUN pipenv sync

# 設定語系環境變數，避免 Python 編碼問題
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# 建立 .env
RUN ENV=PRODUCTION python3 genenv.py

# 啟動容器後，預設執行 bash（開啟終端）
CMD ["/bin/bash"]
