# debug/step0_tasks_etf_list_us.py✅
# 用真實網址測試 crawler/tasks_etf_list_us.fetch_us_etf_list
# 不寫 DB（攔截 write_etfs_to_db），印出輸入→輸出，保留鍵順序，並顯示抓到幾筆。

import json, time
import crawler.tasks_etf_list_us as mod

URL = "https://tw.tradingview.com/markets/etfs/funds-usa/"
REGION = "US"
LIMIT = 0        # 0 = 不截斷（無上限，全部輸出）
SAVE_HTML = False # 是否存原始 HTML

# --- 包裝 requests.get：固定 UA、timeout、可存 HTML
_real_get = mod.requests.get
def _wrapped_get(url, *a, **k):
    k.setdefault("timeout", 15)
    resp = _real_get(url, *a, **k)
    
    # 可忽略儲存html
    if SAVE_HTML:
        try:
            with open("debug_step0_us.html", "w", encoding="utf-8") as f:
                f.write(resp.text)
            print("已存檔：./debug_step0_us.html")
        except Exception as e:
            print(f"⚠️ 存 HTML 失敗：{e}")
    time.sleep(0.3)
    return resp

mod.requests.get = _wrapped_get

# --- 攔截 DB 寫入
_captured = {"rows": None}
def _fake_write_etfs_to_db(rows):
    _captured["rows"] = rows
mod.write_etfs_to_db = _fake_write_etfs_to_db

# --- 執行
try:
    out = mod.fetch_us_etf_list(crawler_url=URL, region=REGION)
except Exception as e:
    print(f"❌ 執行失敗：{e}")
    raise

# --- 統計筆數
n_out = len(out) if isinstance(out, list) else 0
written = _captured.get("rows")
n_written = len(written) if isinstance(written, list) else 0

# --- 視圖（LIMIT=0 => 不截斷）
def _maybe_truncate(seq):
    if not isinstance(seq, list):
        return seq
    if isinstance(LIMIT, int) and LIMIT > 0:
        return seq[:LIMIT]
    return seq

view = _maybe_truncate(out)
written_view = _maybe_truncate(written)

# --- 輸出
print("\n=== 解析後回傳 (list of dict) ===")
print(f"來源：{URL}")
print(f"區域：{REGION}")
print(f"筆數：{n_out}（LIMIT={'無上限' if not LIMIT else LIMIT}）")
print(json.dumps(view, ensure_ascii=False, indent=2))

print("\n=== 準備寫入 DB 的 payload（已攔截） ===")
print(f"筆數：{n_written}（LIMIT={'無上限' if not LIMIT else LIMIT}）")
print(json.dumps(written_view, ensure_ascii=False, indent=2))

if not out:
    print("\n⚠️ 沒抓到資料，可能：選擇器改了／被風控擋／頁面動態載入。先開 debug_step0_us.html 檢查結構。")


