# Monitor Deployment (Render)

這個資料夾是從 `monitor.py` 整理出的可部署版本，適合部署到 Render（支援 WebSocket）。

## 本機測試

1. 建立虛擬環境（可選）
2. 安裝套件
   - `pip install -r requirements.txt`
3. 啟動
   - `python monitor.py`
4. 打開
   - `http://127.0.0.1:8001`（若被占用會自動換 port）

## Render 部署（推薦）

1. 把 `monitor-deploy` 資料夾上傳到 GitHub（可單獨 repo）
2. 到 Render 建立 `Web Service`
3. 選你的 repo
4. 設定：
   - Runtime: `Python 3`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn monitor:app --host 0.0.0.0 --port $PORT`
5. Environment Variables（可選但建議）
   - `COINGECKO_KEY_MODE=demo`
   - `COINGECKO_API_KEY=你的 key`（沒有也可跑，但比較容易遇到 rate limit）
6. 部署完成後打開網站網址

## 健康檢查

- `GET /healthz`

## 注意事項

- 免費方案可能休眠，喚醒後需要一些時間重新抓資料。
- 程式會在服務目錄寫入 `cache_snapshot.json`（已加入 `.gitignore`）。
- WebSocket 使用同網域 `/ws`，Render 可正常支援。
