import os, datetime as dt, requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv

# đọc biến môi trường (khi chạy local có .env; trên Render dùng Env Vars)
load_dotenv()

PAGE_TOKEN   = os.environ["PAGE_TOKEN"]                 # EAA...
VERIFY_TOKEN = os.environ.get("VERIFY_TOKEN", "changeme")
CRON_SECRET  = os.environ.get("CRON_SECRET", "secret")  # để bảo vệ cron endpoint
TEST_PSIDS   = [p.strip() for p in os.environ.get("TEST_PSIDS","").split(",") if p.strip()]

app = Flask(__name__)

# 1) Verify webhook của Messenger
@app.get("/webhook")
def verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge"), 200
    return "Verification failed", 403

@app.post("/webhook")
def receive():
    data = request.get_json(silent=True) or {}
    app.logger.info(f"RAW: {data}")  # <— thêm dòng này
    for entry in data.get("entry", []):
        for evt in entry.get("messaging", []):
            psid = evt.get("sender", {}).get("id")
            if psid:
                app.logger.info(f"PSID: {psid}")
    return "ok", 200

# 2) Nhận sự kiện tin nhắn (lấy PSID)
@app.post("/webhook")
def receive():
    data = request.get_json()
    for entry in data.get("entry", []):
        for evt in entry.get("messaging", []):
            psid = evt["sender"]["id"]
            app.logger.info(f"PSID: {psid}")  # xem ở Render -> Logs
            # TODO: nếu cần, lưu PSID vào DB
    return "ok", 200

# 3) Hàm gửi tin nhắn tiếng Việt
def send_text(psid, text):
    url = "https://graph.facebook.com/v20.0/me/messages"
    r = requests.post(
        url,
        params={"access_token": PAGE_TOKEN},
        json={"recipient": {"id": psid}, "message": {"text": text}},
        timeout=15
    )
    r.raise_for_status()

# 4) Endpoint cho Cron Job gọi mỗi tuần
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)
    today = dt.date.today().strftime("%d/%m/%Y")
    msg = f"Nhắc trả nợ tuần này ({today}). Trả lời 'DỪNG' để hủy."
    count = 0
    for p in TEST_PSIDS:
        send_text(p, msg)
        count += 1
    return jsonify({"sent": count})

@app.get("/")
def root():
    return "OK", 200

if __name__ == "__main__":
    # Render cung cấp PORT qua biến môi trường
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
