import os, datetime as dt, requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv

load_dotenv()

# Dùng getenv để không crash nếu thiếu biến môi trường
PAGE_TOKEN   = os.getenv("PAGE_TOKEN")                 # EAA...
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "changeme")
CRON_SECRET  = os.getenv("CRON_SECRET", "secret")
TEST_PSIDS   = [p.strip() for p in os.getenv("TEST_PSIDS","").split(",") if p.strip()]

app = Flask(__name__)

# 1) Verify webhook của Messenger (GET)
@app.get("/webhook")
def verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge"), 200
    return "Verification failed", 403

# 2) Nhận sự kiện tin nhắn (POST)
@app.post("/webhook")
def receive():
    data = request.get_json(silent=True) or {}
    app.logger.info(f"RAW: {data}")
    for entry in data.get("entry", []):
        for evt in entry.get("messaging", []):
            psid = evt.get("sender", {}).get("id")
            if psid:
                app.logger.info(f"PSID: {psid}")
    return "ok", 200

# 3) Gửi tin nhắn
def send_text(psid, text):
    if not PAGE_TOKEN:
        # Không ném exception để app vẫn sống; log cho bạn biết cần set token
        app.logger.error("PAGE_TOKEN is missing; cannot send messages")
        return
    url = "https://graph.facebook.com/v20.0/me/messages"
    r = requests.post(
        url,
        params={"access_token": PAGE_TOKEN},
        json={"recipient": {"id": psid}, "message": {"text": text}},
        timeout=15
    )
    r.raise_for_status()

# 4) Cron endpoint (Render gọi hằng tuần)
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)
    today = dt.date.today().strftime("%d/%m/%Y")
    msg = f"Nhắc trả nợ tuần này ({today}). Trả lời 'DỪNG' để hủy."
    sent = 0
    for p in TEST_PSIDS:
        try:
            send_text(p, msg)
            sent += 1
        except Exception as e:
            app.logger.exception(f"Send failed for {p}: {e}")
    return jsonify({"sent": sent, "list": TEST_PSIDS})

@app.get("/")
def root():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
