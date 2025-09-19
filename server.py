import os, datetime as dt, requests, json, logging
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv

# đọc biến môi trường (local có .env; Render dùng Env Vars)
load_dotenv()

# KHÔNG làm app crash nếu thiếu biến
PAGE_TOKEN   = os.getenv("PAGE_TOKEN")                 # EAA...
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "changeme")
CRON_SECRET  = os.getenv("CRON_SECRET", "secret")
TEST_PSIDS   = [p.strip() for p in os.getenv("TEST_PSIDS","").split(",") if p.strip()]

app = Flask(__name__)

# Bật logging rõ ràng
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ---- Health check / root ----
@app.get("/")
def root():
    return "OK", 200

# ---- GET /webhook: verify từ Meta (hub.challenge) ----
@app.get("/webhook")
def webhook_verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge"), 200
    return "Verification failed", 403

# ---- POST /webhook: nhận event Messenger ----
@app.post("/webhook")
def webhook_receive():
    # Log headers + raw body (dù content-type gì)
    raw = request.get_data(as_text=True)
    app.logger.info(f"HEADERS: {dict(request.headers)}")
    app.logger.info(f"RAW BODY: {raw}")

    data = None
    if request.is_json:
        data = request.get_json(silent=True)
    if data is None:
        try:
            data = json.loads(raw) if raw else {}
        except Exception:
            data = {}

    # Facebook Page events chuẩn: object == "page" → entry[].messaging[]
    if isinstance(data, dict) and data.get("object") == "page":
        for entry in data.get("entry", []):
            for evt in entry.get("messaging", []):
                psid = (((evt or {}).get("sender") or {}).get("id"))
                if psid:
                    app.logger.info(f"PSID: {psid}")
        # Bạn có thể xử lý thêm message/postback tại đây
    else:
        app.logger.info("Non-messaging or empty payload received")

    return "ok", 200

# ---- Gửi tin nhắn văn bản qua Send API ----
def send_text(psid: str, text: str):
    if not PAGE_TOKEN:
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

# ---- Endpoint để Cron Job gọi hằng tuần ----
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)
    if not PAGE_TOKEN:
        return jsonify({"error": "PAGE_TOKEN is missing"}), 500

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

if __name__ == "__main__":
    # Render cung cấp PORT qua biến môi trường
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
