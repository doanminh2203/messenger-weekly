# server.py
import os
import json
import time
import logging
import datetime as dt
from io import StringIO
import csv
import requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# ====== ENV ======
load_dotenv()  # dùng .env khi chạy local; Render dùng Env Vars

PAGE_TOKEN   = os.getenv("PAGE_TOKEN")                 # EAA...
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "changeme")
CRON_SECRET  = os.getenv("CRON_SECRET", "secret")
TEST_PSIDS   = [p.strip() for p in os.getenv("TEST_PSIDS","").split(",") if p.strip()]
PSIDS_CSV_URL = os.getenv("PSIDS_CSV_URL", "")        # raw URL tới psids.csv (tùy chọn)

# ====== APP & LOG ======
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ====== HELPERS ======
def send_text(psid: str, text: str):
    """Gửi tin nhắn văn bản tới 1 PSID bằng Send API."""
    if not PAGE_TOKEN:
        app.logger.error("PAGE_TOKEN missing; cannot send messages")
        return
    url = "https://graph.facebook.com/v20.0/me/messages"
    r = requests.post(
        url,
        params={"access_token": PAGE_TOKEN},
        json={"recipient": {"id": psid}, "message": {"text": text}},
        timeout=20,
    )
    # Log lỗi body nếu có
    if r.status_code >= 400:
        app.logger.error("Send API error %s: %s", r.status_code, r.text)
    r.raise_for_status()

def load_psids_from_csv():
    """Đọc tất cả PSID từ CSV (bỏ dòng chưa có psid). Nếu không khai báo URL → trả []"""
    targets = []
    if not PSIDS_CSV_URL:
        return targets
    try:
        resp = requests.get(PSIDS_CSV_URL, timeout=10)
        resp.raise_for_status()
        f = StringIO(resp.text)
        reader = csv.DictReader(f)
        for row in reader:
            psid = (row.get("psid") or "").strip()
            if psid:
                targets.append(psid)
    except Exception as e:
        app.logger.exception(f"Failed to load CSV: {e}")
    return targets

def extract_ref(evt: dict):
    """Lấy ref nếu user vào từ m.me?ref=... (không bắt buộc khi bạn dùng CSV/TEST_PSIDS)"""
    if (evt.get("referral") or {}).get("ref"):
        return evt["referral"]["ref"]
    if ((evt.get("message") or {}).get("referral") or {}).get("ref"):
        return evt["message"]["referral"]["ref"]
    if ((evt.get("postback") or {}).get("referral") or {}).get("ref"):
        return evt["postback"]["referral"]["ref"]
    return None

# ====== ROUTES ======
@app.get("/")
def root():
    return "OK", 200

# 1) Verify webhook
@app.get("/webhook")
def webhook_verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge"), 200
    return "Verification failed", 403

# 2) Receive events (log PSID, xử lý GET_STARTED, message, referral)
@app.post("/webhook")
def webhook_receive():
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

    if not isinstance(data, dict) or data.get("object") != "page":
        app.logger.info("Non-messaging or empty payload received")
        return "ok", 200

    for entry in data.get("entry", []):
        for evt in entry.get("messaging", []):
            psid = (((evt or {}).get("sender") or {}).get("id"))
            if psid:
                app.logger.info(f"PSID: {psid}")

            # Bắt postback GET_STARTED
            postback = (evt.get("postback") or {})
            payload = (postback.get("payload") or "")
            if psid and payload == "GET_STARTED":
                app.logger.info(f"GET_STARTED from {psid}")
                try:
                    send_text(
                        psid,
                        "Chào bạn! Bạn đã bắt đầu trò chuyện với 108Lab.\n"
                        "Bạn sẽ nhận nhắc hằng tuần khi được bật. Nhắn 'DỪNG' để hủy bất cứ lúc nào."
                    )
                except Exception as e:
                    app.logger.exception(f"Reply failed: {e}")

            # (Tuỳ chọn) nếu bạn muốn biết người vào từ ref nhóm nào
            ref = extract_ref(evt)
            if psid and ref:
                app.logger.info(f"REF '{ref}' from {psid}")

    return "ok", 200

# 3) Cron endpoint – gửi cho 1 nhóm duy nhất (CSV hoặc TEST_PSIDS); hỗ trợ psids= & msg=
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)
    if not PAGE_TOKEN:
        return jsonify({"error": "PAGE_TOKEN is missing"}), 500

    # Ưu tiên psids= (ad-hoc), sau đó CSV; cuối cùng fallback TEST_PSIDS
    psids_param = request.args.get("psids")  # "111,222"
    custom = request.args.get("msg")         # nội dung tùy chọn

    if psids_param:
        targets = [p.strip() for p in psids_param.split(",") if p.strip()]
        mode = "psids"
    else:
        csv_psids = load_psids_from_csv()
        if csv_psids:
            targets = csv_psids
            mode = "CSV"
        else:
            targets = TEST_PSIDS
            mode = "TEST_PSIDS"

    today = dt.date.today().strftime("%d/%m/%Y")
    msg = custom or f"Nhắc trả nợ tuần này ({today}). Trả 'DỪNG' để hủy."

    sent = 0
    for p in targets:
        try:
            send_text(p, msg)
            sent += 1
            time.sleep(0.2)  # giãn nhẹ, tránh rate limit nếu danh sách dài
        except Exception as e:
            app.logger.exception(f"Send failed for {p}: {e}")

    # Timestamp UTC + VN cho dễ đối chiếu log
    now_utc = dt.datetime.now(dt.timezone.utc)
    vn_tz = ZoneInfo("Asia/Ho_Chi_Minh")
    now_vn = now_utc.astimezone(vn_tz)

    return jsonify({
        "mode": mode,
        "targets": targets,
        "sent": sent,
        "server_time_utc": now_utc.isoformat(timespec="seconds"),
        "server_time_vietnam": now_vn.isoformat(timespec="seconds")
    })

# ====== MAIN ======
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
