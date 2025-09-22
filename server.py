# server.py
import os
import json
import time
import logging
import datetime as dt
from typing import Dict, Any, List

import requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv

# OCR (module riêng)
from ocr_fast import fast_extract_amount_date

# ====== ENV ======
load_dotenv()

PAGE_TOKEN   = os.getenv("PAGE_TOKEN")                 # BẮT BUỘC: page access token
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "changeme")   # dùng khi Verify webhook
CRON_SECRET  = os.getenv("CRON_SECRET", "secret")      # bảo vệ /task/*

# ====== APP & LOG ======
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ====== GỬI TIN NHẮN ======
def send_text(psid: str, text: str):
    if not PAGE_TOKEN:
        app.logger.error("PAGE_TOKEN missing; cannot send messages.")
        return
    url = "https://graph.facebook.com/v20.0/me/messages"
    r = requests.post(
        url,
        params={"access_token": PAGE_TOKEN},
        json={"recipient": {"id": psid}, "message": {"text": text}},
        timeout=20,
    )
    if r.status_code >= 400:
        app.logger.error("Send API error %s: %s", r.status_code, r.text)
    r.raise_for_status()

# ====== DEDUP (tránh trả lời 1 ảnh nhiều lần) ======
_recent_mids: Dict[str, float] = {}   # mid -> timestamp

def seen_mid(mid: str, ttl_sec: int = 600) -> bool:
    now = time.time()
    # dọn rác
    for k, v in list(_recent_mids.items()):
        if now - v > ttl_sec:
            _recent_mids.pop(k, None)
    if not mid:
        return False
    if mid in _recent_mids:
        return True
    _recent_mids[mid] = now
    return False

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

# 2) Webhook nhận message
@app.post("/webhook")
def webhook_receive():
    raw = request.get_data(as_text=True)
    app.logger.info("RAW BODY: %s", raw)

    data: Dict[str, Any] = {}
    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        try:
            data = json.loads(raw) if raw else {}
        except Exception:
            data = {}

    if data.get("object") != "page":
        return "ok", 200

    for entry in data.get("entry", []):
        for evt in entry.get("messaging", []):
            psid = ((evt.get("sender") or {}).get("id"))
            if not psid:
                continue

            # Chặn lặp theo message id
            mid = ((evt.get("message") or {}).get("mid")) or ((evt.get("postback") or {}).get("mid"))
            if mid and seen_mid(mid):
                app.logger.info("Skip duplicate mid=%s", mid)
                continue

            # Postback GET_STARTED
            if evt.get("postback", {}).get("payload") == "GET_STARTED":
                send_text(psid,
                          "Chào bạn! Gửi ảnh biên lai MoMo vào đây, mình sẽ đọc: "
                          "Số tiền / Thời gian / Người thực hiện / Chi tiết.")
                continue

            # Nếu có ảnh → OCR
            msg = evt.get("message") or {}
            atts: List[Dict] = msg.get("attachments") or []
            for att in atts:
                if att.get("type") == "image":
                    image_url = (att.get("payload") or {}).get("url")
                    if not image_url:
                        continue

                    app.logger.info("OCR image_url: %s", image_url)
                    try:
                        result = fast_extract_amount_date(image_url)

                        # === DEBUG: in ra toàn bộ dòng OCR lên log ===
                        lines = result.get("lines", []) or []
                        app.logger.info("=== OCR LINES (%d) ===\n%s\n=== END OCR LINES ===",
                                        len(lines), "\n".join(lines))
                        app.logger.info("OCR SUMMARY: %s", {
                            "amount_text": result.get("amount_text"),
                            "date_text":   result.get("date_text"),
                            "actor_name":  result.get("actor_name"),
                            "detail_text": result.get("detail_text"),
                            "spent_sec":   result.get("spent_sec"),
                        })

                        # Tóm tắt trả về messenger LUÔN kèm preview lines (rút gọn)
                        amt_show = result.get("amount_text") or "-"
                        when     = result.get("date_text")   or "-"
                        actor    = result.get("actor_name")  or "-"
                        detail   = result.get("detail_text") or "-"
                        spent    = result.get("spent_sec", 0)

                        # Rút gọn tối đa 20 dòng / 1200 ký tự để tránh quá dài
                        preview_lines = lines[:20]
                        preview_text  = "\n".join(preview_lines)
                        if len(preview_text) > 1200:
                            preview_text = preview_text[:1200] + "…"

                        reply = (
                            "✅ KẾT QUẢ (MoMo)\n"
                            f"• Số tiền: {amt_show}\n"
                            f"• Thời gian: {when}\n"
                            f"• Người thực hiện: {actor}\n"
                            f"• Chi tiết: {detail}\n"
                            f"(OCR ~{spent}s)\n\n"
                            f"[DEBUG] OCR lines ({len(lines)}):\n{preview_text}"
                        )
                        send_text(psid, reply)

                    except Exception as e:
                        app.logger.exception("OCR failed: %s", e)
                        send_text(psid, "❌ Xin lỗi, không đọc được ảnh này. Bạn thử chụp rõ hơn/đủ sáng nhé.")
            # Text “DỪNG” để minh hoạ
            text = (msg.get("text") or "").strip().lower()
            if text in {"dung", "dừng", "stop"}:
                send_text(psid, "Đã ghi nhận yêu cầu dừng nhắc.")

    return "ok", 200

# 3) Cron mẫu (gửi broadcast thủ công)
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)
    return jsonify({"ok": True, "now": dt.datetime.utcnow().isoformat()})

# ====== MAIN ======
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
