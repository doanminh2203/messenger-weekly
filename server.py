# server.py (đã thêm debug Note + xác nhận Người gửi / Số tiền / Ngày giờ)
import os
import json
import time
import base64
import logging
import datetime as dt
from io import StringIO
import csv

import requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from ocr_model import verify_image_against_expected  # RapidOCR (onnx) nhẹ

# ================== ENV ==================
load_dotenv()  # local .env; Render dùng Env Vars

# Facebook
PAGE_TOKEN    = os.getenv("PAGE_TOKEN")                 # EAA... (Page Access Token)
VERIFY_TOKEN  = os.getenv("VERIFY_TOKEN", "changeme")

# Cron bảo vệ
CRON_SECRET   = os.getenv("CRON_SECRET", "secret")

# Danh sách test thủ công (fallback)
TEST_PSIDS    = [p.strip() for p in os.getenv("TEST_PSIDS", "").split(",") if p.strip()]

# Đọc CSV công khai (raw URL)
PSIDS_CSV_URL = os.getenv("PSIDS_CSV_URL", "")         # vd: https://raw.githubusercontent.com/<owner>/<repo>/main/psids.csv

# Ghi CSV qua GitHub API (commit trực tiếp)
GH_OWNER      = os.getenv("GH_OWNER", "")
GH_REPO       = os.getenv("GH_REPO", "")
GH_BRANCH     = os.getenv("GH_BRANCH", "main")
GH_FILE_PATH  = os.getenv("GH_FILE_PATH", "psids.csv")
GH_TOKEN      = os.getenv("GH_TOKEN", "")              # token RW Contents cho repo trên

# ================== APP & LOG ==================
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ================== HELPERS: FACEBOOK SEND API ==================
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
        timeout=25,
    )
    if r.status_code >= 400:
        app.logger.error("Send API error %s: %s", r.status_code, r.text)
    r.raise_for_status()

# ================== HELPERS: CSV - ĐỌC NGƯỜI NHẬN ==================
def load_psids_from_csv():
    """Đọc tất cả PSID từ CSV public (raw URL). Trả [] nếu không cấu hình hoặc lỗi."""
    targets = []
    if not PSIDS_CSV_URL:
        return targets
    try:
        resp = requests.get(PSIDS_CSV_URL, timeout=15)
        resp.raise_for_status()
        f = StringIO(resp.text)
        reader = csv.DictReader(f)
        for row in reader:
            psid = (row.get("psid") or "").strip()
            if psid:
                targets.append(psid)
    except Exception as e:
        app.logger.exception(f"Failed to load CSV (read): {e}")
    return targets

# ================== HELPERS: GITHUB CONTENTS API - GHI CSV ==================
GITHUB_API = "https://api.github.com"

def _gh_headers():
    if not GH_TOKEN:
        raise RuntimeError("GH_TOKEN missing")
    return {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json",
    }

def gh_get_file(owner, repo, path, branch):
    """GET /repos/{owner}/{repo}/contents/{path}?ref={branch}"""
    url = f"{GITHUB_API}/repos/{owner}/{repo}/contents/{path}"
    r = requests.get(url, headers=_gh_headers(), params={"ref": branch}, timeout=20)
    if r.status_code == 404:
        return None  # file chưa tồn tại
    r.raise_for_status()
    return r.json()  # có 'content'(base64), 'sha'

def gh_put_file(owner, repo, path, branch, content_bytes, sha=None, message="update psids.csv"):
    """PUT /repos/{owner}/{repo}/contents/{path} để tạo/cập nhật file"""
    url = f"{GITHUB_API}/repos/{owner}/{repo}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(url, headers=_gh_headers(), json=payload, timeout=30)
    if r.status_code >= 400:
        app.logger.error("GitHub PUT error %s: %s", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def upsert_psid_to_csv(psid: str) -> bool:
    """
    Đọc GH_FILE_PATH; nếu psid chưa có thì append 1 dòng và commit qua GitHub API.
    Trả True nếu có thêm mới, False nếu đã tồn tại hoặc không đủ ENV.
    """
    if not (GH_OWNER and GH_REPO and GH_FILE_PATH and GH_BRANCH and GH_TOKEN):
        app.logger.warning("GitHub ENV missing; skip CSV upsert")
        return False

    try:
        meta = gh_get_file(GH_OWNER, GH_REPO, GH_FILE_PATH, GH_BRANCH)
        if meta and "content" in meta:
            raw = base64.b64decode(meta["content"])
            text = raw.decode("utf-8", errors="ignore")
            lines = [ln.rstrip("\n") for ln in text.splitlines()]
            # đảm bảo có header
            if not lines or not lines[0].lower().startswith("psid"):
                lines.insert(0, "psid,created_at_iso")
            # kiểm tra tồn tại
            existing = set()
            for ln in lines[1:]:
                if not ln:
                    continue
                first = ln.split(",", 1)[0].strip()
                if first:
                    existing.add(first)
            if psid in existing:
                app.logger.info(f"PSID already in CSV: {psid}")
                return False
            # append
            now_iso = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
            lines.append(f"{psid},{now_iso}")
            new_text = "\n".join(lines) + "\n"
            gh_put_file(
                GH_OWNER, GH_REPO, GH_FILE_PATH, GH_BRANCH,
                new_text.encode("utf-8"), sha=meta.get("sha"),
                message=f"chore: add psid {psid}"
            )
            app.logger.info(f"Appended PSID to CSV: {psid}")
            return True
        else:
            # File chưa tồn tại → tạo mới với header + dòng đầu
            now_iso = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
            new_text = "psid,created_at_iso\n" + f"{psid},{now_iso}\n"
            gh_put_file(
                GH_OWNER, GH_REPO, GH_FILE_PATH, GH_BRANCH,
                new_text.encode("utf-8"), sha=None,
                message=f"chore: create csv with psid {psid}"
            )
            app.logger.info(f"Created CSV and added PSID: {psid}")
            return True
    except Exception as e:
        app.logger.exception(f"Failed to upsert CSV: {e}")
        return False

# ================== HELPERS: WEBHOOK PARSE ==================
def extract_ref(evt: dict):
    """Lấy ref nếu user vào từ m.me?ref=... (tham khảo)."""
    if (evt.get("referral") or {}).get("ref"):
        return evt["referral"]["ref"]
    if ((evt.get("message") or {}).get("referral") or {}).get("ref"):
        return evt["message"]["referral"]["ref"]
    if ((evt.get("postback") or {}).get("referral") or {}).get("ref"):
        return evt["postback"]["referral"]["ref"]
    return None

# ================== ROUTES ==================
@app.get("/")
def root():
    return "OK", 200

# 1) Verify webhook
@app.get("/webhook")
def webhook_verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge"), 200
    return "Verification failed", 403

# 2) Nhận sự kiện từ Messenger
@app.post("/webhook")
def webhook_receive():
    raw = request.get_data(as_text=True) or ""
    if len(raw) > 4000:
        app.logger.info(f"RAW BODY (truncated 4k): {raw[:4000]}...")
    else:
        app.logger.info(f"RAW BODY: {raw}")

    data = request.get_json(silent=True)
    if not isinstance(data, dict) or data.get("object") != "page":
        app.logger.info("Non-messaging or empty payload received")
        return "ok", 200

    for entry in data.get("entry", []):
        for evt in entry.get("messaging", []):
            psid = (((evt or {}).get("sender") or {}).get("id"))
            if psid:
                app.logger.info(f"PSID: {psid}")

            # Postback GET_STARTED
            postback = (evt.get("postback") or {})
            payload = (postback.get("payload") or "")
            if psid and payload == "GET_STARTED":
                app.logger.info(f"GET_STARTED from {psid}")
                upsert_psid_to_csv(psid)
                try:
                    send_text(
                        psid,
                        "Chào bạn! Bạn đã bắt đầu trò chuyện với 108Lab.\n"
                        "Bạn sẽ nhận nhắc hằng tuần khi được bật. Nhắn 'DỪNG' để hủy."
                    )
                except Exception as e:
                    app.logger.exception(f"Reply failed: {e}")

            # Message (text + attachments)
            msg = (evt.get("message") or {})
            text = (msg.get("text") or "").strip()
            if psid and text:
                upsert_psid_to_csv(psid)
                app.logger.info(f"MSG from {psid}: {text!r}")
                if text.upper() == "DỪNG":
                    try:
                        send_text(psid, "Bạn đã hủy nhận nhắc. Nhắn 'BẮT ĐẦU' để bật lại.")
                    except Exception as e:
                        app.logger.exception(f"Reply failed: {e}")

            # Attachments: nếu là ảnh → OCR & GỬI XÁC NHẬN (Người gửi / Số tiền / Ngày giờ)
            attachments = msg.get("attachments") or []
            for att in attachments:
                if (att.get("type") or "").lower() == "image":
                    payload = att.get("payload") or {}
                    image_url = payload.get("url")
                    if not image_url:
                        continue
                    app.logger.info(f"OCR image_url: {image_url}")
                    try:
                        # Có thể truyền expected nếu muốn so khớp; tạm để {}
                        result = verify_image_against_expected(image_url, expected={})
                        ext  = result.get("extracted")   or {}
                        conf = result.get("conf_stats")  or {}
                        raw  = (ext.get("raw_text") or "")[:1000]  # debug cắt 1000 ký tự

                        # Trường chính để XÁC NHẬN
                        sender  = ext.get("sender_name") or "-"
                        amt     = ext.get("amount")
                        when    = ext.get("datetime_text") or "-"

                        # Thông tin thêm
                        acc_from = ext.get("sender_account") or "-"
                        acc_to   = ext.get("account_number") or "-"
                        memo     = (ext.get("memo") or "-").strip()
                        txid     = ext.get("tx_code") or "-"

                        # Format số tiền
                        if isinstance(amt, int):
                            amt_txt = f"{amt:,} VND".replace(",", ".")
                        else:
                            amt_txt = ext.get("amount_text") or "-"

                        # ==== Tin nhắn xác nhận ====
                        confirm = (
                            "✅ ĐÃ NHẬN BẰNG CHỨNG CHUYỂN KHOẢN\n"
                            f"• Người gửi: {sender}\n"
                            f"• Số tiền: {amt_txt}\n"
                            f"• Ngày/giờ: {when}\n"
                            f"• STK gửi → nhận: {acc_from} → {acc_to}\n"
                            f"• Nội dung: {memo}\n"
                            f"• Mã GD: {txid}\n"
                        )

                        # Debug note nếu thiếu
                        missing = []
                        if not ext.get("sender_name"): missing.append("Người gửi")
                        if not ext.get("amount") and not ext.get("amount_text"): missing.append("Số tiền")
                        if not ext.get("datetime_text"): missing.append("Ngày/giờ")

                        if missing:
                            avg = conf.get("avg")
                            debug_note = "\n[Debug]"
                            debug_note += "\n- Thiếu: " + ", ".join(missing)
                            if avg is not None:
                                debug_note += f"\n- Độ tin cậy trung bình OCR: {avg:.2f}"
                            debug_note += "\n- Văn bản OCR (cắt ngắn):\n" + raw
                            confirm += "\n" + debug_note

                        send_text(psid, confirm)

                    except Exception as e:
                        app.logger.exception(f"OCR failed: {e}")
                        try:
                            send_text(psid, "⚠️ Xin lỗi, chưa đọc được ảnh. Vui lòng gửi lại ảnh rõ nét hơn.")
                        except:
                            pass

            # Referral (tham khảo)
            ref = extract_ref(evt)
            if psid and ref:
                app.logger.info(f"REF '{ref}' from {psid}")

    return "ok", 200

# 3) Cron endpoint – gửi theo CSV/ENV; hỗ trợ psids= & msg=
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)
    if not PAGE_TOKEN:
        return jsonify({"error": "PAGE_TOKEN is missing"}), 500

    psids_param = request.args.get("psids")   # ví dụ: "111,222"
    custom      = request.args.get("msg")     # nội dung tùy chọn

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
            time.sleep(0.2)  # giãn nhẹ tránh rate limit
        except Exception as e:
            app.logger.exception(f"Send failed for {p}: {e}")

    # Timestamp UTC + VN
    now_utc = dt.datetime.now(dt.timezone.utc)
    vn_tz   = ZoneInfo("Asia/Ho_Chi_Minh")
    now_vn  = now_utc.astimezone(vn_tz)

    return jsonify({
        "mode": mode,
        "targets": targets,
        "sent": sent,
        "server_time_utc": now_utc.isoformat(timespec="seconds"),
        "server_time_vietnam": now_vn.isoformat(timespec="seconds")
    })

# ================== MAIN ==================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
