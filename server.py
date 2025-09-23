# server.py
import os
import re
import json
import time
import csv
import base64
import logging
import datetime as dt
from io import StringIO
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# ---- OCR nhanh (tùy chọn) ----
try:
    # phải trả về dict: {amount_text, date_text, actor_name, detail_text, lines, spent_sec}
    from ocr_fast import fast_extract_amount_date  # optional
except Exception as e:
    fast_extract_amount_date = None
    _OCR_IMPORT_ERR = e

# ================= ENV =================
load_dotenv()

PAGE_TOKEN      = os.getenv("PAGE_TOKEN")
VERIFY_TOKEN    = os.getenv("VERIFY_TOKEN", "changeme")
CRON_SECRET     = os.getenv("CRON_SECRET", "secret")

# CSV: đọc (raw URL) và ghi (GitHub API)
PSIDS_CSV_URL   = os.getenv("PSIDS_CSV_URL", "")
GH_OWNER        = os.getenv("GH_OWNER", "")
GH_REPO         = os.getenv("GH_REPO", "")
GH_BRANCH       = os.getenv("GH_BRANCH", "main")
GH_FILE_PATH    = os.getenv("GH_FILE_PATH", "psids.csv")
GH_TOKEN        = os.getenv("GH_TOKEN", "")

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# THÊM cột 'status' (1=đã đóng, 0=chưa)
CSV_HEADERS = [
    "psid", "user_facebook", "user_momo",
    "mute_until", "created_at_iso", "last_user_msg_iso",
    "status"
]

# ================= APP & LOG =================
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ================= Messenger helpers =================
def send_text(psid: str, text: str):
    if not PAGE_TOKEN:
        app.logger.error("PAGE_TOKEN missing; cannot send messages.")
        return
    url = "https://graph.facebook.com/v23.0/me/messages"
    r = requests.post(
        url,
        params={"access_token": PAGE_TOKEN},
        json={"recipient": {"id": psid}, "message": {"text": text}},
        timeout=20,
    )
    if r.status_code >= 400:
        app.logger.error("Send API error %s: %s", r.status_code, r.text)
    r.raise_for_status()

def get_user_profile(psid: str) -> Optional[dict]:
    if not PAGE_TOKEN:
        return None
    try:
        r = requests.get(
            f"https://graph.facebook.com/v23.0/{psid}",
            params={"fields": "first_name,last_name,profile_pic", "access_token": PAGE_TOKEN},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def get_display_name(profile: Optional[dict]) -> str:
    if not profile:
        return ""
    first = (profile.get("first_name") or "").strip()
    last  = (profile.get("last_name") or "").strip()
    return f"{first} {last}".strip()

# ================= Date helpers (strict dd/mm/yyyy with slash) =================
SLASH_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")

def extract_strict_slash_date_from_text(text: str) -> Optional[Tuple[str, Tuple[int, int]]]:
    if not text:
        return None
    m = SLASH_DATE_RE.search(text)
    if not m:
        return None
    _d, mth, y = m.groups()
    month = int(mth)
    year = int(y)
    if not (1 <= month <= 12 and 2000 <= year <= 2100):
        return None
    return (m.group(0), (month, year))

def is_current_month_vn(month: int, year: int) -> bool:
    today = datetime.now(VN_TZ).date()
    return (month == today.month) and (year == today.year)

def _first_day_next_month_vn() -> dt.date:
    today = datetime.now(VN_TZ).date()
    year = today.year + (1 if today.month == 12 else 0)
    month = 1 if today.month == 12 else today.month + 1
    return dt.date(year, month, 1)

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def parse_amount_to_int(amount_text: Optional[str]) -> Optional[int]:
    if not amount_text:
        return None
    digits = re.sub(r"[^\d]", "", amount_text)
    return int(digits) if digits.isdigit() else None

# ================= CSV helpers (GitHub) =================
def load_psids_csv() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if not PSIDS_CSV_URL:
        return rows
    try:
        resp = requests.get(PSIDS_CSV_URL, timeout=12)
        resp.raise_for_status()
        text = resp.text
        if not text.strip():
            return rows
        f = StringIO(text)
        reader = csv.DictReader(f)
        for r in reader:
            # fill missing keys with defaults
            row = {h: (r.get(h, "") or "").strip() for h in CSV_HEADERS}
            if "status" not in r:
                row["status"] = "0"
            rows.append(row)
    except Exception as e:
        app.logger.exception(f"Failed to load CSV (read): {e}")
    return rows

def _github_contents_url() -> str:
    return f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{GH_FILE_PATH}"

def _get_file_sha() -> Optional[str]:
    if not all([GH_OWNER, GH_REPO, GH_FILE_PATH, GH_BRANCH, GH_TOKEN]):
        return None
    headers = {"Authorization": f"Bearer {GH_TOKEN}", "Accept": "application/vnd.github+json"}
    params = {"ref": GH_BRANCH}
    r = requests.get(_github_contents_url(), headers=headers, params=params, timeout=15)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json().get("sha")

def save_psids_csv(rows: List[Dict[str, str]], commit_msg: str) -> bool:
    if not all([GH_OWNER, GH_REPO, GH_FILE_PATH, GH_BRANCH, GH_TOKEN]):
        app.logger.error("Missing GH_* env; cannot write CSV.")
        return False

    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=CSV_HEADERS)
    writer.writeheader()
    for r in rows:
        writer.writerow({
            "psid": r.get("psid", "").strip(),
            "user_facebook": r.get("user_facebook", "").strip(),
            "user_momo": r.get("user_momo", "").strip(),
            "mute_until": r.get("mute_until", "").strip(),
            "created_at_iso": r.get("created_at_iso", "").strip(),
            "last_user_msg_iso": r.get("last_user_msg_iso", "").strip(),
            "status": (r.get("status", "").strip() or "0"),
        })
    content_b64 = base64.b64encode(out.getvalue().encode("utf-8")).decode("ascii")

    sha = _get_file_sha()
    payload = {"message": commit_msg, "content": content_b64, "branch": GH_BRANCH}
    if sha:
        payload["sha"] = sha

    headers = {"Authorization": f"Bearer {GH_TOKEN}", "Accept": "application/vnd.github+json"}
    r = requests.put(_github_contents_url(), headers=headers, json=payload, timeout=20)
    if r.status_code >= 400:
        app.logger.error("GitHub update CSV failed %s: %s", r.status_code, r.text)
        return False
    return True

def upsert_row_by_psid(rows: List[Dict[str, str]], psid: str, fb_name: str) -> List[Dict[str, str]]:
    found = False
    for r in rows:
        if r.get("psid") == psid:
            found = True
            if not (r.get("user_facebook") or "").strip() and fb_name:
                r["user_facebook"] = fb_name
            if not (r.get("status") or "").strip():
                r["status"] = "0"
            break
    if not found:
        rows.append({
            "psid": psid,
            "user_facebook": fb_name or "",
            "user_momo": "",
            "mute_until": "",
            "created_at_iso": datetime.now(VN_TZ).isoformat(timespec="seconds"),
            "last_user_msg_iso": "",
            "status": "0",
        })
    return rows

# ================= Strong de-dup (mid + image_url) =================
_processed_mids: Dict[str, float] = {}
_processed_images: Dict[str, float] = {}
DEDUP_TTL_SEC = int(os.getenv("DEDUP_TTL_SEC", "7200"))  # 2h mặc định

def _gc_dedup():
    now = time.time()
    for d in (_processed_mids, _processed_images):
        for k, ts in list(d.items()):
            if now - ts > DEDUP_TTL_SEC:
                d.pop(k, None)

def seen_mid(mid: Optional[str]) -> bool:
    if not mid:
        return False
    _gc_dedup()
    if mid in _processed_mids:
        return True
    _processed_mids[mid] = time.time()
    return False

def seen_image(url: Optional[str]) -> bool:
    if not url:
        return False
    _gc_dedup()
    if url in _processed_images:
        return True
    _processed_images[url] = time.time()
    return False

# ================= Routes =================
@app.get("/")
def root():
    return "OK", 200

@app.get("/webhook")
def webhook_verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge"), 200
    return "Verification failed", 403

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

            # bỏ qua delivery/read events
            if evt.get("delivery") or evt.get("read"):
                continue

            mid = ((evt.get("message") or {}).get("mid")) or ((evt.get("postback") or {}).get("mid"))
            if mid and seen_mid(mid):
                app.logger.info("Skip duplicate by mid=%s", mid)
                continue

            # TẠO/UPDATE DÒNG CSV + TOUCH last_user_msg_iso
            rows = load_psids_csv()
            rows = upsert_row_by_psid(rows, psid, fb_name="")
            idx = next((i for i, r in enumerate(rows) if r.get("psid") == psid), None)
            if idx is not None:
                rows[idx]["last_user_msg_iso"] = _now_utc_iso()
                if not (rows[idx].get("user_facebook") or "").strip():
                    prof = get_user_profile(psid)
                    disp = get_display_name(prof)
                    if disp:
                        rows[idx]["user_facebook"] = disp
                if not (rows[idx].get("created_at_iso") or "").strip():
                    rows[idx]["created_at_iso"] = datetime.now(VN_TZ).isoformat(timespec="seconds")
                save_psids_csv(rows, commit_msg=f"touch last_user_msg for {psid}")

            # GET_STARTED
            if evt.get("postback", {}).get("payload") == "GET_STARTED":
                send_text(psid,
                    "Chào bạn! Gửi ảnh biên lai MoMo để hệ thống kiểm tra.\n"
                    "Trong 24h kể từ khi bạn nhắn, tôi có thể nhắc tự động.")
                continue

            msg = evt.get("message") or {}
            text_in = (msg.get("text") or "").strip().lower()

            # keyword mở phiên (không bắt buộc)
            if text_in in {"bat dau", "bắt đầu", "nhắc tuần", "nhac tuan"}:
                send_text(psid,
                    "✅ Đã ghi nhận tương tác. Trong 24h tới, tôi có thể nhắc tự động.\n"
                    "Gửi ảnh MoMo để tự dừng nhắc khi đủ 120.000đ.")
                continue

            # Xử lý ảnh (1 ảnh đầu tiên + chống trùng)
            atts: List[Dict] = msg.get("attachments") or []
            processed_one = False
            for att in atts:
                if att.get("type") != "image":
                    continue
                image_url = (att.get("payload") or {}).get("url")
                if not image_url:
                    continue

                if mid:
                    _processed_mids[mid] = time.time()
                else:
                    if seen_image(image_url):
                        app.logger.info("Skip duplicate by image_url=%s", image_url)
                        continue

                if processed_one:
                    break

                if fast_extract_amount_date is None:
                    app.logger.error("OCR module not available: %s", _OCR_IMPORT_ERR)
                    send_text(psid, "❌ OCR chưa sẵn sàng trên server.")
                    processed_one = True
                    break

                app.logger.info("OCR image_url: %s", image_url)
                try:
                    result = fast_extract_amount_date(image_url)
                    lines    = result.get("lines", []) or []
                    amt_text = result.get("amount_text") or "-"
                    when_txt = result.get("date_text") or "-"
                    actor    = result.get("actor_name") or "-"
                    detail   = result.get("detail_text") or "-"
                    spent    = result.get("spent_sec", 0.0)

                    # ---- kiểm tra điều kiện 'đã đóng' ----
                    # 1) số tiền
                    amount_val = parse_amount_to_int(amt_text)
                    money_ok = (amount_val >= 120000)

                    # 2) ngày thuộc tháng hiện tại (tìm dd/mm/yyyy trong when_txt hoặc trong các line)
                    month_ok = False
                    date_found = None
                    hit = extract_strict_slash_date_from_text(when_txt)
                    if not hit:
                        # fallback: quét các dòng OCR
                        text_join = "\n".join(lines)
                        hit = extract_strict_slash_date_from_text(text_join)
                    if hit:
                        date_found, (mm, yy) = hit
                        month_ok = is_current_month_vn(mm, yy)

                    # nếu đủ hai điều kiện -> status=1 & mute_until=đầu tháng sau
                    updated = False
                    rows2 = load_psids_csv()
                    t_idx = next((i for i, r in enumerate(rows2) if r.get("psid") == psid), None)
                    if t_idx is not None and money_ok and month_ok:
                        next1 = _first_day_next_month_vn()
                        rows2[t_idx]["status"] = "1"
                        rows2[t_idx]["mute_until"] = next1.isoformat()
                        if not rows2[t_idx].get("created_at_iso"):
                            rows2[t_idx]["created_at_iso"] = datetime.now(VN_TZ).isoformat(timespec="seconds")
                        save_psids_csv(rows2, commit_msg=f"mark paid {psid} until {next1.isoformat()}")
                        updated = True

                    # ghép phản hồi
                    reply = (
                        "✅ KẾT QUẢ (MoMo)\n"
                        f"• Số tiền: {amt_text}\n"
                        f"• Thời gian: {when_txt}\n"
                        f"• Người thực hiện: {actor}\n"
                        f"• Chi tiết: {detail}\n"
                        f"(OCR ~{spent:.2f}s)"
                    )
                    if updated:
                        reply += "\n\n🔔 Đã đánh dấu **ĐÃ ĐÓNG (status=1)** và dừng nhắc tới đầu tháng sau."
                    else:
                        reply += "\n\nℹ️ Chưa đánh dấu đã đóng (cần 120.000đ và ngày thuộc **tháng hiện tại**)."

                    send_text(psid, reply)

                    processed_one = True
                    _processed_images[image_url] = time.time()
                    break

                except Exception as e:
                    app.logger.exception("OCR failed: %s", e)
                    send_text(psid, "❌ Xin lỗi, không đọc được ảnh này. Bạn thử chụp rõ hơn/đủ sáng nhé.")
                    processed_one = True
                    break

            # Lệnh “DỪNG” để mute tới đầu tháng sau (không đổi status)
            if text_in in {"dung", "dừng", "stop"}:
                rows = load_psids_csv()
                rows = upsert_row_by_psid(rows, psid, fb_name="")
                next1 = _first_day_next_month_vn()
                for r in rows:
                    if r.get("psid") == psid:
                        r["mute_until"] = next1.isoformat()
                        if not r.get("created_at_iso"):
                            r["created_at_iso"] = datetime.now(VN_TZ).isoformat(timespec="seconds")
                        break
                save_psids_csv(rows, commit_msg=f"user requested stop until {next1.isoformat()}")
                send_text(psid, f"🔕 Đã dừng nhắc đến {next1.strftime('%d/%m/%Y')}.")

    return "ok", 200

# ================= Cron gửi nhắc tuần (24h window + status) =================
def _parse_iso(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)

    rows = load_psids_csv()

    # dùng dt.* cho đồng nhất imports
    now_utc = dt.datetime.now(dt.timezone.utc)
    today_vn = dt.datetime.now(VN_TZ).date()

    sent = 0
    targets: List[str] = []
    changed = False  # nếu có reset status/mute_until sang tháng mới thì lưu lại

    for r in rows:
        psid = (r.get("psid") or "").strip()
        if not psid:
            continue

        # ==== Reset đầu tháng (phòng hờ) ====
        # Nếu đã qua tháng mới và row đang "đã đóng" nhưng KHÔNG có mute_until (do cập nhật thủ công),
        # thì reset status về 0 để không treo mãi.
        try:
            if today_vn.day == 1 and (r.get("status") or "0") == "1" and not (r.get("mute_until") or "").strip():
                r["status"] = "0"
                changed = True
        except Exception:
            pass

        # ==== Reset khi hết mute_until (cơ chế chính) ====
        mute_until = (r.get("mute_until") or "").strip()
        if mute_until:
            try:
                mu = dt.date.fromisoformat(mute_until)
                # Nếu đã qua ngày mute → coi như sang kỳ mới: reset status về 0 và xóa mute_until
                if mu < today_vn:
                    if (r.get("status") or "0") == "1" or r.get("mute_until"):
                        r["status"] = "0"
                        r["mute_until"] = ""
                        changed = True
            except Exception:
                pass

        # ==== BỎ QUA nếu status=1 (đã đóng trong tháng này) ====
        if (r.get("status") or "0") == "1":
            continue

        # ==== BỎ QUA nếu đang còn mute_until hiệu lực ====
        mute_until = (r.get("mute_until") or "").strip()
        if mute_until:
            try:
                mu = dt.date.fromisoformat(mute_until)
                if mu >= today_vn:
                    continue  # vẫn đang mute
            except Exception:
                pass

        # ==== 24h window (chỉ gửi nếu user có tương tác trong 24h qua) ====
        last_iso = (r.get("last_user_msg_iso") or "").strip()
        try:
            last_dt = dt.datetime.fromisoformat(last_iso) if last_iso else None
        except Exception:
            last_dt = None

        # Chỉ GỬI nếu có last_dt và (now_utc - last_dt) <= 24h
        if (not last_dt) or ((now_utc - last_dt) > dt.timedelta(hours=24)):
            continue

        targets.append(psid)

    if changed:
        save_psids_csv(rows, commit_msg="auto reset status/mute at new month")

    msg = (
        f"Nhắc đóng quỹ 120.000đ tháng này ({dt.datetime.now(VN_TZ):%m/%Y}). "
        f"Gửi ảnh MoMo để hệ thống tự đánh dấu đã đóng. Gửi 'Dừng' để tạm thời không nhận thông báo!"
    )

    for p in targets:
        try:
            send_text(p, msg)
            sent += 1
            time.sleep(0.2)  # nhẹ tránh rate limit
        except Exception as e:
            app.logger.exception(f"Send failed for {p}: {e}")

    return jsonify({
        "sent": sent,
        "eligible": len(targets),
        "targets": targets,
        "today_vn": today_vn.isoformat()
    })


# ================= Main =================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
