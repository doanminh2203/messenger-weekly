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

import requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from ocr_fast import fast_extract_amount_date  # OCR nhanh đã có

# ====== ENV ======
load_dotenv()

PAGE_TOKEN      = os.getenv("PAGE_TOKEN")
VERIFY_TOKEN    = os.getenv("VERIFY_TOKEN", "changeme")
CRON_SECRET     = os.getenv("CRON_SECRET", "secret")

# CSV đọc (raw) và ghi (GitHub API)
PSIDS_CSV_URL   = os.getenv("PSIDS_CSV_URL", "")  # ví dụ: https://raw.githubusercontent.com/<owner>/<repo>/main/psids.csv
GH_OWNER        = os.getenv("GH_OWNER", "")
GH_REPO         = os.getenv("GH_REPO", "")
GH_BRANCH       = os.getenv("GH_BRANCH", "main")
GH_FILE_PATH    = os.getenv("GH_FILE_PATH", "psids.csv")
GH_TOKEN        = os.getenv("GH_TOKEN", "")

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# ====== APP & LOG ======
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ====== HELPER: Messenger ======
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

# ====== HELPER: CSV (load/save GitHub) ======
CSV_HEADERS = ["psid", "name", "mute_until", "created_at_iso"]

def _now_vn_date() -> dt.date:
    return dt.datetime.now(VN_TZ).date()

def _first_day_next_month_vn() -> dt.date:
    today = _now_vn_date()
    year = today.year + (1 if today.month == 12 else 0)
    month = 1 if today.month == 12 else today.month + 1
    return dt.date(year, month, 1)

def load_psids_csv() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if not PSIDS_CSV_URL:
        return rows
    try:
        resp = requests.get(PSIDS_CSV_URL, timeout=12)
        resp.raise_for_status()
        text = resp.text
        # cho phép CSV rỗng
        if not text.strip():
            return rows
        f = StringIO(text)
        reader = csv.DictReader(f)
        for r in reader:
            # đảm bảo tất cả key tồn tại
            row = {h: (r.get(h, "") or "").strip() for h in CSV_HEADERS}
            rows.append(row)
    except Exception as e:
        app.logger.exception(f"Failed to load CSV (read): {e}")
    return rows

def _github_contents_url() -> str:
    return f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{GH_FILE_PATH}"

def _get_file_sha() -> Optional[str]:
    """Lấy sha hiện tại của file trên nhánh GH_BRANCH (để cập nhật)."""
    if not all([GH_OWNER, GH_REPO, GH_FILE_PATH, GH_BRANCH, GH_TOKEN]):
        return None
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    params = {"ref": GH_BRANCH}
    r = requests.get(_github_contents_url(), headers=headers, params=params, timeout=15)
    if r.status_code == 404:
        return None  # chưa có file
    r.raise_for_status()
    return r.json().get("sha")

def save_psids_csv(rows: List[Dict[str, str]], commit_msg: str) -> bool:
    """Ghi CSV lên GitHub (create/update). Trả True nếu OK."""
    if not all([GH_OWNER, GH_REPO, GH_FILE_PATH, GH_BRANCH, GH_TOKEN]):
        app.logger.error("Missing GH_* env; cannot write CSV.")
        return False

    # đảm bảo header & thứ tự cột
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=CSV_HEADERS)
    writer.writeheader()
    for r in rows:
        writer.writerow({
            "psid": r.get("psid", "").strip(),
            "name": r.get("name", "").strip(),
            "mute_until": r.get("mute_until", "").strip(),
            "created_at_iso": r.get("created_at_iso", "").strip(),
        })
    content_b64 = base64.b64encode(out.getvalue().encode("utf-8")).decode("ascii")

    sha = _get_file_sha()  # None nếu file mới
    payload = {
        "message": commit_msg,
        "content": content_b64,
        "branch": GH_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    r = requests.put(_github_contents_url(), headers=headers, json=payload, timeout=20)
    if r.status_code >= 400:
        app.logger.error("GitHub update CSV failed %s: %s", r.status_code, r.text)
        return False
    return True

def upsert_row_by_psid(rows: List[Dict[str, str]], psid: str, name: str) -> List[Dict[str, str]]:
    """Đảm bảo có 1 dòng cho PSID; nếu chưa có thì thêm mới (cập nhật name nếu trống)."""
    found = False
    for r in rows:
        if r.get("psid") == psid:
            found = True
            if not (r.get("name") or "").strip() and name:
                r["name"] = name
            break
    if not found:
        rows.append({
            "psid": psid,
            "name": name or "",
            "mute_until": "",
            "created_at_iso": dt.datetime.now(VN_TZ).isoformat(timespec="seconds"),
        })
    return rows

# ====== HELPER: Name/Amount/Date matching ======
import unicodedata
def _strip_accents(s: str) -> str:
    s_norm = unicodedata.normalize("NFD", s or "")
    s_no = "".join(ch for ch in s_norm if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFC", s_no)

def norm_name(s: str) -> str:
    s2 = _strip_accents(s).lower().strip()
    s2 = re.sub(r"\s+", " ", s2)
    return s2

def match_name(a: str, b: str) -> bool:
    """So sánh tên không dấu, bỏ khoảng trắng thừa."""
    return norm_name(a) == norm_name(b) if (a and b) else False

def parse_amount_to_int(amount_text: Optional[str]) -> Optional[int]:
    if not amount_text:
        return None
    digits = re.sub(r"[^\d]", "", amount_text)
    return int(digits) if digits.isdigit() else None

DATE_PATTERNS = [
    re.compile(r"(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})"),  # dd/mm/yyyy
]

def extract_month_year(text: Optional[str]) -> Optional[Tuple[int, int]]:
    if not text:
        return None
    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if m:
            d, mth, y = m.groups()
            month = int(mth)
            year = int(y)
            if year < 100:  # 25 -> 2025 (đoán)
                year += 2000
            return (month, year)
    return None

def is_current_month_vn(month: int, year: int) -> bool:
    today = _now_vn_date()
    return (month == today.month) and (year == today.year)

# ====== DEDUP MESSAGE ======
_recent_mids: Dict[str, float] = {}

def seen_mid(mid: str, ttl_sec: int = 600) -> bool:
    now = time.time()
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

            mid = ((evt.get("message") or {}).get("mid")) or ((evt.get("postback") or {}).get("mid"))
            if mid and seen_mid(mid):
                app.logger.info("Skip duplicate mid=%s", mid)
                continue

            # GET_STARTED
            if evt.get("postback", {}).get("payload") == "GET_STARTED":
                send_text(psid,
                    "Chào bạn! Gửi ảnh biên lai MoMo để mình kiểm tra và tự dừng nhắc khi đã đóng 120.000đ trong tháng.")
                continue

            msg = evt.get("message") or {}
            text_in = (msg.get("text") or "").strip()

            # Lưu/ cập nhật dòng CSV khi user nhắn bất cứ thứ gì (để có tên nếu bạn chủ động cập nhật tay)
            # Ở đây ta chưa có tên Messenger; tên để match lấy từ biên lai MoMo (actor_name).
            # Tuy nhiên vẫn đảm bảo có hàng PSID trong CSV.
            rows = load_psids_csv()
            rows = upsert_row_by_psid(rows, psid, name="")  # name rỗng nếu chưa biết
            save_psids_csv(rows, commit_msg="upsert psid on message")

            # Xử lý ảnh
            atts: List[Dict] = msg.get("attachments") or []
            for att in atts:
                if att.get("type") == "image":
                    image_url = (att.get("payload") or {}).get("url")
                    if not image_url:
                        continue
                    app.logger.info("OCR image_url: %s", image_url)
                    try:
                        result = fast_extract_amount_date(image_url)

                        # Debug lines
                        lines = result.get("lines", []) or []
                        app.logger.info("=== OCR LINES (%d) ===\n%s\n=== END OCR LINES ===",
                                        len(lines), "\n".join(lines))

                        amt_text  = result.get("amount_text") or "-"
                        when_text = result.get("date_text") or "-"
                        actor     = result.get("actor_name") or "-"
                        detail    = result.get("detail_text") or "-"
                        spent     = result.get("spent_sec", 0.0)

                        # Kết quả trước khi auto-mute
                        preview_lines = "\n".join(lines[:20])
                        if len(preview_lines) > 1200:
                            preview_lines = preview_lines[:1200] + "…"

                        # ====== AUTO-MUTE LOGIC ======
                        # Điều kiện: tên (CSV) khớp tên trong biên lai + amount == 120000 + tháng/năm hiện tại (VN)
                        amount_val = parse_amount_to_int(result.get("amount_text"))
                        month_year = extract_month_year(result.get("date_text"))
                        did_mute = False
                        mute_until_str = ""

                        if amount_val == 120000:
                            # tìm hàng trong CSV có name trùng với actor (không dấu)
                            rows2 = load_psids_csv()
                            actor_norm = norm_name(actor)
                            target_idx = None
                            for idx, r in enumerate(rows2):
                                if r.get("psid") == psid and r.get("name"):
                                    # ưu tiên hàng đúng psid có name
                                    if match_name(r["name"], actor):
                                        target_idx = idx
                                        break
                            if target_idx is None:
                                # nếu chưa có name cho psid, thử match theo name trong CSV (nhóm 1 group duy nhất)
                                for idx, r in enumerate(rows2):
                                    if r.get("name") and match_name(r["name"], actor):
                                        target_idx = idx
                                        break
                            # nếu vẫn None → gán name cho PSID hiện tại bằng actor, rồi mute
                            if target_idx is None:
                                # gán vào hàng của PSID hiện tại
                                for idx, r in enumerate(rows2):
                                    if r.get("psid") == psid:
                                        rows2[idx]["name"] = actor
                                        target_idx = idx
                                        break

                            if target_idx is not None:
                                next1 = _first_day_next_month_vn()
                                rows2[target_idx]["mute_until"] = next1.isoformat()
                                # đảm bảo tạo ngày nếu trống
                                if not rows2[target_idx].get("created_at_iso"):
                                    rows2[target_idx]["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                                if save_psids_csv(rows2, commit_msg=f"auto mute {psid} until {next1.isoformat()}"):
                                    did_mute = True
                                    mute_until_str = next1.strftime("%d/%m/%Y")

                        # ====== TRẢ TIN NHẮN ======
                        reply = (
                            "✅ KẾT QUẢ (MoMo)\n"
                            f"• Số tiền: {amt_text}\n"
                            f"• Thời gian: {when_text}\n"
                            f"• Người thực hiện: {actor}\n"
                            f"• Chi tiết: {detail}\n"
                            f"(OCR ~{spent}s)\n"
                        )
                        if did_mute:
                            reply += f"\n🔕 Đã ghi nhận thanh toán 120.000đ trong tháng hiện tại — tạm **dừng nhắc** đến **{mute_until_str}**."
                        else:
                            reply += "\nℹ️ Chưa đủ điều kiện dừng nhắc (cần đúng tên, 120.000đ, và tháng hiện tại)."

                        reply += f"\n\n[DEBUG] OCR lines ({len(lines)}):\n{preview_lines}"
                        send_text(psid, reply)

                    except Exception as e:
                        app.logger.exception("OCR failed: %s", e)
                        send_text(psid, "❌ Xin lỗi, không đọc được ảnh này. Bạn thử chụp rõ hơn/đủ sáng nhé.")

            # Text “DỪNG”
            t = (text_in or "").strip().lower()
            if t in {"dung", "dừng", "stop"}:
                rows = load_psids_csv()
                rows = upsert_row_by_psid(rows, psid, name="")
                next1 = _first_day_next_month_vn()
                # tìm hàng psid và mute tới đầu tháng sau
                for r in rows:
                    if r.get("psid") == psid:
                        r["mute_until"] = next1.isoformat()
                        if not r.get("created_at_iso"):
                            r["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                        break
                save_psids_csv(rows, commit_msg=f"user requested stop until {next1.isoformat()}")
                send_text(psid, f"Đã dừng nhắc đến {next1.strftime('%d/%m/%Y')}.")

    return "ok", 200

# Cron gửi nhắc — bỏ qua ai đang mute
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)

    rows = load_psids_csv()
    today = _now_vn_date()
    sent = 0
    targets: List[str] = []

    for r in rows:
        psid = (r.get("psid") or "").strip()
        if not psid:
            continue
        mute_until = (r.get("mute_until") or "").strip()
        if mute_until:
            try:
                mu = dt.date.fromisoformat(mute_until)
                if mu > today or mu == today:
                    continue  # đang mute
            except Exception:
                pass
        targets.append(psid)

    msg = f"Nhắc đóng quỹ 120.000đ tháng này ({today.strftime('%m/%Y')}). Gửi ảnh MoMo để hệ thống tự dừng nhắc."
    for p in targets:
        try:
            send_text(p, msg)
            sent += 1
            time.sleep(0.2)
        except Exception as e:
            app.logger.exception(f"Send failed for {p}: {e}")

    return jsonify({
        "sent": sent,
        "eligible": len(targets),
        "date_vn": today.isoformat()
    })

# ====== MAIN ======
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
