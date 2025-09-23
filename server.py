# server.py
import os
import io
import csv
import json
import time
import base64
import logging
import datetime as dt
from typing import List, Dict, Tuple, Optional
from zoneinfo import ZoneInfo
from ocr_fast import fast_extract_amount_date_actor_detail

import requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv

# ====== OCR nhanh (phụ thuộc file ocr_fast.py trong repo) ======
try:
    # YÊU CẦU: ocr_fast.py phải có hàm này, trả về dict:
    # {
    #   "amount_text": "120.000đ",     # hoặc "120000"
    #   "amount_value": 120000,        # int hoặc None
    #   "when_text": "22:25-22/09/2025",
    #   "actor": "Doan Nhat Minh",
    #   "detail": "Gop vao quy",       # có thể None
    #   "lines": ["...","..."],        # list dòng OCR (debug)
    #   "spent": 12.34                 # giây
    # }
    from ocr_fast import fast_extract_amount_date_actor_detail
except Exception:  # fallback an toàn nếu thiếu file
    def fast_extract_amount_date_actor_detail(image_url: str, timeout: int = 30):
        return {
            "amount_text": None,
            "amount_value": None,
            "when_text": None,
            "actor": None,
            "detail": None,
            "lines": [],
            "spent": 0.0,
        }

# ====== ENV ======
load_dotenv()

PAGE_TOKEN       = os.getenv("PAGE_TOKEN")
VERIFY_TOKEN     = os.getenv("VERIFY_TOKEN", "changeme")
CRON_SECRET      = os.getenv("CRON_SECRET", "secret")
TEST_PSIDS       = [p.strip() for p in os.getenv("TEST_PSIDS", "").split(",") if p.strip()]

# CSV đọc (raw) — ví dụ: https://raw.githubusercontent.com/<owner>/<repo>/main/psids.csv
PSIDS_CSV_URL    = os.getenv("PSIDS_CSV_URL", "")

# CSV ghi qua GitHub API
GH_OWNER         = os.getenv("GH_OWNER")
GH_REPO          = os.getenv("GH_REPO")
GH_BRANCH        = os.getenv("GH_BRANCH", "main")
GH_FILE_PATH     = os.getenv("GH_FILE_PATH", "psids.csv")
GH_TOKEN         = os.getenv("GH_TOKEN")

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# ====== APP & LOG ======
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = app.logger
log.setLevel(logging.INFO)

# ====== Chống trả lời trùng ảnh (mid) trong vòng đời process ======
SEEN_MIDS = set()
MAX_SEEN = 5000  # giới hạn bộ nhớ

def seen_mid(mid: str) -> bool:
    if not mid:
        return False
    if mid in SEEN_MIDS:
        return True
    # làm sạch khi lớn
    if len(SEEN_MIDS) > MAX_SEEN:
        SEEN_MIDS.clear()
    SEEN_MIDS.add(mid)
    return False

# ====== Utils gửi tin ======
def send_text(psid: str, text: str):
    """Gửi tin nhắn văn bản tới 1 PSID bằng Send API."""
    if not PAGE_TOKEN:
        log.error("PAGE_TOKEN missing; cannot send messages")
        return
    url = "https://graph.facebook.com/v20.0/me/messages"
    r = requests.post(
        url,
        params={"access_token": PAGE_TOKEN},
        json={"recipient": {"id": psid}, "message": {"text": text}},
        timeout=20,
    )
    if r.status_code >= 400:
        log.error("Send API error %s: %s", r.status_code, r.text)
    r.raise_for_status()

# ====== CSV qua GitHub ======
CSV_FIELDS = ["psid", "user_facebook", "user_momo", "created_at_iso", "last_paid_month"]

def _gh_headers():
    return {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json",
    }

def gh_get_contents():
    """Lấy nội dung file + sha trên nhánh GH_BRANCH."""
    if not all([GH_OWNER, GH_REPO, GH_BRANCH, GH_FILE_PATH, GH_TOKEN]):
        raise RuntimeError("Missing GitHub env vars for write: GH_*")
    url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{GH_FILE_PATH}"
    params = {"ref": GH_BRANCH}
    r = requests.get(url, headers=_gh_headers(), params=params, timeout=20)
    if r.status_code == 404:
        return {"sha": None, "content": ""}  # file chưa tồn tại
    r.raise_for_status()
    data = r.json()
    content_b64 = data.get("content", "")
    sha = data.get("sha")
    content = base64.b64decode(content_b64).decode("utf-8") if content_b64 else ""
    return {"sha": sha, "content": content}

def gh_put_contents(new_text: str, sha: Optional[str], message: str):
    """Ghi (create/update) file CSV qua GitHub API."""
    if not all([GH_OWNER, GH_REPO, GH_BRANCH, GH_FILE_PATH, GH_TOKEN]):
        raise RuntimeError("Missing GitHub env vars for write: GH_*")
    url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{GH_FILE_PATH}"
    payload = {
        "message": message,
        "content": base64.b64encode(new_text.encode("utf-8")).decode("utf-8"),
        "branch": GH_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(url, headers=_gh_headers(), json=payload, timeout=25)
    if r.status_code >= 400:
        log.error("GitHub PUT error %s: %s", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def load_psids_csv() -> List[Dict[str, str]]:
    """Đọc CSV từ GH (ghi) nếu có token; nếu không, fallback PSIDS_CSV_URL (raw)."""
    text = ""
    # Ưu tiên lấy qua API (để chắc chắn sync với ghi)
    if all([GH_OWNER, GH_REPO, GH_BRANCH, GH_FILE_PATH, GH_TOKEN]):
        try:
            obj = gh_get_contents()
            text = obj.get("content", "")
        except Exception as e:
            log.exception(f"Failed gh_get_contents: {e}")
    # Fallback: PSIDS_CSV_URL (raw)
    if not text and PSIDS_CSV_URL:
        try:
            r = requests.get(PSIDS_CSV_URL, timeout=15)
            r.raise_for_status()
            text = r.text
        except Exception as e:
            log.exception(f"Failed to load CSV (read raw): {e}")

    rows: List[Dict[str, str]] = []
    if not text.strip():
        return rows
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    for row in reader:
        # đảm bảo đủ field
        fixed = {k: (row.get(k) or "").strip() for k in CSV_FIELDS}
        rows.append(fixed)
    return rows

def save_psids_csv(rows: List[Dict[str, str]], commit_msg: str):
    """Ghi CSV lên GitHub (yêu cầu GH_*)."""
    if not all([GH_OWNER, GH_REPO, GH_BRANCH, GH_FILE_PATH, GH_TOKEN]):
        log.warning("Skip save_psids_csv: missing GH_* envs")
        return
    # đóng gói CSV
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: r.get(k, "") for k in CSV_FIELDS})
    new_text = out.getvalue()
    # lấy sha hiện tại
    meta = gh_get_contents()
    sha = meta.get("sha")
    gh_put_contents(new_text, sha, commit_msg)

def ensure_psid_row(psid: str, user_facebook: Optional[str] = None):
    """Đảm bảo PSID có mặt trong CSV, nếu thiếu thì thêm mới."""
    rows = load_psids_csv()
    for r in rows:
        if r.get("psid") == psid:
            # fill user_facebook nếu trống
            if user_facebook and not (r.get("user_facebook") or "").strip():
                r["user_facebook"] = user_facebook
                save_psids_csv(rows, commit_msg=f"fill user_facebook for {psid}")
            return
    # thêm mới
    now_iso = dt.datetime.now(dt.timezone.utc).astimezone(VN_TZ).isoformat(timespec="seconds")
    rows.append({
        "psid": psid,
        "user_facebook": (user_facebook or "").strip(),
        "user_momo": "",
        "created_at_iso": now_iso,
        "last_paid_month": "",
    })
    save_psids_csv(rows, commit_msg=f"add new psid {psid}")

def update_last_paid_month(psid: str, yyyymm: str):
    rows = load_psids_csv()
    changed = False
    for r in rows:
        if r.get("psid") == psid:
            if r.get("last_paid_month") != yyyymm:
                r["last_paid_month"] = yyyymm
                changed = True
            break
    if changed:
        save_psids_csv(rows, commit_msg=f"set last_paid_month={yyyymm} for {psid}")

# ====== Chuẩn hoá & match tên (>=67%) ======
from rapidfuzz import fuzz
from rapidfuzz.distance import Levenshtein

def norm_name(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    # chuẩn hoá khoảng trắng và bỏ dấu cách dư
    s = " ".join(s.split())
    # thay một số ký tự đặc biệt thường gặp
    repl = {
        "đ": "d",
        "…": "",
        "•": "",
        "’": "'",
        "–": "-",
        "—": "-",
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    return s

def names_match(a: str, b: str) -> bool:
    """
    So khớp tên chịu lỗi OCR: đạt nếu similarity >= 67% (≈ 2/3 ký tự đúng).
    Dùng nhiều thước đo để bền hơn với khoảng trắng/lỗi nhỏ.
    """
    na, nb = norm_name(a), norm_name(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    sims = [
        fuzz.ratio(na, nb),
        fuzz.partial_ratio(na, nb),
        fuzz.token_set_ratio(na, nb),
        fuzz.token_sort_ratio(na, nb),
        int(100 * Levenshtein.normalized_similarity(na, nb)),
    ]
    best = max(sims)
    return best >= 67

# ====== Ngày tháng VN hiện tại ======
def parse_ddmmyyyy_from_text(t: Optional[str]) -> Optional[Tuple[int, int, int]]:
    """Tìm dd/mm/yyyy trong chuỗi."""
    if not t:
        return None
    # tìm pattern dd/mm/yyyy
    # không dùng regex module ngoài để đơn giản, tách thủ công:
    import re
    m = re.search(r'(\b\d{1,2})/(\d{1,2})/(\d{4}\b)', t)
    if not m:
        return None
    try:
        d = int(m.group(1))
        mth = int(m.group(2))
        y = int(m.group(3))
        # kiểm tra hợp lệ đơn giản
        dt.date(y, mth, d)
        return (d, mth, y)
    except Exception:
        return None

def is_current_month_vn(day: int, month: int, year: int) -> bool:
    now_vn = dt.datetime.now(dt.timezone.utc).astimezone(VN_TZ)
    return (month == now_vn.month and year == now_vn.year)

def ok(b: bool) -> str:
    return "✅" if b else "❌"

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

# 2) Receive events
@app.post("/webhook")
def webhook_receive():
    raw = request.get_data(as_text=True)
    log.info(f"RAW BODY: {raw[:1000]}")  # tránh log quá dài

    data = request.get_json(silent=True)
    if not isinstance(data, dict) or data.get("object") != "page":
        return "ok", 200

    for entry in data.get("entry", []):
        for evt in entry.get("messaging", []):
            psid = (((evt or {}).get("sender") or {}).get("id"))
            if not psid:
                continue
            log.info(f"PSID: {psid}")

            # GET_STARTED
            postback = (evt.get("postback") or {})
            payload = (postback.get("payload") or "")
            if payload == "GET_STARTED":
                ensure_psid_row(psid)
                try:
                    send_text(
                        psid,
                        "Chào bạn! Bạn đã bắt đầu trò chuyện.\n"
                        "Khi gửi ảnh sao kê MoMo, mình sẽ đọc số tiền, thời gian, người thực hiện và chi tiết.\n"
                        "Nếu trùng tên MoMo trong CSV + đúng 120.000đ + thuộc tháng hiện tại, mình sẽ dừng nhắc đến đầu tháng sau."
                    )
                except Exception as e:
                    log.exception(f"Send GET_STARTED failed: {e}")
                continue

            # Nhận ảnh -> OCR
            message = evt.get("message") or {}
            mid = message.get("mid")
            atts = message.get("attachments") or []

            # chống gửi trùng (một mid chỉ trả lời 1 lần)
            if mid and seen_mid(mid):
                log.info(f"Skip duplicate mid={mid}")
                continue

            # nếu có ảnh
            img_url = None
            for a in atts:
                if (a.get("type") == "image") and ((a.get("payload") or {}).get("url")):
                    img_url = a["payload"]["url"]
                    break

            if not img_url:
                # không có ảnh => nếu là text có thể cập nhật tên FB
                text = (message.get("text") or "").strip()
                if text:
                    # lưu PSID + user_facebook nếu trống
                    ensure_psid_row(psid, user_facebook=text if len(text) >= 3 else None)
                continue

            log.info(f"OCR image_url: {img_url}")

            # gọi OCR nhanh (timeout trong ocr_fast)
            try:
                result = fast_extract_amount_date_actor_detail(img_url, timeout=30)
            except Exception as e:
                log.exception(f"OCR exploded: {e}")
                result = {
                    "amount_text": None, "amount_value": None, "when_text": None,
                    "actor": None, "detail": None, "lines": [], "spent": 0.0,
                }

            amt_text   = result.get("amount_text") or "-"
            amount_val = result.get("amount_value")
            when_txt   = result.get("when_text") or "-"
            actor      = result.get("actor") or "-"
            detail     = result.get("detail") or "-"
            lines      = result.get("lines") or []
            spent      = float(result.get("spent") or 0.0)

            # bóc dd/mm/yyyy từ when_txt
            day_month_year = parse_ddmmyyyy_from_text(when_txt)
            month_year_str = "-"
            cond_date = False
            if day_month_year:
                d_, m_, y_ = day_month_year
                month_year_str = f"{d_:02d}/{m_:02d}/{y_:04d}"
                cond_date = is_current_month_vn(d_, m_, y_)

            # lấy dòng CSV cho PSID
            rows2 = load_psids_csv()
            target_idx = None
            for i, r in enumerate(rows2):
                if r.get("psid") == psid:
                    target_idx = i
                    break
            # nếu chưa có thì thêm mới
            if target_idx is None:
                ensure_psid_row(psid)

            # CHỈ dùng user_momo để so tên
            csv_fb_now = ""
            csv_momo_now = ""
            row_ok = False
            # làm lại sau ensure_psid_row
            rows2 = load_psids_csv()
            for i, r in enumerate(rows2):
                if r.get("psid") == psid:
                    row_ok = True
                    target_idx = i
                    csv_fb_now = (r.get("user_facebook") or "").strip()
                    csv_momo_now = (r.get("user_momo") or "").strip()
                    break

            # nếu OCR có actor và CSV chưa có user_facebook -> điền để tiện theo dõi (KHÔNG dùng cho auto-mute)
            if row_ok and actor != "-" and not csv_fb_now:
                rows2[target_idx]["user_facebook"] = actor
                try:
                    save_psids_csv(rows2, commit_msg=f"fill user_facebook for {psid} from OCR")
                except Exception as e:
                    log.exception(f"save fb name failed: {e}")

            # điều kiện
            cond_amount = (amount_val == 120000)
            cond_name   = bool(csv_momo_now and actor != "-" and names_match(csv_momo_now, actor))

            # similarity để in debug
            name_sim = None
            if csv_momo_now and actor != "-":
                try:
                    a = norm_name(csv_momo_now); b = norm_name(actor)
                    sims = [
                        fuzz.ratio(a, b),
                        fuzz.partial_ratio(a, b),
                        fuzz.token_set_ratio(a, b),
                        fuzz.token_sort_ratio(a, b),
                        int(100 * Levenshtein.normalized_similarity(a, b)),
                    ]
                    name_sim = max(sims)
                except Exception:
                    name_sim = None

            amount_ok = cond_amount
            date_ok   = cond_date
            name_ok   = cond_name

            # đủ 3 điều kiện => mute tới đầu tháng sau (cập nhật last_paid_month = YYYY-MM hiện tại)
            mute_ok = False
            if amount_ok and date_ok and name_ok:
                now_vn = dt.datetime.now(dt.timezone.utc).astimezone(VN_TZ)
                yyyymm = f"{now_vn.year:04d}-{now_vn.month:02d}"
                try:
                    update_last_paid_month(psid, yyyymm)
                    mute_ok = True
                except Exception as e:
                    log.exception(f"update_last_paid_month failed: {e}")

            when_txt_display = when_txt
            reply = (
                "✅ KẾT QUẢ (MoMo)\n"
                f"• Số tiền: {amt_text}\n"
                f"• Thời gian: {when_txt_display}\n"
                f"• Người thực hiện: {actor}\n"
                f"• Chi tiết: {detail}\n"
                f"(OCR ~{spent:.2f}s)\n\n"
                "📋 Checklist (đối chiếu với user_momo):\n"
                + "\n".join("• " + line for line in [
                    f"{ok(row_ok)} Có dòng CSV cho PSID",
                    f"{ok(bool(csv_momo_now))} Có user_momo trong CSV (MoMo='{csv_momo_now or '-'}')",
                    f"{ok(name_ok)} Tên khớp user_momo ↔ OCR: '{csv_momo_now or '-'}' ~ '{actor}'"
                    + (f" (similarity≈{name_sim}%)" if name_sim is not None else ""),
                    f"{ok(amount_ok)} Số tiền = 120.000đ (OCR: {amt_text})",
                    f"{ok(date_ok)} Ngày thuộc tháng hiện tại (OCR: {month_year_str})",
                    f"{ok(mute_ok)} Đã đặt tắt nhắc tới đầu tháng sau",
                ])
            )

            # kèm debug lines OCR để bạn theo dõi
            if lines:
                preview = lines[:30]  # tránh quá dài
                body = "\n".join(preview)
                reply += f"\n\n[DEBUG] OCR lines ({len(lines)}):\n" + body

            # gửi kết quả
            try:
                send_text(psid, reply)
            except Exception as e:
                log.exception(f"send reply failed: {e}")

    return "ok", 200

# 3) Endpoint cron gửi hằng tuần (tôn trọng last_paid_month)
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)
    if not PAGE_TOKEN:
        return jsonify({"error": "PAGE_TOKEN is missing"}), 500

    # Cho phép override psids= và msg=
    psids_param = request.args.get("psids")  # "111,222"
    custom_msg  = request.args.get("msg")

    # Lấy danh sách mục tiêu
    targets = []
    mode = "CSV"
    rows = load_psids_csv()
    now_vn = dt.datetime.now(dt.timezone.utc).astimezone(VN_TZ)
    current_yyyymm = f"{now_vn.year:04d}-{now_vn.month:02d}"

    if psids_param:
        mode = "psids"
        targets = [p.strip() for p in psids_param.split(",") if p.strip()]
    elif rows:
        # bỏ qua ai đã last_paid_month == current_yyyymm
        for r in rows:
            psid = (r.get("psid") or "").strip()
            if not psid:
                continue
            if (r.get("last_paid_month") or "") == current_yyyymm:
                continue  # đã trả trong tháng
            targets.append(psid)
    else:
        mode = "TEST_PSIDS"
        targets = TEST_PSIDS

    today = now_vn.strftime("%d/%m/%Y")
    msg = custom_msg or f"Nhắc đóng quỹ tháng {now_vn.month:02d}/{now_vn.year}: 120.000đ. Gửi ảnh MoMo để xác nhận tự động ({today})."

    sent = 0
    for p in targets:
        try:
            send_text(p, msg)
            sent += 1
            time.sleep(0.2)
        except Exception as e:
            log.exception(f"Send failed for {p}: {e}")

    return jsonify({
        "mode": mode,
        "targets": targets,
        "sent": sent,
        "server_time_vietnam": now_vn.isoformat(timespec="seconds")
    })

# ====== MAIN ======
if __name__ == "__main__":
    # khi chạy local: python server.py
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
