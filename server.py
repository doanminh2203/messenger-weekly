# server.py
import os
import io
import re
import csv
import json
import time
import base64
import logging
import unicodedata
import datetime as dt
from typing import List, Dict, Tuple, Optional
from zoneinfo import ZoneInfo

import requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv

# ====== OCR nhanh (file ocr_fast.py trong repo) ======
try:
    # Kỳ vọng trả về:
    # {
    #   "amount_text": "120.000đ", "amount_value": 120000,
    #   "when_text": "22:25-22/09/2025",
    #   "actor": "Doan Nhat Minh",
    #   "detail": "Gop vao quy",
    #   "lines": [...], "spent": 12.3
    # }
    from ocr_fast import fast_extract_amount_date_actor_detail
except Exception:
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

# CSV đọc (raw)
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

# ====== Chống trả lời trùng ảnh (mid) ======
SEEN_MIDS = set()
MAX_SEEN = 5000

def seen_mid(mid: str) -> bool:
    if not mid:
        return False
    if mid in SEEN_MIDS:
        return True
    if len(SEEN_MIDS) > MAX_SEEN:
        SEEN_MIDS.clear()
    SEEN_MIDS.add(mid)
    return False

# ====== Utils gửi tin ======
def send_text(psid: str, text: str):
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
    if not all([GH_OWNER, GH_REPO, GH_BRANCH, GH_FILE_PATH, GH_TOKEN]):
        raise RuntimeError("Missing GitHub env vars for write: GH_*")
    url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{GH_FILE_PATH}"
    params = {"ref": GH_BRANCH}
    r = requests.get(url, headers=_gh_headers(), params=params, timeout=20)
    if r.status_code == 404:
        return {"sha": None, "content": ""}
    r.raise_for_status()
    data = r.json()
    content_b64 = data.get("content", "")
    sha = data.get("sha")
    content = base64.b64decode(content_b64).decode("utf-8") if content_b64 else ""
    return {"sha": sha, "content": content}

def gh_put_contents(new_text: str, sha: Optional[str], message: str):
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
    text = ""
    if all([GH_OWNER, GH_REPO, GH_BRANCH, GH_FILE_PATH, GH_TOKEN]):
        try:
            obj = gh_get_contents()
            text = obj.get("content", "")
        except Exception as e:
            log.exception(f"Failed gh_get_contents: {e}")
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
        fixed = {k: (row.get(k) or "").strip() for k in CSV_FIELDS}
        rows.append(fixed)
    return rows

def save_psids_csv(rows: List[Dict[str, str]], commit_msg: str):
    if not all([GH_OWNER, GH_REPO, GH_BRANCH, GH_FILE_PATH, GH_TOKEN]):
        log.warning("Skip save_psids_csv: missing GH_* envs")
        return
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: r.get(k, "") for k in CSV_FIELDS})
    new_text = out.getvalue()
    meta = gh_get_contents()
    sha = meta.get("sha")
    gh_put_contents(new_text, sha, commit_msg)

def ensure_psid_row(psid: str, user_facebook: Optional[str] = None):
    rows = load_psids_csv()
    for r in rows:
        if r.get("psid") == psid:
            if user_facebook and not (r.get("user_facebook") or "").strip():
                r["user_facebook"] = user_facebook
                save_psids_csv(rows, commit_msg=f"fill user_facebook for {psid}")
            return
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

# ====== Chuẩn hoá & so khớp tên theo từng ký tự (LCS) ======
def strip_accents(s: str) -> str:
    if not s:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def norm_name(s: str) -> str:
    """Lower, bỏ dấu, chỉ giữ a-z0-9 + space, gọn khoảng trắng."""
    if not s:
        return ""
    s = strip_accents(s.lower())
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def lcs_length(a: str, b: str) -> int:
    n, m = len(a), len(b)
    if n == 0 or m == 0:
        return 0
    dp = [0] * (m + 1)
    for i in range(1, n + 1):
        prev = 0
        ai = a[i - 1]
        for j in range(1, m + 1):
            tmp = dp[j]
            if ai == b[j - 1]:
                dp[j] = prev + 1
            else:
                dp[j] = dp[j] if dp[j] >= dp[j - 1] else dp[j - 1]
            prev = tmp
    return dp[m]

def char_similarity(a: str, b: str) -> float:
    """Similarity = LCS( bỏ space ) / max(len)."""
    na = norm_name(a).replace(" ", "")
    nb = norm_name(b).replace(" ", "")
    if not na or not nb:
        return 0.0
    l = lcs_length(na, nb)
    return l / max(len(na), len(nb))

def names_match(a: str, b: str, threshold: float = 0.67) -> bool:
    return char_similarity(a, b) >= threshold

# ====== Ngày tháng VN hiện tại ======
def parse_ddmmyyyy_from_text(t: Optional[str]) -> Optional[Tuple[int, int, int]]:
    if not t:
        return None
    m = re.search(r'(\b\d{1,2})/(\d{1,2})/(\d{4}\b)', t)
    if not m:
        return None
    try:
        d = int(m.group(1))
        mm = int(m.group(2))
        y = int(m.group(3))
        dt.date(y, mm, d)
        return (d, mm, y)
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
    log.info(f"RAW BODY: {raw[:1000]}")

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
                        "Chào bạn! Gửi ảnh MoMo, mình sẽ đọc số tiền / thời gian / người thực hiện / chi tiết.\n"
                        "Nếu tên trùng với 'user_momo' trong CSV (≈≥67%), số tiền = 120.000đ và ngày thuộc tháng hiện tại,"
                        " mình sẽ dừng nhắc tới đầu tháng sau."
                    )
                except Exception as e:
                    log.exception(f"Send GET_STARTED failed: {e}")
                continue

            message = evt.get("message") or {}
            mid = message.get("mid")
            atts = message.get("attachments") or []

            # chống gửi lặp
            if mid and seen_mid(mid):
                log.info(f"Skip duplicate mid={mid}")
                continue

            # nếu không có ảnh: lưu text làm user_facebook (nếu hợp lệ)
            if not atts:
                text = (message.get("text") or "").strip()
                if text:
                    ensure_psid_row(psid, user_facebook=text if len(text) >= 3 else None)
                continue

            # lấy ảnh đầu tiên
            img_url = None
            for a in atts:
                if (a.get("type") == "image") and ((a.get("payload") or {}).get("url")):
                    img_url = a["payload"]["url"]
                    break
            if not img_url:
                continue

            log.info(f"OCR image_url: {img_url}")

            # gọi OCR nhanh
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

            # bóc dd/mm/yyyy
            dmy = parse_ddmmyyyy_from_text(when_txt)
            month_year_str = "-"
            cond_date = False
            if dmy:
                d_, m_, y_ = dmy
                month_year_str = f"{d_:02d}/{m_:02d}/{y_:04d}"
                cond_date = is_current_month_vn(d_, m_, y_)

            # lấy dòng CSV của PSID, nếu thiếu thì tạo
            rows2 = load_psids_csv()
            target_idx = None
            for i, r in enumerate(rows2):
                if r.get("psid") == psid:
                    target_idx = i
                    break
            if target_idx is None:
                ensure_psid_row(psid)
                rows2 = load_psids_csv()
                for i, r in enumerate(rows2):
                    if r.get("psid") == psid:
                        target_idx = i
                        break

            csv_fb_now = rows2[target_idx].get("user_facebook", "").strip() if target_idx is not None else ""
            csv_momo_now = rows2[target_idx].get("user_momo", "").strip() if target_idx is not None else ""

            # nếu OCR có actor và CSV trống user_facebook -> lưu tham khảo (không dùng auto-mute)
            if target_idx is not None and actor != "-" and not csv_fb_now:
                rows2[target_idx]["user_facebook"] = actor
                try:
                    save_psids_csv(rows2, commit_msg=f"fill user_facebook for {psid} from OCR")
                except Exception as e:
                    log.exception(f"save fb name failed: {e}")

            # điều kiện
            cond_amount = (amount_val == 120000)
            cond_name   = bool(csv_momo_now and actor != "-" and names_match(csv_momo_now, actor))

            # similarity để in %
            name_sim = None
            if csv_momo_now and actor != "-":
                try:
                    name_sim = int(round(char_similarity(csv_momo_now, actor) * 100))
                except Exception:
                    name_sim = None

            amount_ok = cond_amount
            date_ok   = cond_date
            name_ok   = cond_name

            mute_ok = False
            if amount_ok and date_ok and name_ok:
                now_vn = dt.datetime.now(dt.timezone.utc).astimezone(VN_TZ)
                yyyymm = f"{now_vn.year:04d}-{now_vn.month:02d}"
                try:
                    update_last_paid_month(psid, yyyymm)
                    mute_ok = True
                except Exception as e:
                    log.exception(f"update_last_paid_month failed: {e}")

            # trả kết quả + checklist + debug
            reply = (
                "✅ KẾT QUẢ (MoMo)\n"
                f"• Số tiền: {amt_text}\n"
                f"• Thời gian: {when_txt}\n"
                f"• Người thực hiện: {actor}\n"
                f"• Chi tiết: {detail}\n"
                f"(OCR ~{spent:.2f}s)\n\n"
                "📋 Checklist (đối chiếu với user_momo trong CSV):\n"
                f"• {ok(target_idx is not None)} Có dòng CSV cho PSID\n"
                f"• {ok(bool(csv_momo_now))} Có user_momo trong CSV (MoMo='{csv_momo_now or '-'}')\n"
                f"• {ok(name_ok)} Tên khớp user_momo ↔ OCR: '{csv_momo_now or '-'}' ~ '{actor}'"
                + (f" (similarity≈{name_sim}%)" if name_sim is not None else "") + "\n"
                f"• {ok(amount_ok)} Số tiền = 120.000đ (OCR: {amt_text})\n"
                f"• {ok(date_ok)} Ngày thuộc tháng hiện tại (OCR: {month_year_str})\n"
                f"• {ok(mute_ok)} Đã đặt tắt nhắc tới đầu tháng sau"
            )

            if lines:
                preview = lines[:30]
                body = "\n".join(preview)
                reply += f"\n\n[DEBUG] OCR lines ({len(lines)}):\n{body}"

            try:
                send_text(psid, reply)
            except Exception as e:
                log.exception(f"send reply failed: {e}")

    return "ok", 200

# 3) Cron gửi nhắc hằng tuần (tôn trọng last_paid_month)
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)
    if not PAGE_TOKEN:
        return jsonify({"error": "PAGE_TOKEN is missing"}), 500

    psids_param = request.args.get("psids")
    custom_msg  = request.args.get("msg")

    targets: List[str] = []
    mode = "CSV"
    rows = load_psids_csv()
    now_vn = dt.datetime.now(dt.timezone.utc).astimezone(VN_TZ)
    current_yyyymm = f"{now_vn.year:04d}-{now_vn.month:02d}"

    if psids_param:
        mode = "psids"
        targets = [p.strip() for p in psids_param.split(",") if p.strip()]
    elif rows:
        for r in rows:
            psid = (r.get("psid") or "").strip()
            if not psid:
                continue
            if (r.get("last_paid_month") or "") == current_yyyymm:
                continue
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
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
