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

# ---- OCR nhanh (yêu cầu ocr_fast.py trong repo) ----
try:
    # phải trả về dict: {amount_text, date_text, actor_name, detail_text, lines, spent_sec}
    from ocr_fast import fast_extract_amount_date
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
CSV_HEADERS = ["psid", "user_facebook", "user_momo", "mute_until", "created_at_iso"]

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

def extract_momo_date(lines: List[str], fallback_text: str) -> Tuple[str, Optional[Tuple[int,int]]]:
    # ưu tiên dòng chứa "Thời gian/Thoi gian"
    for ln in lines or []:
        if re.search(r"\b(thoi\s*gia[mn]|thời\s*gia[mn])\b", ln, flags=re.I):
            hit = extract_strict_slash_date_from_text(ln)
            if hit:
                return hit[0], hit[1]
    # quét tất cả dòng
    for ln in lines or []:
        hit = extract_strict_slash_date_from_text(ln)
        if hit:
            return hit[0], hit[1]
    # fallback: text tổng hợp
    hit = extract_strict_slash_date_from_text(fallback_text or "")
    if hit:
        return hit[0], hit[1]
    return "-", None

def _now_vn_date() -> dt.date:
    return dt.datetime.now(VN_TZ).date()

def _first_day_next_month_vn() -> dt.date:
    today = _now_vn_date()
    year = today.year + (1 if today.month == 12 else 0)
    month = 1 if today.month == 12 else today.month + 1
    return dt.date(year, month, 1)

def is_current_month_vn(month: int, year: int) -> bool:
    today = _now_vn_date()
    return (month == today.month) and (year == today.year)

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
            row = {h: (r.get(h, "") or "").strip() for h in CSV_HEADERS}
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
            break
    if not found:
        rows.append({
            "psid": psid,
            "user_facebook": fb_name or "",
            "user_momo": "",
            "mute_until": "",
            "created_at_iso": dt.datetime.now(VN_TZ).isoformat(timespec="seconds"),
        })
    return rows

# ================= Name/Amount matching =================
import unicodedata
from rapidfuzz.fuzz import partial_ratio, ratio

def _strip_accents(s: str) -> str:
    s_norm = unicodedata.normalize("NFD", s or "")
    s_no = "".join(ch for ch in s_norm if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFC", s_no)

def norm_name(s: str) -> str:
    s2 = _strip_accents(s).lower().strip()
    s2 = re.sub(r"\s+", " ", s2)
    return s2

def names_match(a: str, b: str) -> bool:
    na, nb = norm_name(a), norm_name(b)
    if not na or not nb:
        return False
    if na == nb or na in nb or nb in na:
        return True
    return partial_ratio(na, nb) >= 90 or ratio(na, nb) >= 85

def parse_amount_to_int(amount_text: Optional[str]) -> Optional[int]:
    if not amount_text:
        return None
    digits = re.sub(r"[^\d]", "", amount_text)
    return int(digits) if digits.isdigit() else None

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

            # GET_STARTED
            if evt.get("postback", {}).get("payload") == "GET_STARTED":
                send_text(psid,
                    "Chào bạn! Gửi ảnh biên lai MoMo để hệ thống kiểm tra và tự dừng nhắc khi đã đóng 120.000đ trong tháng.")
                rows = load_psids_csv()
                rows = upsert_row_by_psid(rows, psid, fb_name="")
                idx = next((i for i, r in enumerate(rows) if r.get("psid") == psid), None)
                if idx is not None and not (rows[idx].get("user_facebook") or "").strip():
                    prof = get_user_profile(psid)
                    disp = get_display_name(prof)
                    if disp:
                        rows[idx]["user_facebook"] = disp
                        if not rows[idx].get("created_at_iso"):
                            rows[idx]["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                        save_psids_csv(rows, commit_msg=f"set fb name for {psid} -> {disp}")
                else:
                    save_psids_csv(rows, commit_msg="upsert psid on get_started")
                continue

            msg = evt.get("message") or {}
            text_in = (msg.get("text") or "").strip()

            # bảo đảm có dòng CSV cho PSID
            rows = load_psids_csv()
            rows = upsert_row_by_psid(rows, psid, fb_name="")
            idx = next((i for i, r in enumerate(rows) if r.get("psid") == psid), None)
            if idx is not None and not (rows[idx].get("user_facebook") or "").strip():
                prof = get_user_profile(psid)
                disp = get_display_name(prof)
                if disp:
                    rows[idx]["user_facebook"] = disp
                    if not rows[idx].get("created_at_iso"):
                        rows[idx]["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                    save_psids_csv(rows, commit_msg=f"set fb name for {psid} -> {disp}")
                else:
                    save_psids_csv(rows, commit_msg="upsert psid on message")

            # Xử lý ảnh: CHỈ 1 ảnh đầu tiên, và chống trùng theo URL
            atts: List[Dict] = msg.get("attachments") or []
            processed_one = False
            for att in atts:
                if att.get("type") != "image":
                    continue
                image_url = (att.get("payload") or {}).get("url")
                if not image_url:
                    continue

                # nếu lần đầu thấy mid thì đánh dấu mid; nếu không có mid thì chống trùng theo url
                if mid:
                    # mid đã được seen_mid ở trên (để skip sớm). ở đây chỉ đánh dấu lần nữa để chắc chắn
                    _processed_mids[mid] = time.time()
                else:
                    if seen_image(image_url):
                        app.logger.info("Skip duplicate by image_url=%s", image_url)
                        continue

                # Đã xử lý 1 ảnh rồi thì dừng (tránh gửi nhiều lần nếu có nhiều image trong cùng message)
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

                    app.logger.info("=== OCR LINES (%d) ===\n%s\n=== END OCR LINES ===",
                                    len(lines), "\n".join(lines))

                    # nếu CSV user_facebook trống & OCR có actor -> set ngay
                    try:
                        if actor and actor != "-":
                            rows_cur = load_psids_csv()
                            idx_psid = next((i for i, r in enumerate(rows_cur) if r.get("psid") == psid), None)
                            if idx_psid is not None and not (rows_cur[idx_psid].get("user_facebook") or "").strip():
                                rows_cur[idx_psid]["user_facebook"] = actor
                                if not rows_cur[idx_psid].get("created_at_iso"):
                                    rows_cur[idx_psid]["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                                ok = save_psids_csv(rows_cur, commit_msg=f"set user_facebook for {psid} -> {actor}")
                                app.logger.info("Set user_facebook from OCR for %s -> %s (saved=%s)", psid, actor, ok)
                    except Exception as e:
                        app.logger.exception("Set-name-after-OCR failed: %s", e)

                    # ---- ngày MoMo dd/mm/yyyy (slash only) ----
                    def extract_strict_date_anywhere(lines_local: List[str], fallback: str):
                        # ưu tiên dòng chứa từ khóa thời gian
                        for ln in lines_local or []:
                            if re.search(r"\b(thoi\s*gia[mn]|thời\s*gia[mn])\b", ln, flags=re.I):
                                hit = extract_strict_slash_date_from_text(ln)
                                if hit:
                                    return hit
                        # sau đó thử toàn bộ
                        for ln in lines_local or []:
                            hit = extract_strict_slash_date_from_text(ln)
                            if hit:
                                return hit
                        # fallback
                        return extract_strict_slash_date_from_text(fallback or "")

                    hit = extract_strict_date_anywhere(lines, when_txt)
                    if hit:
                        date_text_strict, month_year = hit[0], hit[1]
                    else:
                        date_text_strict, month_year = "-", None

                    when_txt_display = when_txt or date_text_strict or "-"

                    # điều kiện auto-mute
                    amount_val = parse_amount_to_int(amt_text)
                    did_mute = False
                    mute_until_str = ""

                    rows2 = load_psids_csv()

                    # Tìm dòng ứng viên theo PSID
                    target_idx = next((i for i, r in enumerate(rows2) if r.get("psid") == psid), None)

                    # Lấy tên tham chiếu để so khớp: ưu tiên user_momo, fallback user_facebook
                    csv_name = ""
                    if target_idx is not None:
                        row = rows2[target_idx]
                        csv_name = (row.get("user_momo") or "").strip() or (row.get("user_facebook") or "").strip()

                    # Nếu PSID chưa có tên & actor có => set vào user_facebook
                    if target_idx is not None and actor and actor != "-" and not csv_name:
                        rows2[target_idx]["user_facebook"] = actor
                        save_psids_csv(rows2, commit_msg=f"fill fb name for {psid} from OCR")
                        csv_name = actor

                    cond_amount = (amount_val == 120000)
                    cond_date   = bool(month_year and is_current_month_vn(*month_year))
                    cond_name   = bool(csv_name and actor and names_match(csv_name, actor))

                    if target_idx is not None and cond_amount and cond_date and cond_name:
                        next1 = _first_day_next_month_vn()
                        rows2[target_idx]["mute_until"] = next1.isoformat()
                        if not rows2[target_idx].get("created_at_iso"):
                            rows2[target_idx]["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                        if save_psids_csv(rows2, commit_msg=f"auto mute {psid} until {next1.isoformat()}"):
                            did_mute = True
                            mute_until_str = next1.strftime("%d/%m/%Y")

                    # === CHECKLIST ===
                    def ok(v: bool) -> str:
                        return "✅" if v else "❌"

                    month_year_str = f"{month_year[0]:02d}/{month_year[1]}" if month_year else "-"

                    # lấy lại dòng CSV hiện tại cho checklist
                    rows_csv = load_psids_csv()
                    row_ok = False
                    csv_fb_now = ""
                    csv_momo_now = ""
                    for r in rows_csv:
                        if r.get("psid") == psid:
                            row_ok = True
                            csv_fb_now = (r.get("user_facebook") or "").strip()
                            csv_momo_now = (r.get("user_momo") or "").strip()
                            break
                    csv_name_now = csv_momo_now or csv_fb_now
                    try:
                        name_ok = bool(csv_name_now and actor and names_match(csv_name_now, actor))
                    except Exception:
                        name_ok = bool(csv_name_now and actor and (csv_name_now.strip().lower() == actor.strip().lower()))
                    amount_ok = cond_amount
                    date_ok   = cond_date
                    mute_ok   = bool(did_mute)

                    reply = (
                        "✅ KẾT QUẢ (MoMo)\n"
                        f"• Số tiền: {amt_text}\n"
                        f"• Thời gian: {when_txt_display}\n"
                        f"• Người thực hiện: {actor}\n"
                        f"• Chi tiết: {detail}\n"
                    )
                    if not mute_ok:
                        reply += "\n\nℹ️ Chưa đủ điều kiện dừng nhắc (cần đúng tên, 120.000đ, và tháng hiện tại)."
                    else:
                        reply += f"\n\n🔕 Đã dừng nhắc đến {mute_until_str}."

                    # debug lines (giới hạn ~20 dòng)
                    preview_lines = "\n".join(lines[:20])
                    if len(preview_lines) > 1200:
                        preview_lines = preview_lines[:1200] + "…"

                    send_text(psid, reply)
                    processed_one = True
                    # đánh dấu image_url đã xử lý để retry cùng URL không lặp
                    _processed_images[image_url] = time.time()
                    break  # chỉ xử lý ảnh đầu tiên

                except Exception as e:
                    app.logger.exception("OCR failed: %s", e)
                    send_text(psid, "❌ Xin lỗi, không đọc được ảnh này. Bạn thử chụp rõ hơn/đủ sáng nhé.")
                    processed_one = True
                    break

            # Text “DỪNG” để mute tới đầu tháng sau
            t = (text_in or "").strip().lower()
            if t in {"dung", "dừng", "stop"}:
                rows = load_psids_csv()
                rows = upsert_row_by_psid(rows, psid, fb_name="")
                next1 = _first_day_next_month_vn()
                for r in rows:
                    if r.get("psid") == psid:
                        r["mute_until"] = next1.isoformat()
                        if not r.get("created_at_iso"):
                            r["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                        break
                save_psids_csv(rows, commit_msg=f"user requested stop until {next1.isoformat()}")
                send_text(psid, f"Đã dừng nhắc đến {next1.strftime('%d/%m/%Y')}.")

    return "ok", 200

# ================= Cron gửi nhắc tuần =================
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
                if mu >= today:
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

    return jsonify({"sent": sent, "eligible": len(targets), "date_vn": today.isoformat()})

# ================= Main =================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
