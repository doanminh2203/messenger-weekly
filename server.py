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

# -------- OCR nhanh (yêu cầu file ocr_fast.py trong repo) ----------
try:
    # trả về dict: {amount_text, date_text, actor_name, detail_text, lines, spent_sec}
    from ocr_fast import fast_extract_amount_date
except Exception as e:
    fast_extract_amount_date = None  # sẽ báo lỗi khi gọi
    _OCR_IMPORT_ERR = e

# ================= ENV =================
load_dotenv()

PAGE_TOKEN      = os.getenv("PAGE_TOKEN")
VERIFY_TOKEN    = os.getenv("VERIFY_TOKEN", "changeme")
CRON_SECRET     = os.getenv("CRON_SECRET", "secret")

# CSV: đọc (raw URL) và ghi (GitHub API)
PSIDS_CSV_URL   = os.getenv("PSIDS_CSV_URL", "")  # vd: https://raw.githubusercontent.com/<owner>/<repo>/main/psids.csv
GH_OWNER        = os.getenv("GH_OWNER", "")
GH_REPO         = os.getenv("GH_REPO", "")
GH_BRANCH       = os.getenv("GH_BRANCH", "main")
GH_FILE_PATH    = os.getenv("GH_FILE_PATH", "psids.csv")
GH_TOKEN        = os.getenv("GH_TOKEN", "")

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
CSV_HEADERS = ["psid", "name", "mute_until", "created_at_iso"]

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

# (tuỳ chọn) lấy tên hiển thị FB (first/last) nếu cần tự động điền name khi CSV trống
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
SLASH_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")  # chỉ dd/mm/yyyy

def extract_strict_slash_date_from_text(text: str) -> Optional[Tuple[str, Tuple[int, int]]]:
    """
    Trả về (date_text, (month, year)) nếu tìm thấy dd/mm/yyyy với dấu '/'; ngược lại None.
    """
    if not text:
        return None
    m = SLASH_DATE_RE.search(text)
    if not m:
        return None
    d, mth, y = m.groups()
    month = int(mth)
    year = int(y)
    # kiểm tra hợp lệ sơ bộ
    if not (1 <= month <= 12 and 2000 <= year <= 2100):
        return None
    return (m.group(0), (month, year))

def extract_momo_date(lines: List[str], fallback_text: str) -> Tuple[str, Optional[Tuple[int,int]]]:
    """
    Quét toàn bộ lines OCR để tìm dd/mm/yyyy (slash). 
    Ưu tiên dòng chứa cả 'Thoi gian'/'Thời gian'; nếu không có thì tìm bất kỳ.
    Nếu vẫn không thấy, thử lấy từ fallback_text (ví dụ '22:25-22/09/2025').
    """
    # ưu tiên dòng chứa từ khóa
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
    # fallback: text tổng hợp (ví dụ '22:25-22/09/2025' → vẫn sẽ match '22/09/2025')
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
            "name": r.get("name", "").strip(),
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

def upsert_row_by_psid(rows: List[Dict[str, str]], psid: str, name: str) -> List[Dict[str, str]]:
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

# ================= Dedup message =================
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

            mid = ((evt.get("message") or {}).get("mid")) or ((evt.get("postback") or {}).get("mid"))
            if mid and seen_mid(mid):
                app.logger.info("Skip duplicate mid=%s", mid)
                continue

            # GET_STARTED
            if evt.get("postback", {}).get("payload") == "GET_STARTED":
                send_text(psid,
                    "Chào bạn! Gửi ảnh biên lai MoMo để hệ thống kiểm tra và tự dừng nhắc khi đã đóng 120.000đ trong tháng.")
                # đảm bảo có dòng CSV
                rows = load_psids_csv()
                rows = upsert_row_by_psid(rows, psid, name="")
                # thử điền tên FB nếu trống
                idx = next((i for i, r in enumerate(rows) if r.get("psid") == psid), None)
                if idx is not None and not (rows[idx].get("name") or "").strip():
                    prof = get_user_profile(psid)
                    disp = get_display_name(prof)
                    if disp:
                        rows[idx]["name"] = disp
                        if not rows[idx].get("created_at_iso"):
                            rows[idx]["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                        save_psids_csv(rows, commit_msg=f"set fb name for {psid} -> {disp}")
                continue

            msg = evt.get("message") or {}
            text_in = (msg.get("text") or "").strip()

            # Bảo đảm có dòng CSV cho PSID
            rows = load_psids_csv()
            rows = upsert_row_by_psid(rows, psid, name="")
            # nếu chưa có name → thử lấy từ FB
            idx = next((i for i, r in enumerate(rows) if r.get("psid") == psid), None)
            if idx is not None and not (rows[idx].get("name") or "").strip():
                prof = get_user_profile(psid)
                disp = get_display_name(prof)
                if disp:
                    rows[idx]["name"] = disp
                    if not rows[idx].get("created_at_iso"):
                        rows[idx]["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                    save_psids_csv(rows, commit_msg=f"set fb name for {psid} -> {disp}")
                else:
                    # vẫn lưu upsert nếu chưa lưu
                    save_psids_csv(rows, commit_msg="upsert psid on message")

            # Xử lý ảnh
            atts: List[Dict] = msg.get("attachments") or []
            for att in atts:
                if att.get("type") != "image":
                    continue
                image_url = (att.get("payload") or {}).get("url")
                if not image_url:
                    continue

                if fast_extract_amount_date is None:
                    app.logger.error("OCR module not available: %s", _OCR_IMPORT_ERR)
                    send_text(psid, "❌ OCR chưa sẵn sàng trên server.")
                    continue

                app.logger.info("OCR image_url: %s", image_url)
                try:
                    result = fast_extract_amount_date(image_url)
                    lines    = result.get("lines", []) or []
                    amt_text = result.get("amount_text") or "-"
                    when_txt = result.get("date_text") or "-"
                    actor    = result.get("actor_name") or "-"
                    detail   = result.get("detail_text") or "-"
                    spent    = result.get("spent_sec", 0.0)

                    # log lines
                    app.logger.info("=== OCR LINES (%d) ===\n%s\n=== END OCR LINES ===",
                                    len(lines), "\n".join(lines))

                    # nếu CSV name trống & OCR có actor -> set ngay
                    try:
                        if actor and actor != "-":
                            rows_cur = load_psids_csv()
                            idx_psid = next((i for i, r in enumerate(rows_cur) if r.get("psid") == psid), None)
                            if idx_psid is not None and not (rows_cur[idx_psid].get("name") or "").strip():
                                rows_cur[idx_psid]["name"] = actor
                                if not rows_cur[idx_psid].get("created_at_iso"):
                                    rows_cur[idx_psid]["created_at_iso"] = dt.datetime.now(VN_TZ).isoformat(timespec="seconds")
                                ok = save_psids_csv(rows_cur, commit_msg=f"set name for {psid} -> {actor}")
                                app.logger.info("Set name from OCR for %s -> %s (saved=%s)", psid, actor, ok)
                    except Exception as e:
                        app.logger.exception("Set-name-after-OCR failed: %s", e)

                    # ---- dùng date strict dd/mm/yyyy (slash) ----
                    date_text_strict, month_year = extract_momo_date(lines, when_txt)
                    when_txt_display = when_txt or date_text_strict or "-"

                    # điều kiện auto-mute
                    amount_val = parse_amount_to_int(amt_text)
                    did_mute = False
                    mute_until_str = ""

                    rows2 = load_psids_csv()
                    csv_name = ""
                    # ưu tiên đúng PSID có name
                    target_idx = None
                    for idx2, r in enumerate(rows2):
                        if r.get("psid") == psid and (r.get("name") or "").strip():
                            csv_name = r.get("name").strip()
                            if names_match(csv_name, actor):
                                target_idx = idx2
                                break
                    # nếu PSID chưa có name → thử match theo tên trong CSV
                    if target_idx is None:
                        for idx2, r in enumerate(rows2):
                            nm = (r.get("name") or "").strip()
                            if nm and names_match(nm, actor):
                                csv_name = nm
                                target_idx = idx2
                                break
                    # nếu vẫn None → gán name vào dòng của PSID hiện tại
                    if target_idx is None:
                        for idx2, r in enumerate(rows2):
                            if r.get("psid") == psid:
                                rows2[idx2]["name"] = actor
                                csv_name = actor
                                target_idx = idx2
                                break

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
                    # lấy lại name hiện tại trong CSV cho checklist
                    rows_csv = load_psids_csv()
                    row_ok = False
                    csv_name_now = ""
                    for r in rows_csv:
                        if r.get("psid") == psid:
                            row_ok = True
                            csv_name_now = (r.get("name") or "").strip()
                            break

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
                        f"(OCR ~{spent}s)\n\n"
                        "📋 Checklist:\n"
                        + "\n".join("• " + line for line in [
                            f"{ok(row_ok)} Có dòng CSV cho PSID",
                            f"{ok(bool(csv_name_now))} Có tên trong CSV: {(csv_name_now or '-')}",
                            f"{ok(name_ok)} Tên khớp CSV↔OCR: CSV='{csv_name_now or '-'}' ~ OCR='{actor}'",
                            f"{ok(amount_ok)} Số tiền = 120.000đ (OCR: {amt_text})",
                            f"{ok(date_ok)} Ngày thuộc tháng hiện tại (VN) (OCR: {when_txt_display} ~ {month_year_str})",
                            f"{ok(mute_ok)} Đã đặt tắt nhắc (mute) tới đầu tháng sau",
                        ])
                    )
                    if not mute_ok:
                        reply += "\n\nℹ️ Chưa đủ điều kiện dừng nhắc (cần đúng tên, 120.000đ, và tháng hiện tại)."
                    else:
                        reply += f"\n\n🔕 Đã dừng nhắc đến {mute_until_str}."

                    # debug lines (rút gọn)
                    preview_lines = "\n".join(lines[:20])
                    if len(preview_lines) > 1200:
                        preview_lines = preview_lines[:1200] + "…"
                    reply += f"\n\n[DEBUG] OCR lines ({len(lines)}):\n{preview_lines}"

                    send_text(psid, reply)

                except Exception as e:
                    app.logger.exception("OCR failed: %s", e)
                    send_text(psid, "❌ Xin lỗi, không đọc được ảnh này. Bạn thử chụp rõ hơn/đủ sáng nhé.")

            # Text “DỪNG” để mute tới đầu tháng sau
            t = (text_in or "").strip().lower()
            if t in {"dung", "dừng", "stop"}:
                rows = load_psids_csv()
                rows = upsert_row_by_psid(rows, psid, name="")
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
