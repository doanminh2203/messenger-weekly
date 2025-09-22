# server.py
import os
import json
import time
import base64
import logging
import datetime as dt
from io import StringIO
import csv
from typing import List, Dict, Optional

import requests
from flask import Flask, request, abort, jsonify
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# === OCR nhanh (Amount/Date ≤30s) ===
from ocr_fast import fast_extract_amount_date

# ================== ENV ==================
load_dotenv()  # local dùng .env; Render dùng Env Vars

PAGE_TOKEN      = os.getenv("PAGE_TOKEN")                  # EAA...
VERIFY_TOKEN    = os.getenv("VERIFY_TOKEN", "changeme")
CRON_SECRET     = os.getenv("CRON_SECRET", "secret")
TEST_PSIDS      = [p.strip() for p in os.getenv("TEST_PSIDS", "").split(",") if p.strip()]
PSIDS_CSV_URL   = os.getenv("PSIDS_CSV_URL", "")           # raw URL tới psids.csv (đọc)
# Ghi CSV qua GitHub API:
GH_OWNER        = os.getenv("GH_OWNER", "")
GH_REPO         = os.getenv("GH_REPO", "")
GH_BRANCH       = os.getenv("GH_BRANCH", "main")
GH_FILE_PATH    = os.getenv("GH_FILE_PATH", "psids.csv")
GH_TOKEN        = os.getenv("GH_TOKEN", "")                # token có quyền Contents: Read & write

# ================ APP & LOG ================
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ================ HELPERS: Facebook ================
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
    if r.status_code >= 400:
        app.logger.error("Send API error %s: %s", r.status_code, r.text)
    r.raise_for_status()

# ================ HELPERS: CSV đọc (public raw) ================
def load_psids_from_csv_public() -> List[str]:
    """Đọc PSID từ PSIDS_CSV_URL (raw Github). Bỏ dòng header/blank."""
    targets: List[str] = []
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
        app.logger.exception(f"Failed to load CSV (read): {e}")
    return targets

# ================ HELPERS: GitHub Contents API (đọc/ghi CSV) ================
def _gh_headers() -> Dict[str, str]:
    if not GH_TOKEN:
        raise RuntimeError("GH_TOKEN is missing")
    return {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json",
    }

def gh_get_file_info(owner: str, repo: str, path: str, ref: str) -> Dict:
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    r = requests.get(url, headers=_gh_headers(), params={"ref": ref}, timeout=15)
    if r.status_code == 404:
        return {"not_found": True}
    r.raise_for_status()
    return r.json()

def gh_put_file(owner: str, repo: str, path: str, content_b64: str, message: str, sha: Optional[str], branch: str):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    body = {"message": message, "content": content_b64, "branch": branch}
    if sha:
        body["sha"] = sha
    r = requests.put(url, headers=_gh_headers(), json=body, timeout=20)
    if r.status_code >= 400:
        app.logger.error("GitHub PUT error %s: %s", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def gh_read_csv_targets(owner: str, repo: str, path: str, ref: str) -> Dict[str, Dict]:
    """
    Trả: dict[psid] = {"psid":..., "created_at_iso":...}
    Nếu file chưa tồn tại -> trả dict rỗng.
    """
    info = gh_get_file_info(owner, repo, path, ref)
    if info.get("not_found"):
        return {}
    if info.get("encoding") == "base64" and "content" in info:
        raw = base64.b64decode(info["content"]).decode("utf-8", errors="ignore")
    else:
        # fallback: tải qua download_url
        dl = info.get("download_url")
        if not dl:
            return {}
        r = requests.get(dl, timeout=12)
        r.raise_for_status()
        raw = r.text

    out: Dict[str, Dict] = {}
    try:
        f = StringIO(raw)
        reader = csv.DictReader(f)
        for row in reader:
            psid = (row.get("psid") or "").strip()
            created = (row.get("created_at_iso") or "").strip()
            if psid:
                out[psid] = {"psid": psid, "created_at_iso": created}
    except Exception as e:
        app.logger.exception(f"Parse CSV error: {e}")
    # append info.sha để update
    if "sha" in info:
        out["_sha"] = info["sha"]
    return out

def gh_upsert_psid(psid: str) -> bool:
    """Thêm PSID vào CSV nếu chưa có. Tạo file nếu chưa tồn tại."""
    if not (GH_OWNER and GH_REPO and GH_FILE_PATH and GH_BRANCH and GH_TOKEN):
        app.logger.info("GH_* env missing; skip writing PSID to GitHub.")
        return False

    # đọc hiện trạng
    exists = gh_read_csv_targets(GH_OWNER, GH_REPO, GH_FILE_PATH, GH_BRANCH)
    sha = exists.get("_sha")
    targets = {k: v for k, v in exists.items() if not k.startswith("_")}

    if psid in targets:
        app.logger.info(f"PSID {psid} already in CSV")
        return True

    # build CSV mới
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    targets[psid] = {"psid": psid, "created_at_iso": now_iso}

    # luôn có header
    rows = sorted(targets.values(), key=lambda x: x["created_at_iso"])
    out_io = StringIO()
    writer = csv.DictWriter(out_io, fieldnames=["psid", "created_at_iso"])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    new_b64 = base64.b64encode(out_io.getvalue().encode("utf-8")).decode("ascii")

    # ghi lại
    msg = f"chore(csv): upsert psid {psid}"
    gh_put_file(GH_OWNER, GH_REPO, GH_FILE_PATH, new_b64, msg, sha, GH_BRANCH)
    app.logger.info(f"Appended PSID {psid} to {GH_FILE_PATH}")
    return True

# ================ MISC ================
def extract_ref(evt: dict) -> Optional[str]:
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

# 2) Receive events
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

            # Lưu PSID ngay khi có bất kỳ tương tác
            try:
                gh_upsert_psid(psid)
            except Exception as e:
                app.logger.exception(f"upsert psid failed: {e}")

            # GET_STARTED postback
            postback = (evt.get("postback") or {})
            payload = (postback.get("payload") or "")
            if psid and payload == "GET_STARTED":
                try:
                    send_text(psid,
                        "Chào bạn! Bạn đã bắt đầu trò chuyện với 108Lab.\n"
                        "Bạn sẽ nhận nhắc hằng tuần khi được bật. Nhắn 'DỪNG' để hủy bất cứ lúc nào."
                    )
                except Exception as e:
                    app.logger.exception(f"Reply failed: {e}")

            # Message text: cho phép user dừng
            msg = evt.get("message") or {}
            text = (msg.get("text") or "").strip()
            if psid and text:
                if text.upper() in {"DUNG", "DỪNG", "STOP"}:
                    # (tuỳ chọn) bạn có thể xoá PSID khỏi CSV nếu muốn
                    try:
                        send_text(psid, "Đã ghi nhận dừng nhắc. Cảm ơn bạn!")
                    except Exception:
                        pass

            # (Tuỳ chọn) biết người vào từ ref nào
            ref = extract_ref(evt)
            if psid and ref:
                app.logger.info(f"REF '{ref}' from {psid}")

            # Attachments: nếu là ảnh → OCR nhanh (Amount/Date)
            attachments = msg.get("attachments") or []
            for att in attachments:
                if (att.get("type") or "").lower() == "image":
                    payload = att.get("payload") or {}
                    image_url = payload.get("url")
                    if not image_url:
                        continue
                    app.logger.info(f"OCRFAST image_url: {image_url}")
                    try:
                        out = fast_extract_amount_date(image_url)
                        amt_txt = out.get("amount_text")
                        amt_val = out.get("amount")
                        when    = out.get("date_text")
                        avgc    = out.get("avg_conf")

                        # format số tiền
                        if isinstance(amt_val, int):
                            amt_show = f"{amt_val:,} VND".replace(",", ".")
                        else:
                            amt_show = amt_txt or "-"

                        note = ""
                        if out.get("timeout"):
                            note = "\n[Note] Xử lý lâu, đã trả nhanh theo kết quả tạm."
                        if out.get("note"):
                            note += f"\n[Note] {out.get('note')}"

                        confirm = (
                            "✅ KẾT QUẢ NHANH\n"
                            f"• Số tiền: {amt_show}\n"
                            f"• Ngày/giờ: {when or '-'}\n"
                        )
                        if avgc is not None:
                            confirm += f"• Độ tin cậy trung bình: {avgc:.2f}\n"
                        confirm += note

                        if not amt_txt and not when:
                            confirm += "\n⚠️ Chưa nhận diện được Số tiền/Ngày. Vui lòng gửi ảnh rõ hơn."

                        send_text(psid, confirm)

                    except Exception as e:
                        app.logger.exception(f"OCRFAST failed: {e}")
                        try:
                            send_text(psid, "⚠️ Xin lỗi, chưa đọc được ảnh (chế độ nhanh). Vui lòng gửi ảnh rõ nét hơn.")
                        except Exception:
                            pass

    return "ok", 200

# 3) Cron endpoint – gửi cho 1 nhóm duy nhất (CSV public hoặc TEST_PSIDS); hỗ trợ psids=&msg=
@app.post("/task/weekly")
def task_weekly():
    if request.headers.get("X-CRON-SECRET") != CRON_SECRET:
        abort(403)
    if not PAGE_TOKEN:
        return jsonify({"error": "PAGE_TOKEN is missing"}), 500

    # Ưu tiên psids= (ad-hoc), sau đó CSV public; cuối cùng fallback TEST_PSIDS
    psids_param = request.args.get("psids")  # "111,222"
    custom = request.args.get("msg")         # nội dung tùy chọn

    if psids_param:
        targets = [p.strip() for p in psids_param.split(",") if p.strip()]
        mode = "psids"
    else:
        csv_psids = load_psids_from_csv_public()
        if csv_psids:
            targets = csv_psids
            mode = "CSV_PUBLIC"
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
            time.sleep(0.2)  # giãn nhẹ
        except Exception as e:
            app.logger.exception(f"Send failed for {p}: {e}")

    # Timestamp UTC + VN
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

# 4) Endpoint debug OCR nhanh
@app.get("/debug/ocr")
def debug_ocr():
    url = request.args.get("url")
    if not url:
        return jsonify({"error": "missing url"}), 400
    try:
        out = fast_extract_amount_date(url)
        # rút gọn danh sách dòng để JSON gọn
        if "lines" in out and isinstance(out["lines"], list):
            out["lines"] = out["lines"][:60]
        return jsonify(out)
    except Exception as e:
        app.logger.exception(f"/debug/ocr failed: {e}")
        return jsonify({"error": str(e)}), 500

# ================ MAIN ================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
