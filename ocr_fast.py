# ocr_fast.py
# OCR nhanh cho biên lai MoMo: Người gửi / Người nhận / Số tiền / Ngày-giờ / Mã GD
import logging, time, re
from typing import Dict, Tuple, List, Optional

import requests
import numpy as np, cv2
from io import BytesIO
from PIL import Image
from rapidocr_onnxruntime import RapidOCR

logger = logging.getLogger("server")

HTTP_TIMEOUT = 12
MAX_BYTES    = 12 * 1024 * 1024
MAX_SIDE     = 1280
OCR_HARD_BUDGET = 20

_OCR = None
def _ocr():
    global _OCR
    if _OCR is None:
        _OCR = RapidOCR()
    return _OCR

def _fetch_image_bytes(url: str) -> bytes:
    r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=HTTP_TIMEOUT, stream=True)
    r.raise_for_status()
    buf = BytesIO(); read = 0
    for piece in r.iter_content(64*1024):
        if not piece: break
        read += len(piece)
        if read > MAX_BYTES: raise ValueError("Image too large >MAX_BYTES")
        buf.write(piece)
    data = buf.getvalue()
    if not data: raise ValueError("Empty image bytes")
    logger.info(f"OCRFAST bytes={len(data)} url_final={r.url}")
    return data

def _decode_to_bgr(data: bytes) -> np.ndarray:
    try:
        arr = np.frombuffer(data, np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is not None: return img
    except Exception: pass
    pil = Image.open(BytesIO(data)).convert("RGB")
    return np.array(pil)[:, :, ::-1]  # RGB -> BGR

def _resize_fast(img: np.ndarray) -> np.ndarray:
    h, w = img.shape[:2]; mx = max(h, w)
    if mx <= MAX_SIDE: return img
    s = MAX_SIDE/float(mx)
    return cv2.resize(img, (int(w*s), int(h*s)), interpolation=cv2.INTER_LINEAR)

def _run_rapidocr(img: np.ndarray) -> Tuple[List[str], List[float]]:
    out = _ocr()(img)
    lines, confs = [], []
    # RapidOCR có 2 kiểu output phổ biến
    if isinstance(out, tuple) and len(out) == 2:         # ([ [box,text,score], ...], elapse)
        result, _ = out
        if isinstance(result, list):
            for it in result:
                if isinstance(it, (list, tuple)) and len(it) >= 3:
                    t = (it[1] or "").strip()
                    s = float(it[2]) if it[2] is not None else 0.0
                    if t: lines.append(t); confs.append(s)
    elif isinstance(out, tuple) and len(out) == 3:       # (boxes, texts, scores)
        _, texts, scores = out
        lines = [str(t).strip() for t in (texts or []) if str(t).strip()]
        confs = [float(s) if s is not None else 0.0 for s in (scores or [])]
    return lines, confs

# ===== Regex và từ khóa theo format MoMo =====
AMOUNT_PATS = [
    r"Số\s*tiền[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
    r"Amount[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
    r"([0-9][0-9\.\, ]{3,}\s?(?:VND|VNĐ|Đ|D))",
    r"([0-9][0-9\.\, ]{3,})\s*(?:VND|VNĐ|Đ|D)",
    r"([0-9][0-9\.\, ]{3,})\s*đ",
]
DATE_PATS = [
    r"(?:Thời\s*gian|Ngày\s*giao\s*dịch|Thời\s*điểm|Date|Time)[:\s\-]*([0-9]{1,2}[:h][0-9]{2}(?::[0-9]{2})?\s*[-]?\s*[0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4})",
    r"(?:Thời\s*gian|Ngày\s*giao\s*dịch|Thời\s*điểm|Date|Time)[:\s\-]*([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4}\s+[0-9]{1,2}[:h][0-9]{2}(?::[0-9]{2})?)",
    r"([0-9]{1,2}[:h][0-9]{2}(?::[0-9]{2})?\s*[\,\-]?\s*[0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4})",
    r"([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4}\s+[0-9]{1,2}[:h][0-9]{2}(?::[0-9]{2})?)",
]
VN_PHONE = re.compile(r"(\+?84|0)\s?\d{2}\s?\d{3}\s?\d{4,5}")

# Người nhận (đã có)
RECIPIENT_KEYS = ["Đến","Tới","Chuyển tới","Chuyển đến","Người nhận","Recipient","To", "Chi tiết"]
# Người gửi (thêm mới)
SENDER_KEYS = ["Từ","Người gửi","Người chuyển","Sender","From"]

TXN_KEYS = ["Mã giao dịch","Mã GD","Transaction ID","Mã tham chiếu","Ref"]

def _vn_amount_to_int(s: str) -> Optional[int]:
    if not s: return None
    x = s.upper()
    for k in ["VND","VNĐ","Đ","D"]: x = x.replace(k, "")
    x = x.replace(".", "").replace(",", "").replace(" ", "")
    x = re.sub(r"[^0-9]", "", x)
    if not x: return None
    try: return int(x)
    except: return None

def _find_first(pats: List[str], text: str) -> Optional[str]:
    for p in pats:
        m = re.search(p, text, flags=re.IGNORECASE|re.MULTILINE)
        if m: return (m.group(1) or m.group(0)).strip()
    return None

def _extract_party(lines: List[str], keys: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Bóc người/tên + SĐT dựa trên danh sách từ khóa (áp dụng cho Sender/Recipient giống nhau).
    Trả về (name, phone).
    """
    name = None; phone = None
    for i, ln in enumerate(lines):
        raw = ln.strip()
        if not raw: continue
        if any(k.lower() in raw.lower() for k in keys):
            after = raw
            for key in keys:
                after = re.sub(key, "", after, flags=re.IGNORECASE)
            after = after.strip(" :|-–—")
            if after:
                ph = VN_PHONE.search(after)
                if ph:
                    phone = ph.group(0).replace(" ", "")
                    after = after.replace(ph.group(0), "").strip(" ,.-–—")
                if after:
                    name = after
            if not name:
                for j in range(1,3):
                    if i+j < len(lines):
                        cand = lines[i+j].strip()
                        if not cand: continue
                        ph = VN_PHONE.search(cand)
                        if ph and not phone:
                            phone = ph.group(0).replace(" ", "")
                            cand_wo = cand.replace(ph.group(0), "").strip(" ,.-–—")
                            name = cand_wo or name
                        else:
                            if not name: name = cand
                        if name: break
            if name or phone: break
    return name, phone

def _momo_parse(lines: List[str], full: str) -> Dict[str, Optional[str]]:
    # Ngày/giờ
    datetime_text = _find_first(DATE_PATS, full)
    # Người gửi
    sender_name, sender_phone = _extract_party(lines, SENDER_KEYS)
    # Người nhận
    recipient_name, recipient_phone = _extract_party(lines, RECIPIENT_KEYS)

    # Mã GD
    txn_id = None
    for ln in lines:
        if any(k.lower() in ln.lower() for k in TXN_KEYS):
            m = re.search(r"(?:[:：]\s*)([A-Za-z0-9\-\.]{6,})", ln)
            if m:
                txn_id = m.group(1).strip(".- ")
            else:
                m2 = re.search(r"([A-Za-z0-9][A-Za-z0-9\-\.]{5,})", ln)
                if m2: txn_id = m2.group(1).strip(".- ")
            if txn_id: break

    # Số tiền
    amount_text = _find_first(AMOUNT_PATS, full)
    amount_val  = _vn_amount_to_int(amount_text) if amount_text else None

    return {
        "sender_name": sender_name,
        "sender_phone": sender_phone,
        "recipient_name": recipient_name,
        "recipient_phone": recipient_phone,
        "datetime_text": datetime_text,
        "txn_id": txn_id,
        "amount_text": amount_text,
        "amount_val": amount_val,
    }

def fast_extract_amount_date(image_url: str) -> Dict:
    t0 = time.time()
    data = _fetch_image_bytes(image_url)
    if time.time()-t0 > OCR_HARD_BUDGET:
        return {"timeout": True, "error": "download too slow"}

    img = _resize_fast(_decode_to_bgr(data))
    lines, confs = _run_rapidocr(img)
    if not lines:
        spent = time.time()-t0
        return {"amount_text":None,"amount":None,"date_text":None,
                "sender_name":None,"sender_phone":None,
                "recipient_name":None,"recipient_phone":None,"txn_id":None,
                "lines":[], "avg_conf":None, "spent_sec":round(spent,2), "note":"no text detected"}

    full = "\n".join(lines[:250])
    momo = _momo_parse(lines, full)
    avg_conf = (sum(confs)/len(confs)) if confs else None
    spent = time.time()-t0

    out = {
        "amount_text": momo.get("amount_text"),
        "amount": momo.get("amount_val"),
        "date_text": momo.get("datetime_text"),
        "sender_name": momo.get("sender_name"),
        "sender_phone": momo.get("sender_phone"),
        "recipient_name": momo.get("recipient_name"),
        "recipient_phone": momo.get("recipient_phone"),
        "txn_id": momo.get("txn_id"),
        "lines": lines[:60],
        "avg_conf": avg_conf,
        "spent_sec": round(spent, 2),
    }
    if spent > OCR_HARD_BUDGET:
        out["timeout"] = True
    return out
