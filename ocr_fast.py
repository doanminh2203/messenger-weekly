# ocr_fast.py
# OCR nhanh cho biên lai MoMo: Amount / DateTime / Recipient (≤ ~30s)
import logging, time, re
from typing import Dict, Tuple, List, Optional

import requests
import numpy as np, cv2
from io import BytesIO
from PIL import Image
from rapidocr_onnxruntime import RapidOCR

logger = logging.getLogger("server")  # dùng chung logger với server.py

# ==== Tham số tốc độ/giới hạn ====
HTTP_TIMEOUT = 12                 # giây cho tải ảnh
MAX_BYTES    = 12 * 1024 * 1024   # 12MB
MAX_SIDE     = 1280               # resize chiều dài lớn nhất
OCR_HARD_BUDGET = 20              # giây cho khối OCR + regex (từ lúc bắt đầu)

_OCR = None
def _ocr():
    """Khởi tạo RapidOCR 1 lần."""
    global _OCR
    if _OCR is None:
        _OCR = RapidOCR()
    return _OCR

def _fetch_image_bytes(url: str) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True, stream=True)
    r.raise_for_status()
    buf = BytesIO()
    read = 0
    chunk = 64 * 1024
    for piece in r.iter_content(chunk_size=chunk):
        if not piece:
            break
        read += len(piece)
        if read > MAX_BYTES:
            raise ValueError("Image too large >MAX_BYTES")
        buf.write(piece)
    data = buf.getvalue()
    if not data:
        raise ValueError("Empty image bytes")
    logger.info(f"OCRFAST bytes={len(data)} url_final={r.url}")
    return data

def _decode_to_bgr(data: bytes) -> np.ndarray:
    try:
        arr = np.frombuffer(data, np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is not None:
            return img
    except Exception as e:
        logger.exception(f"cv2.imdecode error: {e}")
    pil = Image.open(BytesIO(data)).convert("RGB")
    return np.array(pil)[:, :, ::-1]  # RGB -> BGR

def _resize_fast(img: np.ndarray) -> np.ndarray:
    h, w = img.shape[:2]
    mx = max(h, w)
    if mx <= MAX_SIDE:
        return img
    scale = MAX_SIDE / float(mx)
    new_w, new_h = int(w * scale), int(h * scale)
    return cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_LINEAR)

def _run_rapidocr(img: np.ndarray) -> Tuple[List[str], List[float]]:
    """
    Hỗ trợ 2 kiểu output RapidOCR:
      - (result, elapse) với result: [[box, text, score], ...]
      - (boxes, texts, scores)
    Trả về (lines, confs).
    """
    out = _ocr()(img)
    lines: List[str] = []
    confs: List[float] = []
    if isinstance(out, tuple) and len(out) == 2:
        result, _ = out
        if isinstance(result, list):
            for it in result:
                if isinstance(it, (list, tuple)) and len(it) >= 3:
                    t = (it[1] or "").strip()
                    s = float(it[2]) if it[2] is not None else 0.0
                    if t:
                        lines.append(t)
                        confs.append(s)
    elif isinstance(out, tuple) and len(out) == 3:
        _, texts, scores = out
        if texts:
            lines = [str(t).strip() for t in texts if str(t).strip()]
            confs = [float(s) if s is not None else 0.0 for s in (scores or [0.0]*len(lines))]
    return lines, confs

# ===== Regex cơ bản =====
AMOUNT_PATS = [
    r"Số\s*tiền[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
    r"Amount[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
    r"([0-9][0-9\.\, ]{3,}\s?(?:VND|VNĐ|Đ|D))",
    r"([0-9][0-9\.\, ]{3,})\s*(?:VND|VNĐ|Đ|D)",
    r"([0-9][0-9\.\, ]{3,})\s*đ",  # momo hay có "đ"
]

# Các kiểu ngày/giờ thường thấy của MoMo:
#  - "Thời gian: 10:35 - 20/09/2025"
#  - "Ngày giao dịch: 20/09/2025 10:35"
#  - "10:35 20/09/2025" | "20-09-2025 10:35:21"
DATE_PATS = [
    r"(?:Thời\s*gian|Ngày\s*giao\s*dịch|Thời\s*điểm|Date|Time)[:\s\-]*([0-9]{1,2}[:h][0-9]{2}(?::[0-9]{2})?\s*[-]?\s*[0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4})",
    r"(?:Thời\s*gian|Ngày\s*giao\s*dịch|Thời\s*điểm|Date|Time)[:\s\-]*([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4}\s+[0-9]{1,2}[:h][0-9]{2}(?::[0-9]{2})?)",
    r"([0-9]{1,2}[:h][0-9]{2}(?::[0-9]{2})?\s*[\,\-]?\s*[0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4})",
    r"([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4}\s+[0-9]{1,2}[:h][0-9]{2}(?::[0-9]{2})?)",
]

VN_PHONE = re.compile(r"(\+?84|0)\s?\d{2}\s?\d{3}\s?\d{4,5}")

RECIPIENT_KEYS = [
    "Đến", "Tới", "Chuyển tới", "Chuyển đến", "Người nhận", "Recipient", "To"
]
TXN_KEYS = [
    "Mã giao dịch", "Mã GD", "Transaction ID", "Mã tham chiếu", "Ref"
]

def _vn_amount_to_int(s: str) -> Optional[int]:
    if not s: return None
    x = s.upper()
    for k in ["VND","VNĐ","Đ","D"]:
        x = x.replace(k, "")
    x = x.replace(".", "").replace(",", "").replace(" ", "")
    x = re.sub(r"[^0-9]", "", x)
    if not x: return None
    try:
        return int(x)
    except:
        return None

def _find_first(patterns: List[str], text: str) -> Optional[str]:
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            return (m.group(1) or m.group(0)).strip()
    return None

def _momo_parse(lines: List[str], full: str) -> Dict[str, Optional[str]]:
    """
    Cố gắng trích xuất theo bố cục quen thuộc của MoMo:
    - Người nhận (tên) có thể nằm cùng dòng với từ khóa: "Đến Nguyễn Văn A"
      hoặc dòng kế tiếp sau từ khóa.
    - SĐT: bắt theo regex VN_PHONE gần vùng "Đến/Tới/Người nhận".
    - Ngày/giờ: theo các DATE_PATS ở trên.
    - Mã GD: theo TXN_KEYS.
    """
    recipient_name = None
    recipient_phone = None
    datetime_text = None
    txn_id = None

    # 1) DateTime (ưu tiên match toàn văn để dễ lấy nhanh)
    datetime_text = _find_first(DATE_PATS, full)

    # 2) Recipient + phone: quét từng dòng để bắt cụm "Đến|Tới|Người nhận"
    for i, ln in enumerate(lines):
        raw = ln.strip()
        if not raw:
            continue
        low = raw.lower()
        if any(k.lower() in low for k in RECIPIENT_KEYS):
            # TH1: cùng dòng ("Đến Nguyễn Văn A 0903...")
            after = raw
            for key in RECIPIENT_KEYS:
                after = re.sub(key, "", after, flags=re.IGNORECASE)
            after = after.strip(" :|-–—")
            # Nếu sau từ khóa còn nội dung có thể chính là tên
            if after and len(after) >= 2:
                # Tách phone nếu có
                ph = VN_PHONE.search(after)
                if ph:
                    recipient_phone = ph.group(0).replace(" ", "")
                    after = after.replace(ph.group(0), "").strip(" ,.-–—")
                if after:
                    recipient_name = after

            # TH2: nếu dòng khóa chỉ có từ, lấy 1-2 dòng tiếp theo
            if not recipient_name or (recipient_name and len(recipient_name) < 2):
                for j in range(1, 3):
                    if i + j < len(lines):
                        cand = lines[i + j].strip()
                        if not cand:
                            continue
                        # hay có dạng: "Nguyễn Văn A" (xuống dòng)
                        ph = VN_PHONE.search(cand)
                        if ph and not recipient_phone:
                            recipient_phone = ph.group(0).replace(" ", "")
                            cand_wo_ph = cand.replace(ph.group(0), "").strip(" ,.-–—")
                            if cand_wo_ph:
                                recipient_name = cand_wo_ph
                            else:
                                # nếu dòng chỉ là số đt, thử lấy tên ở dòng kế nữa
                                if i + j + 1 < len(lines) and not recipient_name:
                                    cand2 = lines[i + j + 1].strip()
                                    if cand2:
                                        recipient_name = cand2
                        else:
                            # nếu không có phone, coi cả dòng là tên
                            if cand and not recipient_name:
                                recipient_name = cand
                        if recipient_name:
                            break
            if recipient_name or recipient_phone:
                break

    # 3) Transaction ID (mã giao dịch) — tìm dòng có key gần nhất
    #    rồi lấy cụm chữ/số bên phải
    for i, ln in enumerate(lines):
        low = ln.lower()
        if any(k.lower() in low for k in TXN_KEYS):
            # lấy phần sau dấu : hoặc lấy liên tiếp chữ-số
            m = re.search(r"(?:[:：]\s*)([A-Za-z0-9\-\.]{6,})", ln)
            if m:
                txn_id = m.group(1).strip(".- ")
            else:
                # nếu không có “:” thì vớt các block chữ số-kí tự dài
                m2 = re.search(r"([A-Za-z0-9][A-Za-z0-9\-\.]{5,})", ln)
                if m2:
                    txn_id = m2.group(1).strip(".- ")
            if txn_id:
                break

    # 4) Amount: dùng regex cơ bản trên toàn văn
    amount_text = _find_first(AMOUNT_PATS, full)
    amount_val = _vn_amount_to_int(amount_text) if amount_text else None

    return {
        "recipient_name": recipient_name,
        "recipient_phone": recipient_phone,
        "datetime_text": datetime_text,
        "txn_id": txn_id,
        "amount_text": amount_text,
        "amount_val": amount_val,
    }

def fast_extract_amount_date(image_url: str) -> Dict:
    """
    Trả về:
    {
      "amount_text": "...",
      "amount": 1500000,
      "date_text": "...",
      "recipient_name": "...",
      "recipient_phone": "...",
      "txn_id": "...",
      "lines": [...],
      "avg_conf": 0.xx,
      "spent_sec": 3.21,
      "timeout": True/False,
      "note": "..."
    }
    """
    t0 = time.time()

    # tải ảnh
    data = _fetch_image_bytes(image_url)
    if time.time() - t0 > OCR_HARD_BUDGET:
        return {"timeout": True, "error": "download too slow"}

    # decode & resize
    img = _decode_to_bgr(data)
    img = _resize_fast(img)

    # OCR
    lines, confs = _run_rapidocr(img)
    if not lines:
        spent = time.time() - t0
        return {
            "amount_text": None, "amount": None,
            "date_text": None,
            "recipient_name": None, "recipient_phone": None, "txn_id": None,
            "lines": [], "avg_conf": None,
            "spent_sec": round(spent, 2),
            "note": "no text detected"
        }

    full = "\n".join(lines[:250])

    # Ưu tiên parse theo layout MoMo
    momo = _momo_parse(lines, full)

    avg_conf = (sum(confs)/len(confs)) if confs else None
    spent = time.time() - t0
    logger.info(
        "MOMO_PARSE lines=%d avg_conf=%.3f spent=%.2fs",
        len(lines), (avg_conf or 0.0), spent
    )

    out = {
        "amount_text": momo.get("amount_text"),
        "amount": momo.get("amount_val"),
        "date_text": momo.get("datetime_text"),
        "recipient_name": momo.get("recipient_name"),
        "recipient_phone": momo.get("recipient_phone"),
        "txn_id": momo.get("txn_id"),
        "lines": lines[:60],      # cắt bớt
        "avg_conf": avg_conf,
        "spent_sec": round(spent, 2),
    }

    if spent > OCR_HARD_BUDGET:
        out["timeout"] = True
    return out
