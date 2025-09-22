# ocr_fast.py
# OCR nhanh cho Amount/Date (≤ ~30s)
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
    """Khởi tạo RapidOCR 1 lần (nhẹ, không truyền det/cls/rec)."""
    global _OCR
    if _OCR is None:
        _OCR = RapidOCR()
    return _OCR

def _fetch_image_bytes(url: str) -> bytes:
    """Tải ảnh với header thân thiện, theo dõi redirect, giới hạn kích thước."""
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
    """Decode ảnh: ưu tiên OpenCV, nếu fail dùng Pillow."""
    # OpenCV
    try:
        arr = np.frombuffer(data, np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is not None:
            return img
    except Exception as e:
        logger.exception(f"cv2.imdecode error: {e}")
    # Pillow fallback
    pil = Image.open(BytesIO(data)).convert("RGB")
    return np.array(pil)[:, :, ::-1]  # RGB -> BGR (OpenCV dùng BGR)

def _resize_fast(img: np.ndarray) -> np.ndarray:
    """Giảm kích thước ảnh về tối đa MAX_SIDE để OCR nhanh hơn."""
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

# ===== Regex tối ưu cho Amount / Date =====
_AMOUNT_PATS = [
    r"Số\s*tiền[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
    r"Amount[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
    r"([0-9][0-9\.\, ]{3,}\s?(?:VND|VNĐ|Đ|D))",
    r"([0-9][0-9\.\, ]{3,})\s*(?:VND|VNĐ|Đ|D)",
]
_DATE_PATS = [
    r"(?:Thời\s*gian|Ngày\s*giao\s*dịch|Time|Date)[:\s]*([0-9\/\-\:\s]{8,20})",
    r"(\d{1,2}\/\d{1,2}\/\d{2,4}\s+\d{1,2}:\d{2})",
    r"(\d{4}\-\d{1,2}\-\d{1,2}\s+\d{1,2}:\d{2})",
    r"(\d{1,2}\-\d{1,2}\-\d{2,4}\s+\d{1,2}:\d{2})",
]

def _vn_amount_to_int(s: str) -> Optional[int]:
    """Chuẩn hoá số tiền VN: bỏ dấu . , khoảng trắng và hậu tố VND/VNĐ/Đ/D → int."""
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
    """Tìm match đầu tiên theo danh sách pattern."""
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            return (m.group(1) or m.group(0)).strip()
    return None

def fast_extract_amount_date(image_url: str) -> Dict:
    """
    Trả về:
    {
      "amount_text": "...",  # chuỗi tìm thấy
      "amount": 1500000,     # int sau khi chuẩn hoá (nếu parse được)
      "date_text": "...",    # chuỗi ngày/giờ tìm thấy
      "lines": [...],        # tối đa ~50 dòng để debug nhẹ
      "avg_conf": 0.xx,      # độ tin cậy trung bình (nếu có)
      "spent_sec": 3.21,     # thời gian ước lượng
      "timeout": True/False, # nếu vượt budget
      "note": "..."          # ghi chú (nếu có)
    }
    """
    t0 = time.time()

    # 1) tải ảnh (giới hạn kích thước + timeout)
    data = _fetch_image_bytes(image_url)
    if time.time() - t0 > OCR_HARD_BUDGET:
        return {"timeout": True, "error": "download too slow"}

    # 2) decode & resize nhanh
    img = _decode_to_bgr(data)
    img = _resize_fast(img)

    # 3) OCR
    lines, confs = _run_rapidocr(img)
    if not lines:
        spent = time.time() - t0
        return {
            "amount_text": None, "amount": None,
            "date_text": None,
            "lines": [], "avg_conf": None,
            "spent_sec": round(spent, 2),
            "note": "no text detected"
        }

    # 4) Gộp text & tìm amount/date bằng regex tối giản
    full = "\n".join(lines[:200])
    amt_text = _find_first(_AMOUNT_PATS, full)
    date_text = _find_first(_DATE_PATS, full)
    amt_val  = _vn_amount_to_int(amt_text) if amt_text else None

    avg_conf = (sum(confs)/len(confs)) if confs else None
    spent = time.time() - t0
    logger.info(f"OCRFAST lines={len(lines)} avg_conf={avg_conf} spent={spent:.2f}s")

    out = {
        "amount_text": amt_text, "amount": amt_val,
        "date_text": date_text,
        "lines": lines[:50],      # cắt bớt cho nhẹ
        "avg_conf": avg_conf,
        "spent_sec": round(spent, 2),
    }

    if spent > OCR_HARD_BUDGET:
        out["timeout"] = True
    return out
