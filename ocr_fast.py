# ocr_fast.py
# Trích xuất nhanh biên lai MoMo: amount / time / actor("Người thực hiện") / detail("Chi tiết")
# Bắt giá trị khi nhãn và thông tin có nhiều khoảng trắng, có/không có dấu ":".

import logging, time, re
from typing import Dict, List, Optional, Tuple

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
        if read > MAX_BYTES:
            raise ValueError("Image too large")
        buf.write(piece)
    data = buf.getvalue()
    if not data:
        raise ValueError("Empty image")
    return data

def _decode_to_bgr(data: bytes) -> np.ndarray:
    try:
        arr = np.frombuffer(data, np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is not None:
            return img
    except Exception:
        pass
    pil = Image.open(BytesIO(data)).convert("RGB")
    return np.array(pil)[:, :, ::-1]

def _resize_fast(img: np.ndarray) -> np.ndarray:
    h, w = img.shape[:2]
    if max(h, w) <= MAX_SIDE:
        return img
    s = MAX_SIDE / float(max(h, w))
    return cv2.resize(img, (int(w*s), int(h*s)), interpolation=cv2.INTER_LINEAR)

def _run_ocr(img: np.ndarray) -> List[str]:
    out = _ocr()(img)
    lines: List[str] = []
    if isinstance(out, tuple) and len(out) == 2:
        result, _ = out
        if isinstance(result, list):
            for it in result:
                if isinstance(it, (list, tuple)) and len(it) >= 2:
                    t = (it[1] or "").strip()
                    if t: lines.append(t)
    elif isinstance(out, tuple) and len(out) == 3:
        _, texts, _ = out
        lines = [str(t).strip() for t in (texts or []) if str(t).strip()]
    # chuẩn hoá khoảng trắng
    return [re.sub(r"\s+", " ", ln).strip() for ln in lines]

# ===== Regex theo format MoMo =====
RE_AMOUNT = re.compile(
    r"(?:^[\+\-]?\s*\d{1,3}(?:[\. ]\d{3})+\s*[đd]|Số\s*tiền\s*[:\-]?\s*\d{1,3}(?:[\. ]\d{3})+\s*(?:VND|VNĐ|đ|d)?)",
    re.IGNORECASE
)
RE_TIME = re.compile(
    r"(\d{1,2}[:h]\d{2}(?::\d{2})?\s*-\s*\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\s+\d{1,2}[:h]\d{2}(?::\d{2})?)"
)

# Tạo regex nhãn với \s+ giữa các từ (để chịu được OCR bẻ khoảng trắng)
def _label_pattern(words: str) -> str:
    # ví dụ "Người thực hiện" -> "Người\s+thực\s+hiện"
    return r"\s+".join(map(re.escape, words.split()))

LABELS_TIME   = [ "Thời gian", "Time", "Date" ]
LABELS_ACTOR  = [ "Người thực hiện", "Người gửi", "Sender", "From", "Nguoi thuc hien" ]
LABELS_DETAIL = [ "Chi tiết", "Nội dung", "Content", "Detail" ]

TIME_RE   = re.compile(rf"^({'|'.join(_label_pattern(x) for x in LABELS_TIME)})\b", re.IGNORECASE)
ACTOR_RE  = re.compile(rf"^({'|'.join(_label_pattern(x) for x in LABELS_ACTOR)})\b", re.IGNORECASE)
DETAIL_RE = re.compile(rf"^({'|'.join(_label_pattern(x) for x in LABELS_DETAIL)})\b", re.IGNORECASE)

def _normalize_amount(token: str) -> Optional[str]:
    if not token: return None
    x = token.upper().replace("VND", "đ").replace("VNĐ", "đ").replace(" ", "")
    m = re.search(r"[\+\-]?\d{1,3}(?:[\.]\d{3})+\s*[đD]", x)
    if m:
        show = m.group(0).replace("D", "đ")
        return show
    m2 = re.search(r"\d{1,3}(?:[\.]\d{3})+", x)
    if m2:
        return m2.group(0) + "đ"
    return None

def _looks_like_label(s: str) -> bool:
    return bool(TIME_RE.match(s) or ACTOR_RE.match(s) or DETAIL_RE.match(s))

def _value_after_label(lines: List[str], idx: int) -> Optional[str]:
    """
    Lấy giá trị cho 1 nhãn: thử cùng dòng (sau ':' hoặc chỉ cách bằng space),
    nếu không có thì lấy 1–3 dòng kế tiếp (bỏ qua dòng rỗng hoặc lại là nhãn).
    """
    cur = lines[idx]

    # 1) cùng dòng — có hoặc không có dấu ':' / '-'
    m = re.search(r"(?:[:\-–—]\s*|\s{2,})(.+)$", cur)
    if m:
        val = m.group(1).strip(" :|-–—")
        if val:
            return val

    # 2) ghép cửa sổ 2 dòng (label + next) để bắt dạng label   value (hai cột)
    if idx + 1 < len(lines):
        join2 = (cur + " " + lines[idx+1]).strip()
        m2 = re.search(r"(?:[:\-–—]\s*|\s{2,})(.+)$", join2)
        if m2:
            val = m2.group(1).strip(" :|-–—")
            if val:
                return val

    # 3) dò 1–3 dòng kế tiếp
    for j in range(1, 4):
        if idx + j < len(lines):
            cand = lines[idx + j].strip(" :|-–—").strip()
            if cand and not _looks_like_label(cand):
                return cand
    return None

def fast_extract_amount_date(image_url: str) -> Dict:
    t0 = time.time()
    data = _fetch_image_bytes(image_url)
    img  = _resize_fast(_decode_to_bgr(data))
    lines = _run_ocr(img)
    full  = "\n".join(lines)

    # ----- SỐ TIỀN -----
    amount_text = None
    for ln in lines:
        m = RE_AMOUNT.search(ln)
        if m:
            amount_text = _normalize_amount(m.group(0))
        if amount_text:
            break
    if not amount_text:
        m2 = re.search(r"[\+\-]?\s*\d{1,3}(?:[\. ]\d{3})+\s*[đdD]\b", full)
        if m2:
            amount_text = _normalize_amount(m2.group(0))

    amount_val = None
    if amount_text:
        num = re.sub(r"[^\d]", "", amount_text)
        if num.isdigit():
            try: amount_val = int(num)
            except: pass

    # ----- THỜI GIAN -----
    date_text = None
    for i, ln in enumerate(lines):
        if TIME_RE.match(ln):
            date_text = _value_after_label(lines, i)
            if date_text: break
    if not date_text:
        m = RE_TIME.search(full)
        if m: date_text = m.group(1)

    # ----- NGƯỜI THỰC HIỆN -----
    actor_name = None
    for i, ln in enumerate(lines):
        if ACTOR_RE.match(ln):
            actor_name = _value_after_label(lines, i)
            if actor_name: break

    # ----- CHI TIẾT -----
    detail_text = None
    for i, ln in enumerate(lines):
        if DETAIL_RE.match(ln):
            detail_text = _value_after_label(lines, i)
            if detail_text: break

    spent = round(time.time() - t0, 2)
    return {
        "amount_text": amount_text,
        "amount": amount_val,
        "date_text": date_text,
        "actor_name": actor_name,
        "detail_text": detail_text,
        "spent_sec": spent,
        "lines": lines[:80],  # debug nếu cần
    }
