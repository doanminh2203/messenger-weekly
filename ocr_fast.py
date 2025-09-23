# ocr_fast.py
# MoMo OCR nhanh: Số tiền / Thời gian / Người thực hiện / Chi tiết
# BẮT NHÃN KHÔNG PHÂN BIỆT DẤU + LINH HOẠT KHOẢNG TRẮNG

import logging, time, re, unicodedata
from typing import Dict, List, Optional

import requests
import numpy as np, cv2
from io import BytesIO
from PIL import Image
from rapidocr_onnxruntime import RapidOCR

logger = logging.getLogger("server")

HTTP_TIMEOUT = 12
MAX_BYTES    = 12 * 1024 * 1024
MAX_SIDE     = 1280

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
    for chunk in r.iter_content(64*1024):
        if not chunk: break
        read += len(chunk)
        if read > MAX_BYTES:
            raise ValueError("Image too large")
        buf.write(chunk)
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
    # rút gọn khoảng trắng
    return [re.sub(r"\s+", " ", ln).strip() for ln in lines]

# ---------- Chuẩn hoá không dấu ----------
def _strip_accents(s: str) -> str:
    # loại bỏ dấu tiếng Việt (NFD + bỏ combining marks)
    s_norm = unicodedata.normalize('NFD', s)
    s_no = "".join(ch for ch in s_norm if unicodedata.category(ch) != 'Mn')
    return unicodedata.normalize('NFC', s_no)

def _norm_key(s: str) -> str:
    # không dấu + thường + rút gọn khoảng trắng
    s2 = _strip_accents(s).lower()
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2

# ---------- Regex/nhãn ----------
RE_AMOUNT = re.compile(
    r"(?:^[\+\-]?\s*\d{1,3}(?:[\. ]\d{3})+\s*[đd]|so\s*tien\s*[:\-]?\s*\d{1,3}(?:[\. ]\d{3})+\s*(?:vnd|vnđ|đ|d)?)",
    re.IGNORECASE
)
RE_TIME_FALLBACK = re.compile(
    r"(\d{1,2}[:h]\d{2}(?::\d{2})?\s*-\s*\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\s+\d{1,2}[:h]\d{2}(?::\d{2})?)"
)

LABELS_TIME    = ["thoi gian", "time", "date"]
LABELS_ACTOR   = ["nguoi thuc hien", "nguoi gui", "sender", "from"]
LABELS_DETAIL  = ["chi tiet", "noi dung", "content", "detail"]

def _normalize_amount(token: str) -> Optional[str]:
    if not token: return None
    x = token.upper().replace("VND","đ").replace("VNĐ","đ")
    x = x.replace(" ", "")
    m = re.search(r"[\+\-]?\d{1,3}(?:[\.]\d{3})+\s*[đD]", x)
    if m:
        return m.group(0).replace("D","đ")
    m2 = re.search(r"\d{1,3}(?:[\.]\d{3})+", x)
    if m2:
        return m2.group(0) + "đ"
    return None

def _looks_like_label_norm(norm: str) -> bool:
    return any(norm.startswith(k) for k in LABELS_TIME + LABELS_ACTOR + LABELS_DETAIL)

def _value_after_label(orig_lines: List[str], idx: int) -> Optional[str]:
    """
    Lấy value từ bản gốc:
      - cùng dòng sau ':', '-', hoặc khoảng trắng kép
      - nếu không có → lấy 1–3 dòng kế tiếp (bỏ qua dòng là nhãn)
    """
    cur = orig_lines[idx]

    m = re.search(r"(?:[:\-–—]\s*|\s{2,})(.+)$", cur)
    if m:
        val = m.group(1).strip(" :|-–—")
        if val: return val

    # Ghép 2 dòng (để bắt layout 2 cột)
    if idx + 1 < len(orig_lines):
        join2 = (cur + " " + orig_lines[idx+1]).strip()
        m2 = re.search(r"(?:[:\-–—]\s*|\s{2,})(.+)$", join2)
        if m2:
            val = m2.group(1).strip(" :|-–—")
            if val: return val

    for j in range(1, 4):
        if idx + j < len(orig_lines):
            cand = orig_lines[idx+j].strip(" :|-–—").strip()
            if cand and not _looks_like_label_norm(_norm_key(cand)):
                return cand
    return None

def fast_extract_amount_date(image_url: str) -> Dict:
    t0 = time.time()
    data = _fetch_image_bytes(image_url)
    img  = _resize_fast(_decode_to_bgr(data))
    lines = _run_ocr(img)

    # Bản không dấu để dò nhãn
    norms = [_norm_key(ln) for ln in lines]

    # ---------- AMOUNT ----------
    amount_text = None
    for ln in lines:
        m = RE_AMOUNT.search(_norm_key(ln))
        if m:
            # m.group(0) là trên bản đã normalize; để hiển thị đẹp, lấy lại từ ln
            # thử tìm số trong ln gốc
            cand = _normalize_amount(ln)
            if not cand:
                cand = _normalize_amount(m.group(0))
            if cand:
                amount_text = cand
                break
    if not amount_text:
        # fallback: quét trong toàn bộ text không dấu
        full_norm = "\n".join(norms)
        m2 = re.search(r"[\+\-]?\s*\d{1,3}(?:[\. ]\d{3})+\s*[đd]\b", full_norm)
        if m2:
            amount_text = _normalize_amount(m2.group(0))

    amount_val = None
    if amount_text:
        num = re.sub(r"[^\d]", "", amount_text)
        if num.isdigit():
            try: amount_val = int(num)
            except: pass

    # ---------- TIME ----------
    date_text = None
    for i, n in enumerate(norms):
        if any(n.startswith(k) for k in LABELS_TIME):
            date_text = _value_after_label(lines, i)
            if date_text: break
    if not date_text:
        # fallback regex trên bản gốc
        full = "\n".join(lines)
        m = RE_TIME_FALLBACK.search(full)
        if m: date_text = m.group(1)

    # ---------- ACTOR ----------
    actor_name = None
    for i, n in enumerate(norms):
        if any(n.startswith(k) for k in LABELS_ACTOR):
            actor_name = _value_after_label(lines, i)
            if actor_name: break

    # ---------- DETAIL ----------
    detail_text = None
    for i, n in enumerate(norms):
        if any(n.startswith(k) for k in LABELS_DETAIL):
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
        "lines": lines[:100],
    }
