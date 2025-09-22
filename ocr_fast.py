# ocr_fast.py
# Trích xuất nhanh biên lai MoMo: amount / time / actor("Người thực hiện") / detail("Chi tiết")
# Giữ thời gian chạy ngắn (RapidOCR + regex theo nhãn MoMo).
import logging, time, re
from typing import Dict, List, Tuple, Optional

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
        _OCR = RapidOCR()  # model nhẹ, không phải PyTorch
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
    return np.array(pil)[:, :, ::-1]  # RGB->BGR

def _resize_fast(img: np.ndarray) -> np.ndarray:
    h, w = img.shape[:2]
    if max(h, w) <= MAX_SIDE:
        return img
    s = MAX_SIDE / float(max(h, w))
    return cv2.resize(img, (int(w*s), int(h*s)), interpolation=cv2.INTER_LINEAR)

def _run_ocr(img: np.ndarray) -> List[str]:
    """Trả về list các dòng text (đã clean)."""
    out = _ocr()(img)
    lines: List[str] = []

    # RapidOCR có 2 kiểu trả về: ([ [box,text,score], ...], elapse) hoặc (boxes, texts, scores)
    if isinstance(out, tuple) and len(out) == 2:
        result, _ = out
        if isinstance(result, list):
            for it in result:
                if isinstance(it, (list, tuple)) and len(it) >= 2:
                    t = (it[1] or "").strip()
                    if t:
                        lines.append(t)
    elif isinstance(out, tuple) and len(out) == 3:
        _, texts, _ = out
        lines = [str(t).strip() for t in (texts or []) if str(t).strip()]

    # gom các dấu cách lạ
    return [re.sub(r"\s+", " ", ln).strip() for ln in lines]

# ===== Regex theo format MoMo =====
RE_AMOUNT = re.compile(
    r"(?:^[\+\-]?\s*\d{1,3}(?:[\. ]\d{3})+\s*[đd]|"
    r"Số\s*tiền[: ]*\d{1,3}(?:[\. ]\d{3})+\s*(?:VND|VNĐ|đ|d)?)",
    re.IGNORECASE
)

RE_TIME = re.compile(
    r"(\d{1,2}[:h]\d{2}(?::\d{2})?\s*-\s*\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\s+\d{1,2}[:h]\d{2}(?::\d{2})?)"
)

LABEL_TIME    = re.compile(r"^(Thời gian|Time|Date)\b", re.IGNORECASE)
LABEL_ACTOR   = re.compile(r"^(Người thực hiện|Người gửi|Sender|From)\b", re.IGNORECASE)
LABEL_DETAIL  = re.compile(r"^(Chi tiết|Nội dung|Content|Detail)\b", re.IGNORECASE)

def _pick_value_after_label(lines: List[str], idx: int) -> Optional[str]:
    """
    Lấy giá trị nằm cùng dòng (sau dấu :) hoặc ở 1–2 dòng kế tiếp.
    """
    cur = lines[idx]
    # cùng dòng sau ':'
    m = re.search(r":\s*(.+)$", cur)
    if m:
        val = m.group(1).strip()
        if val:
            return val
    # dòng kế tiếp
    for j in range(1, 3):
        if idx + j < len(lines):
            cand = lines[idx + j].strip().strip(":").strip()
            if cand and not LABEL_TIME.match(cand) and not LABEL_ACTOR.match(cand) and not LABEL_DETAIL.match(cand):
                return cand
    return None

def _normalize_amount(token: str) -> Optional[str]:
    """Giữ dạng '10.000đ' hoặc '10.000 VND' như người dùng thấy; fallback None."""
    if not token:
        return None
    x = token.upper().replace("VND", "đ").replace("VNĐ", "đ")
    x = x.replace(" ", "")
    # chỉ giữ phần số + 'đ'
    m = re.search(r"[\+\-]?\d{1,3}(?:[\.]\d{3})+\s*[đD]", x)
    if m:
        show = m.group(0)
        # chuẩn hoá đ -> đ (lowercase)
        show = show.replace("D", "đ")
        return show
    # trường hợp “Số tiền: 10.000”
    m2 = re.search(r"\d{1,3}(?:[\.]\d{3})+", x)
    if m2:
        return m2.group(0) + "đ"
    return None

def fast_extract_amount_date(image_url: str) -> Dict:
    """
    Trả:
      amount_text, date_text, actor_name, detail_text
      đồng thời giữ 'amount' ở dạng số nguyên (nếu parse được).
    """
    t0 = time.time()
    data = _fetch_image_bytes(image_url)
    img  = _resize_fast(_decode_to_bgr(data))
    lines = _run_ocr(img)

    # ghép thành chuỗi lớn để tìm nhanh
    full = "\n".join(lines)

    # ----- SỐ TIỀN -----
    amount_text = None
    for ln in lines:
        if RE_AMOUNT.search(ln):
            amount_text = _normalize_amount(RE_AMOUNT.search(ln).group(0))
            if amount_text:
                break
    # fallback: tìm dòng đầu trang có dấu '+' và 'đ'
    if not amount_text:
        m = re.search(r"[\+\-]?\s*\d{1,3}(?:[\. ]\d{3})+\s*[đdD]\b", full)
        if m:
            amount_text = _normalize_amount(m.group(0))

    amount_val = None
    if amount_text:
        num = re.sub(r"[^\d]", "", amount_text)
        if num.isdigit():
            try:
                amount_val = int(num)
            except:  # pragma: no cover
                pass

    # ----- THỜI GIAN -----
    date_text = None
    # 1) ưu tiên dòng có nhãn "Thời gian:"
    for i, ln in enumerate(lines):
        if LABEL_TIME.match(ln):
            date_text = _pick_value_after_label(lines, i)
            if date_text: break
    # 2) fallback regex “HH:MM - DD/MM/YYYY”
    if not date_text:
        m = RE_TIME.search(full)
        if m:
            date_text = m.group(1)

    # ----- NGƯỜI THỰC HIỆN -----
    actor_name = None
    for i, ln in enumerate(lines):
        if LABEL_ACTOR.match(ln):
            actor_name = _pick_value_after_label(lines, i)
            if actor_name: break

    # ----- CHI TIẾT -----
    detail_text = None
    for i, ln in enumerate(lines):
        if LABEL_DETAIL.match(ln):
            detail_text = _pick_value_after_label(lines, i)
            if detail_text: break

    spent = round(time.time() - t0, 2)
    return {
        "amount_text": amount_text,
        "amount": amount_val,
        "date_text": date_text,
        "actor_name": actor_name,
        "detail_text": detail_text,
        "spent_sec": spent,
        "lines": lines[:60],  # để debug nếu cần
    }
