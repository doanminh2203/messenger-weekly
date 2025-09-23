# ocr_fast.py
import re
import time
import io
import unicodedata
from typing import List, Tuple, Dict, Optional

import cv2
import numpy as np
import requests

try:
    # RapidOCR (onnxruntime) – đã có trong requirements
    from rapidocr_onnxruntime import RapidOCR
    _HAS_RAPID = True
except Exception:
    RapidOCR = None
    _HAS_RAPID = False


# -------------------- Helpers --------------------
def _strip_accents(s: str) -> str:
    if not s:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _to_plain(s: str) -> str:
    """Bỏ dấu + lowercase (phục vụ tìm key VN/không dấu)."""
    return _strip_accents(s).lower()

def _load_image_from_url(url: str, timeout: int = 15) -> Optional[np.ndarray]:
    r = requests.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    data = r.content
    img_arr = np.frombuffer(data, np.uint8)
    img = cv2.imdecode(img_arr, cv2.IMREAD_COLOR)
    return img

def _opencv_boost(img: np.ndarray) -> np.ndarray:
    """
    Tăng tương phản / sharpen nhẹ để OCR ổn định hơn.
    Làm rất nhanh, không tốn nhiều thời gian.
    """
    if img is None:
        return img
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # CLAHE
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
    cl = clahe.apply(gray)
    # Sharpen
    kernel = np.array([[0, -1, 0],
                       [-1, 5, -1],
                       [0, -1, 0]])
    sharp = cv2.filter2D(cl, -1, kernel)
    return sharp

def _run_rapid_ocr(img: np.ndarray) -> Tuple[List[str], float]:
    """
    Chạy RapidOCR, trả về (lines, spent_seconds).
    Xử lý cả hai kiểu trả về: (boxes, texts, scores) hoặc (texts, scores).
    """
    start = time.time()
    ocr = RapidOCR(det=True, cls=True, rec=True) if _HAS_RAPID else None
    if ocr is None:
        return [], 0.0

    result = ocr(img)
    texts: List[str] = []
    try:
        # kiểu (boxes, texts, scores)
        boxes, texts, scores = result
        if texts is None:
            texts = []
    except Exception:
        # kiểu (texts, scores)
        try:
            texts, scores = result
            if texts is None:
                texts = []
        except Exception:
            texts = []

    # Chuẩn hoá khoảng trắng & loại bỏ rỗng
    lines = [_norm_space(t) for t in texts if _norm_space(t)]
    spent = time.time() - start
    return lines, spent


# -------------------- Parsers --------------------
_AMT_RE = re.compile(
    r"([+\-]?\s?\d{1,3}(?:[.,]\d{3})+|\d+)\s*(?:đ|d|vnđ|vnd)?",
    flags=re.IGNORECASE
)

_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
_TIME_RE = re.compile(r"\b(\d{1,2}:\d{2})\b")

def _parse_amount(lines: List[str]) -> Tuple[Optional[str], Optional[int]]:
    """
    Tìm số tiền ưu tiên có dấu '+' hoặc lớn nhất.
    Trả (amount_text, amount_value_int)
    """
    best_text = None
    best_val = None
    for ln in lines:
        for m in _AMT_RE.finditer(ln):
            txt = m.group(1)
            raw = re.sub(r"[^\d]", "", txt)
            if not raw:
                continue
            val = int(raw)
            # ưu tiên có dấu '+' ngay trước cụm
            plus_prior = '+' in ln[max(0, m.start()-1):m.start()+1]
            if best_val is None or plus_prior or val > best_val:
                best_val = val
                # cố giữ hậu tố đ/đồng nếu có trong chuỗi
                tail = ""
                if re.search(r"(đ|vnđ|vnd)", ln[m.end():m.end()+3], re.I):
                    tail = "đ"
                best_text = _norm_space(f"{txt}{tail}")
            # nếu đã có '+' thì break sớm khỏi dòng
            if plus_prior:
                break
    return best_text, best_val

def _parse_datetime(lines: List[str]) -> Optional[str]:
    """
    Tạo chuỗi 'HH:MM-dd/mm/yyyy' nếu có, ngược lại trả date đơn.
    Mẫu MoMo hay có '22:25-22/09/2025' hoặc tách dòng.
    """
    # 1) Tìm trên từng dòng: có cả time & date
    for ln in lines:
        # case 'HH:MM- dd/mm/yyyy' trên cùng 1 dòng
        mdate = _DATE_RE.search(ln)
        if mdate:
            # tìm time gần date nhất trong cùng dòng
            mt = _TIME_RE.search(ln[:mdate.start()]) or _TIME_RE.search(ln[mdate.end():])
            d, mm, y = mdate.groups()
            if mt:
                return f"{mt.group(1)}-{int(d):02d}/{int(mm):02d}/{int(y):04d}"
            else:
                return f"{int(d):02d}/{int(mm):02d}/{int(y):04d}"

    # 2) Nếu không có cùng dòng: ghép từ dòng time & dòng date liền kề
    time_idx, date_idx = -1, -1
    times = []
    dates = []
    for i, ln in enumerate(lines):
        if _TIME_RE.search(ln):
            times.append((i, _TIME_RE.search(ln).group(1)))
        if _DATE_RE.search(ln):
            g = _DATE_RE.search(ln).groups()
            dates.append((i, f"{int(g[0]):02d}/{int(g[1]):02d}/{int(g[2]):04d}"))
    if times and dates:
        # chọn cặp gần nhau nhất
        best = None
        best_gap = 9999
        for ti, tt in times:
            for di, dd in dates:
                gap = abs(di - ti)
                if gap < best_gap:
                    best_gap = gap
                    best = (tt, dd)
        if best:
            return f"{best[0]}-{best[1]}"

    # 3) chỉ có date độc lập
    if dates:
        return dates[0][1]
    return None

def _find_label_value(lines: List[str], labels: List[str]) -> Optional[str]:
    """
    Tìm giá trị ngay sau nhãn (trên cùng dòng hoặc dòng kế).
    labels: danh sách không dấu (vd: ["nguoi thuc hien", "nguoi thanh toan"])
    """
    for idx, ln in enumerate(lines):
        plain = _to_plain(ln)
        for lb in labels:
            pos = plain.find(lb)
            if pos >= 0:
                # lấy phần còn lại sau label trên cùng dòng
                tail = ln[pos + len(lb):].strip(" :|-")
                # nếu tail còn chữ => dùng luôn
                if tail and len(tail) >= 2:
                    return _norm_space(tail)
                # nếu không, thử dòng kế tiếp
                if idx + 1 < len(lines):
                    nxt = _norm_space(lines[idx + 1])
                    if nxt:
                        return nxt
                # hoặc lùi 1 dòng nếu nhãn riêng 1 dòng phía sau
                if idx > 0:
                    prev = _norm_space(lines[idx - 1])
                    if prev:
                        return prev
    return None


# -------------------- Public API --------------------
def fast_extract_amount_date_actor_detail(image_url: str, timeout: int = 30) -> Dict[str, object]:
    """
    Trích nhanh: amount, datetime (text), actor, detail + full lines, thời gian chạy.
    Ưu tiên chạy OCR (RapidOCR). Nếu lỗi/timeout, trả kết quả rỗng.
    """
    t0 = time.time()
    lines: List[str] = []
    amount_text: Optional[str] = None
    amount_value: Optional[int] = None
    when_text: Optional[str] = None
    actor: Optional[str] = None
    detail: Optional[str] = None

    try:
        img = _load_image_from_url(image_url, timeout=min(15, timeout))
        if img is None:
            raise RuntimeError("Cannot decode image")
        proc = _opencv_boost(img)

        if not _HAS_RAPID:
            # Không cài RapidOCR – trả rỗng để server vẫn hoạt động
            spent = time.time() - t0
            return {
                "amount_text": None,
                "amount_value": None,
                "when_text": None,
                "actor": None,
                "detail": None,
                "lines": [],
                "spent": spent,
            }

        # Chạy RapidOCR
        lines, ocr_spent = _run_rapid_ocr(proc)

        # Safety timeout
        if time.time() - t0 > timeout:
            return {
                "amount_text": None,
                "amount_value": None,
                "when_text": None,
                "actor": None,
                "detail": None,
                "lines": lines,
                "spent": time.time() - t0,
            }

        # --------- PARSE ----------
        amount_text, amount_value = _parse_amount(lines)
        when_text = _parse_datetime(lines)

        # actor (người thực hiện) – các biến thể có/không dấu
        actor = _find_label_value(
            lines,
            labels=[
                "nguoi thuc hien",
                "nguoithuchien",
                "nguoi thanh toan",
                "nguoi chuyen",
                "chu tai khoan",
                "ten tai khoan",
            ],
        )

        # detail (chi tiết)
        detail = _find_label_value(
            lines,
            labels=[
                "chi tiet",
                "noi dung",
                "noi dung chuyen tien",
                "noi dung giao dich",
                "mo ta",
            ],
        )

        spent = time.time() - t0
        return {
            "amount_text": amount_text,
            "amount_value": amount_value,
            "when_text": when_text,
            "actor": actor,
            "detail": detail,
            "lines": lines,
            "spent": spent,
        }

    except Exception:
        spent = time.time() - t0
        return {
            "amount_text": None,
            "amount_value": None,
            "when_text": None,
            "actor": None,
            "detail": None,
            "lines": lines,
            "spent": spent,
        }
