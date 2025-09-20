# ocr_model.py (RapidOCR nhẹ + regex mở rộng + fallback)
import re
from typing import Any, Dict, List, Tuple, Optional
from rapidfuzz import fuzz
import requests

# RapidOCR
from rapidocr_onnxruntime import RapidOCR

_OCR = None

def _get_ocr():
    """Khởi tạo RapidOCR với cấu hình mặc định (tránh lỗi det_/cls_/rec_)."""
    global _OCR
    if _OCR is None:
        _OCR = RapidOCR()  # KHÔNG truyền det/cls/rec
    return _OCR

def _fetch_image_bytes(url: str) -> bytes:
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    return r.content

def _vn_to_int_amount(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    x = s.strip().upper()
    x = x.replace("VND", "").replace("VNĐ", "").replace("Đ", "").replace("D", "")
    x = x.replace(".", "").replace(",", "")
    x = re.sub(r"[^0-9]", "", x)
    if not x:
        return None
    try:
        return int(x)
    except:
        return None

def _pick_first(patterns: List[str], text: str) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            return (m.group(1) or m.group(0)).strip() if m.groups() else m.group(0).strip()
    return None

def _largest_number_as_amount(text: str) -> Optional[int]:
    """Fallback: tìm số có độ dài lớn trong toàn văn, coi như số tiền (rủi ro, để cuối)."""
    cands = re.findall(r"\b\d[\d\.\, ]{4,}\b", text)
    best = 0
    for c in cands:
        v = _vn_to_int_amount(c)
        if v and v > best:
            best = v
    return best or None

def parse_fields(full_text: str) -> Dict[str, Any]:
    """Tách các trường phổ biến trên bill chuyển khoản VN từ full OCR text."""
    # Chuẩn hóa khoảng trắng để regex ổn định hơn
    text = re.sub(r"[ \t]+", " ", full_text)
    text_noaccent_upper = (
        text.upper()
        .replace("Ầ","A").replace("Ằ","A").replace("Ắ","A").replace("Ẳ","A").replace("Ẵ","A").replace("Ặ","A")
        .replace("À","A").replace("Á","A").replace("Ả","A").replace("Ã","A").replace("Ạ","A")
        .replace("Â","A").replace("Ă","A")
        .replace("È","E").replace("É","E").replace("Ẻ","E").replace("Ẽ","E").replace("Ẹ","E")
        .replace("Ê","E")
        .replace("Ì","I").replace("Í","I").replace("Ỉ","I").replace("Ĩ","I").replace("Ị","I")
        .replace("Ò","O").replace("Ó","O").replace("Ỏ","O").replace("Õ","O").replace("Ọ","O")
        .replace("Ô","O").replace("Ơ","O")
        .replace("Ù","U").replace("Ú","U").replace("Ủ","U").replace("Ũ","U").replace("Ụ","U")
        .replace("Ư","U")
        .replace("Ỳ","Y").replace("Ý","Y").replace("Ỷ","Y").replace("Ỹ","Y").replace("Ỵ","Y")
        .replace("Đ","D")
    )

    # ===== SỐ TIỀN =====
    amt = _pick_first([
        r"Số tiền[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
        r"Amount[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
        r"(?:SO\s*TIEN|AMOUNT)[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
        r"([0-9][0-9\.\, ]{3,}VND)",
        r"([0-9][0-9\.\, ]{3,}\s?(?:VNĐ|Đ|D))",
    ], text)
    amt_int = _vn_to_int_amount(amt) if amt else None
    if amt_int is None:
        # Fallback: lấy số lớn nhất trong toàn văn
        amt_int = _largest_number_as_amount(text)

    # ===== STK NHẬN =====
    acc_to = _pick_first([
        r"(?:STK|Số\s*tài\s*khoản|Account\s*No\.?)[:\s]*([0-9\- ]{6,})",
        r"(?:Tài\s*khoản\s*nhận|ACC\s*TO)[:\s]*([0-9\- ]{6,})",
        r"(?:Account\s*to)[:\s]*([0-9\- ]{6,})",
    ], text)
    acc_to = acc_to.replace(" ", "").replace("-", "") if acc_to else None

    # ===== TÊN NGƯỜI NHẬN =====
    name_to = _pick_first([
        r"(?:Tên\s*người\s*nhận|Người\s*nhận|Beneficiary\s*Name)[:\s]*([A-Za-zÀ-ỹ\s\.\-]{3,})",
        r"(?:CHU\s*TK\s*NHAN|TO\s*NAME)[:\s]*([A-Z\s\.\-]{3,})",
    ], text)

    # ===== NGƯỜI GỬI =====
    name_from = _pick_first([
        r"(?:Người\s*gửi|Tên\s*người\s*gửi|Chủ\s*tài\s*khoản\s*gửi|Sender|Payer|From\s*Name)[:\s]*([A-Za-zÀ-ỹ\s\.\-]{3,})",
        r"(?:NGUOI\s*CHUYEN|NGUOI\s*GUI|FROM\s*NAME)[:\s]*([A-Z\s\.\-]{3,})",
    ], text)
    if not name_from:
        m = re.search(r"(?:FROM|NGUOI\s*CHUYEN|PAYER)[:\s]*([A-ZÀ-Ỵ\s\.\-]{3,})", text_noaccent_upper, re.I)
        if m:
            name_from = m.group(1).strip()

    # ===== STK GỬI =====
    acc_from = _pick_first([
        r"(?:STK\s*nguồn|Tài\s*khoản\s*gửi|From\s*Account|Account\s*from)[:\s]*([0-9\- ]{6,})",
        r"(?:SO\s*TK\s*GUI|ACC\s*FROM)[:\s]*([0-9\- ]{6,})",
    ], text)
    acc_from = acc_from.replace(" ", "").replace("-", "") if acc_from else None

    # ===== NỘI DUNG =====
    memo = _pick_first([
        r"(?:Nội\s*dung|Ghi\s*chú|Content|Description|ND\s*chuyển|NOI\s*DUNG)[:\s]*([^\n]+)",
    ], text)

    # ===== THỜI GIAN =====
    when = _pick_first([
        r"(?:Thời\s*gian|Ngày\s*giao\s*dịch|Time|Date)[:\s]*([0-9\/\-\:\s]{8,20})",
        r"(?:NGAY|TIME|DATE)[:\s]*([0-9\/\-\:\s]{8,20})",
        r"(\d{1,2}\/\d{1,2}\/\d{2,4}\s+\d{1,2}:\d{2})",
        r"(\d{4}\-\d{1,2}\-\d{1,2}\s+\d{1,2}:\d{2})",
    ], text)

    # ===== MÃ GIAO DỊCH =====
    tx = _pick_first([
        r"(?:Mã\s*giao\s*dịch|Transaction\s*ID|Ref(?:erence)?)[:\s]*([A-Za-z0-9\-]{6,})",
    ], text)

    return {
        "raw_text": full_text,
        "amount_text": amt,
        "amount": amt_int,

        "sender_name": name_from.strip() if name_from else None,
        "sender_account": acc_from,

        "receiver_name": name_to.strip() if name_to else None,
        "account_number": acc_to,   # tài khoản nhận

        "memo": memo.strip() if memo else None,
        "datetime_text": when,
        "tx_code": tx
    }

def score_match(extracted: Dict[str, Any], expected: Dict[str, Any]) -> Dict[str, Any]:
    report = {"checks": {}, "overall": 0.0}

    exp_amt = expected.get("amount")
    if exp_amt is not None:
        if isinstance(exp_amt, str):
            exp_amt = _vn_to_int_amount(exp_amt)
        ok = (extracted.get("amount") == exp_amt)
        report["checks"]["amount"] = 100.0 if ok else 0.0

    exp_acc = expected.get("account_number")
    if exp_acc:
        ex_acc = extracted.get("account_number") or ""
        report["checks"]["account_number"] = float(fuzz.partial_ratio(exp_acc, ex_acc))

    exp_name = expected.get("name")
    if exp_name:
        ex_name = (extracted.get("receiver_name") or "")
        report["checks"]["name"] = float(fuzz.token_set_ratio(exp_name.upper(), ex_name.upper()))

    exp_memo = expected.get("memo")
    if exp_memo:
        ex_memo = (extracted.get("memo") or "")
        report["checks"]["memo"] = float(fuzz.partial_ratio(exp_memo.upper(), ex_memo.upper()))

    if expected.get("date_from") or expected.get("date_to"):
        report["checks"]["datetime_text_present"] = 100.0 if extracted.get("datetime_text") else 0.0

    vals = list(report["checks"].values())
    report["overall"] = sum(vals)/len(vals) if vals else 0.0
    return report

def ocr_extract_text(image_url: str) -> Tuple[str, List[Tuple[str, float]]]:
    """
    Chạy RapidOCR trên ảnh URL, trả (full_text, items[text/conf]).
    Hỗ trợ cả 2 kiểu output của RapidOCR:
      - (result, elapse) với result là list[[box, text, score], ...]
      - (boxes, texts, scores)
    """
    ocr = _get_ocr()
    img_bytes = _fetch_image_bytes(image_url)

    import numpy as np, cv2
    nparr = np.frombuffer(img_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    out = ocr(img)

    lines: List[str] = []
    confs: List[float] = []

    # Kiểu 1: (result, elapse) -> result: list[[box, text, score], ...]
    if isinstance(out, tuple) and len(out) == 2:
        result, _ = out
        if isinstance(result, list) and result:
            # mỗi phần tử: [box, text, score]
            for item in result:
                # Phòng trường hợp lib thay đổi cấu trúc
                if isinstance(item, (list, tuple)) and len(item) >= 3:
                    text = str(item[1]) if item[1] is not None else ""
                    score = float(item[2]) if item[2] is not None else 0.0
                    if text:
                        lines.append(text)
                        confs.append(score)
                # nếu chỉ có box (det-only) thì bỏ qua
        # else: không đọc được gì

    # Kiểu 2: (boxes, texts, scores)
    elif isinstance(out, tuple) and len(out) == 3:
        boxes, texts, scores = out
        if texts:
            lines = [str(t) for t in texts]
            # scores có thể là None hoặc list; normalize về float
            if scores:
                confs = [float(s) if s is not None else 0.0 for s in scores]
            else:
                confs = [0.0] * len(lines)

    # Trường hợp khác: không đúng kiểu kỳ vọng
    else:
        # fallback: không có text
        lines, confs = [], []

    full_text = "\n".join(lines)
    items = list(zip(lines, confs))
    return full_text, items


def verify_image_against_expected(image_url: str, expected: Dict[str, Any]) -> Dict[str, Any]:
    full_text, items = ocr_extract_text(image_url)
    extracted = parse_fields(full_text)
    matched = score_match(extracted, expected or {})

    confs = [c for (_, c) in items if c is not None]
    conf_stats = {
        "avg": (sum(confs)/len(confs)) if confs else None,
        "min": min(confs) if confs else None,
        "max": max(confs) if confs else None,
        "count": len(confs),
    }

    return {
        "ok": True,
        "image_url": image_url,
        "extracted": extracted,
        "match": matched,
        "conf_stats": conf_stats,
        "ocr_items": [
            {"text": t, "conf": c} for (t, c) in items
        ][:100]
    }
