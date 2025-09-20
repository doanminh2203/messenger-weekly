# ocr_model.py
import re
from typing import Any, Dict, List, Tuple
from rapidfuzz import fuzz
import requests

# Lazy init EasyOCR reader để không tốn RAM lúc import
_READER = None

def _get_reader():
    global _READER
    if _READER is None:
        import easyocr
        # Tiếng Việt + Anh, chạy CPU
        _READER = easyocr.Reader(['vi', 'en'], gpu=False, verbose=False)
    return _READER

def _fetch_image_bytes(url: str) -> bytes:
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    return r.content

def _vn_to_int_amount(s: str) -> int | None:
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

def _pick_first(patterns: List[str], text: str) -> str | None:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            return (m.group(1) or m.group(0)).strip() if m.groups() else m.group(0).strip()
    return None

def parse_fields(full_text: str) -> Dict[str, Any]:
    """Tách các trường phổ biến trên bill VN từ full OCR text."""
    text = re.sub(r"[ \t]+", " ", full_text)

    amt = _pick_first([
        r"Số tiền[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
        r"Amount[:\s]*([0-9\.\, ]+(?:VND|VNĐ|Đ|D)?)",
        r"([0-9][0-9\.\, ]{3,}VND)",
        r"([0-9][0-9\.\, ]{3,}\s?(?:VNĐ|Đ|D))",
    ], text)
    amt_int = _vn_to_int_amount(amt) if amt else None

    acc = _pick_first([
        r"(?:STK|Số\s*tài\s*khoản|Account\s*No\.?)[:\s]*([0-9\- ]{6,})",
        r"(?:Tài\s*khoản\s*nhận)[:\s]*([0-9\- ]{6,})",
    ], text)
    acc = acc.replace(" ", "").replace("-", "") if acc else None

    name = _pick_first([
        r"(?:Tên\s*người\s*nhận|Người\s*nhận|Beneficiary\s*Name)[:\s]*([A-Za-zÀ-ỹ\s\.\-]{3,})",
        r"(?:Chủ\s*tài\s*khoản)[:\s]*([A-Za-zÀ-ỹ\s\.\-]{3,})",
    ], text)

    memo = _pick_first([
        r"(?:Nội\s*dung|Ghi\s*chú|Content|Description)[:\s]*([^\n]+)",
    ], text)

    when = _pick_first([
        r"(?:Thời\s*gian|Ngày\s*giao\s*dịch|Time|Date)[:\s]*([0-9\/\-\:\s]{8,20})",
        r"(\d{1,2}\/\d{1,2}\/\d{2,4}\s+\d{1,2}:\d{2})",
        r"(\d{4}\-\d{1,2}\-\d{1,2}\s+\d{1,2}:\d{2})",
    ], text)

    tx = _pick_first([
        r"(?:Mã\s*giao\s*dịch|Transaction\s*ID|Ref(?:erence)?)[:\s]*([A-Za-z0-9\-]{6,})",
    ], text)

    return {
        "raw_text": full_text,
        "amount_text": amt,
        "amount": amt_int,
        "account_number": acc,
        "receiver_name": name.strip() if name else None,
        "memo": memo,
        "datetime_text": when,
        "tx_code": tx
    }

def score_match(extracted: Dict[str, Any], expected: Dict[str, Any]) -> Dict[str, Any]:
    """
    So khớp mềm (fuzzy) với kỳ vọng.
    expected: amount(int|str), account_number(str), name(str), memo(str), date_from/date_to(str, optional)
    """
    report = {"checks": {}, "overall": 0.0}

    # Amount
    exp_amt = expected.get("amount")
    if exp_amt is not None:
        if isinstance(exp_amt, str):
            exp_amt = _vn_to_int_amount(exp_amt)
        ok = (extracted.get("amount") == exp_amt)
        report["checks"]["amount"] = 100.0 if ok else 0.0

    # Account number
    exp_acc = expected.get("account_number")
    if exp_acc:
        ex_acc = extracted.get("account_number") or ""
        s = fuzz.partial_ratio(exp_acc, ex_acc)
        report["checks"]["account_number"] = float(s)

    # Name
    exp_name = expected.get("name")
    if exp_name:
        ex_name = (extracted.get("receiver_name") or "")
        s = fuzz.token_set_ratio(exp_name.upper(), ex_name.upper())
        report["checks"]["name"] = float(s)

    # Memo
    exp_memo = expected.get("memo")
    if exp_memo:
        ex_memo = (extracted.get("memo") or "")
        s = fuzz.partial_ratio(exp_memo.upper(), ex_memo.upper())
        report["checks"]["memo"] = float(s)

    # Date window: ở đây chỉ check có trường hay không
    if expected.get("date_from") or expected.get("date_to"):
        report["checks"]["datetime_text_present"] = 100.0 if extracted.get("datetime_text") else 0.0

    vals = list(report["checks"].values())
    report["overall"] = sum(vals)/len(vals) if vals else 0.0
    return report

def ocr_extract_text(image_url: str) -> Tuple[str, List[Tuple]]:
    """Trả về (full_text, list items từ EasyOCR)."""
    img = _fetch_image_bytes(image_url)
    reader = _get_reader()
    results = reader.readtext(img, detail=1, paragraph=True)  # [ [bbox, text, conf], ... ]
    lines = [r[1] for r in results if len(r) >= 2 and isinstance(r[1], str)]
    full_text = "\n".join(lines)
    return full_text, results

def verify_image_against_expected(image_url: str, expected: Dict[str, Any]) -> Dict[str, Any]:
    """Pipeline: OCR → parse → so khớp → trả kết quả."""
    full_text, results = ocr_extract_text(image_url)
    extracted = parse_fields(full_text)
    matched = score_match(extracted, expected or {})
    return {
        "ok": True,
        "image_url": image_url,
        "extracted": extracted,
        "match": matched,
        "ocr_items": [
            {"text": r[1], "conf": float(r[2]) if len(r) >= 3 else None}
            for r in results
        ][:50]
    }
