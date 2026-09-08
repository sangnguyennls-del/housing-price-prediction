"""Schema chuẩn cho tin rao bất động sản.

Mọi nguồn (Kaggle, Chợ Tốt, batdongsan) đều được ép về cùng một cấu trúc trước
khi đẩy vào Kafka. Nhờ vậy Spark chỉ phải hiểu một schema duy nhất, và thêm
nguồn mới sau này chỉ tốn một hàm `to_canonical`.

Quy ước đơn vị — thống nhất toàn hệ thống, đừng đổi:
    area          m²
    frontage      m (mặt tiền)
    access_road   m (đường vào)
    price         tỷ VND
    price_per_m2  triệu VND/m²
"""

from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

# Thứ tự cột chuẩn. Spark đọc JSON theo tên nên thứ tự không bắt buộc, nhưng
# giữ một danh sách tường minh giúp phát hiện sớm khi một nguồn thiếu trường.
CANONICAL_FIELDS: tuple[str, ...] = (
    "listing_id",
    "source",
    "crawled_at",
    # Thời điểm tin được ĐĂNG, khác hẳn crawled_at (thời điểm ta tải về).
    # Một phiên crawl cho crawled_at giống hệt nhau ở mọi tin, nên không dựng
    # được chuỗi thời gian; posted_at trải suốt kho lưu trữ của site và là
    # nguồn chuỗi thời gian THẬT duy nhất mà hệ thống tự sinh ra được.
    "posted_at",
    "url",
    "address_raw",
    "province_raw",
    "district_raw",
    "ward_raw",
    "area",
    "frontage",
    "access_road",
    "house_direction",
    "balcony_direction",
    "floors",
    "bedrooms",
    "bathrooms",
    "legal_status",
    "furniture_state",
    "property_type",
    "price",
    "price_per_m2",
)

# ── Bí danh cột của dataset Kaggle ────────────────────────────────────────
# Dataset trên Kaggle được cộng đồng chỉnh sửa nhiều lần, tên cột không ổn định
# (có bản tiếng Anh, có bản tiếng Việt, có bản snake_case). Thay vì đoán một
# tên rồi vỡ khi tải về, ta khai báo mọi biến thể đã gặp và khớp không phân
# biệt hoa thường / dấu / gạch dưới.
KAGGLE_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "address_raw": ("address", "dia chi", "diachi", "location", "full address"),
    "area": ("area", "dien tich", "acreage", "size", "area m2", "square"),
    "frontage": ("frontage", "mat tien", "facade", "width"),
    "access_road": ("access road", "duong vao", "road width", "access road width"),
    "house_direction": ("house direction", "huong nha", "direction"),
    "balcony_direction": ("balcony direction", "huong ban cong"),
    "floors": ("floors", "so tang", "number of floors", "n floors", "storeys"),
    "bedrooms": ("bedrooms", "so phong ngu", "n bedrooms", "bedroom", "rooms"),
    "bathrooms": ("bathrooms", "so toilet", "n bathrooms", "bathroom", "toilets", "wc"),
    "legal_status": ("legal status", "phap ly", "legal", "legal document"),
    "furniture_state": ("furniture state", "noi that", "furniture", "interior"),
    "property_type": ("property type", "loai bds", "loai hinh", "type", "house type"),
    "price": ("price", "gia", "gia ban", "price bil", "price billion"),
}


def _norm_key(s: str) -> str:
    """Chuẩn hóa tên cột để so khớp: bỏ dấu, thường hóa, gộp khoảng trắng."""
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("đ", "d").replace("Đ", "d")
    s = re.sub(r"[^a-zA-Z0-9]+", " ", s).strip().lower()
    return re.sub(r"\s+", " ", s)


def resolve_columns(columns: list[str]) -> dict[str, str | None]:
    """Ánh xạ tên cột thật của file CSV sang tên chuẩn.

    Trả về {tên_chuẩn: tên_cột_thật_hoặc_None}. Gọi hàm này ngay sau khi đọc
    CSV để biết nguồn thiếu trường nào, thay vì phát hiện lúc train.
    """
    lookup = {_norm_key(c): c for c in columns}
    resolved: dict[str, str | None] = {}
    for canonical, aliases in KAGGLE_COLUMN_ALIASES.items():
        found = None
        for alias in aliases:
            if alias in lookup:
                found = lookup[alias]
                break
        resolved[canonical] = found
    return resolved


# ── Ép kiểu an toàn ───────────────────────────────────────────────────────
_NUM_RE = re.compile(r"-?\d+(?:[.,]\d+)?")


def to_float(value: Any) -> float | None:
    """Đổi giá trị bất kỳ sang float, trả None nếu không đọc được.

    Chịu được các dạng bẩn thường gặp trong tin rao: "80 m²", "4,5", "12.5 tỷ",
    chuỗi rỗng, "Không xác định", NaN.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return None if isinstance(value, float) and math.isnan(value) else float(value)
    text = str(value).strip()
    if not text or _norm_key(text) in {"nan", "none", "null", "khong xac dinh", "n a"}:
        return None
    m = _NUM_RE.search(text.replace(".", "", text.count(".") - 1) if text.count(".") > 1 else text)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


def to_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or _norm_key(text) in {"nan", "none", "null"}:
        return None
    return text


def make_listing_id(source: str, *parts: Any) -> str:
    """ID ổn định, tái lập được: cùng tin rao luôn cho cùng id.

    Cần thiết vì replay producer có thể chạy nhiều lần — dùng id ngẫu nhiên sẽ
    tạo bản ghi trùng trên data lake.
    """
    raw = "|".join(str(p) for p in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"{source}_{digest}"


def compute_price_per_m2(price_ty: float | None, area_m2: float | None) -> float | None:
    """price (tỷ VND) + area (m²) → đơn giá (triệu VND/m²)."""
    if not price_ty or not area_m2 or area_m2 <= 0:
        return None
    return round(price_ty * 1000.0 / area_m2, 4)


def blank_record(source: str) -> dict[str, Any]:
    rec: dict[str, Any] = {f: None for f in CANONICAL_FIELDS}
    rec["source"] = source
    return rec


def finalize(rec: dict[str, Any]) -> dict[str, Any]:
    """Bước cuối trước khi đẩy vào Kafka: tính đơn giá, đóng dấu thời gian."""
    if rec.get("price_per_m2") is None:
        rec["price_per_m2"] = compute_price_per_m2(rec.get("price"), rec.get("area"))
    if rec.get("crawled_at") is None and rec.get("source") != "kaggle":
        rec["crawled_at"] = datetime.now(timezone.utc).isoformat()
    return rec


# ── Bộ lọc hợp lệ ─────────────────────────────────────────────────────────
# Ngưỡng đặt rộng có chủ ý: ở đây chỉ loại rác không thể dùng được. Việc phát
# hiện giá bất thường tinh vi là nhiệm vụ của bài toán 5 (Isolation Forest),
# không nên làm ở tầng ingestion — lọc quá tay sẽ xoá mất chính thứ ta cần học.
MIN_AREA_M2, MAX_AREA_M2 = 5.0, 10_000.0
MIN_PRICE_TY, MAX_PRICE_TY = 0.05, 1_000.0


def is_usable(rec: dict[str, Any]) -> bool:
    area, price = rec.get("area"), rec.get("price")
    if area is None or not (MIN_AREA_M2 <= area <= MAX_AREA_M2):
        return False
    if price is None or not (MIN_PRICE_TY <= price <= MAX_PRICE_TY):
        return False
    return bool(rec.get("address_raw") or rec.get("district_raw"))


# ── Chuyển đổi theo nguồn ─────────────────────────────────────────────────
def kaggle_row_to_canonical(
    row: dict[str, Any], colmap: dict[str, str | None], row_idx: int
) -> dict[str, Any]:
    """Một dòng DataFrame Kaggle → bản ghi chuẩn."""
    rec = blank_record("kaggle")

    def get(field: str) -> Any:
        col = colmap.get(field)
        return row.get(col) if col else None

    rec["address_raw"] = to_text(get("address_raw"))
    for f in ("area", "frontage", "access_road", "floors", "bedrooms", "bathrooms", "price"):
        rec[f] = to_float(get(f))
    for f in ("house_direction", "balcony_direction", "legal_status", "furniture_state"):
        rec[f] = to_text(get(f))

    # Dataset Kaggle không có mốc thời gian đăng tin — để None một cách tường
    # minh. Đây chính là lý do bài toán dự báo xu hướng phải dùng nguồn khác.
    rec["crawled_at"] = None
    rec["listing_id"] = make_listing_id(
        "kaggle", rec["address_raw"], rec["area"], rec["price"], row_idx
    )
    return finalize(rec)


# Chợ Tốt trả giá bằng VND nguyên; hệ thống quy ước tỷ VND.
VND_PER_TY = 1_000_000_000


def chotot_ad_to_canonical(ad: dict[str, Any]) -> dict[str, Any]:
    """Một quảng cáo từ API Chợ Tốt → bản ghi chuẩn.

    Chỉ lấy trường công khai mô tả bất động sản. Cố tình KHÔNG lấy thông tin
    cá nhân người đăng (tên, số điện thoại) — xem mục đạo đức thu thập dữ liệu
    trong báo cáo.
    """
    rec = blank_record("chotot")

    price_vnd = to_float(ad.get("price"))
    rec["price"] = round(price_vnd / VND_PER_TY, 6) if price_vnd else None
    rec["area"] = to_float(ad.get("size"))
    rec["frontage"] = to_float(ad.get("property_width"))
    rec["access_road"] = to_float(ad.get("property_road_width"))
    rec["floors"] = to_float(ad.get("floors"))
    rec["bedrooms"] = to_float(ad.get("rooms"))
    rec["bathrooms"] = to_float(ad.get("toilets"))
    rec["house_direction"] = to_text(ad.get("direction"))
    rec["legal_status"] = to_text(ad.get("property_legal_document"))
    rec["furniture_state"] = to_text(ad.get("furnishing_status"))

    rec["province_raw"] = to_text(ad.get("region_name"))
    rec["district_raw"] = to_text(ad.get("area_name"))
    rec["ward_raw"] = to_text(ad.get("ward_name"))
    rec["address_raw"] = ", ".join(
        p for p in (rec["ward_raw"], rec["district_raw"], rec["province_raw"]) if p
    ) or to_text(ad.get("address"))

    rec["url"] = f"https://www.nhatot.com/{ad['list_id']}.htm" if ad.get("list_id") else None
    rec["listing_id"] = make_listing_id("chotot", ad.get("list_id"))
    rec["crawled_at"] = datetime.now(timezone.utc).isoformat()
    return finalize(rec)


def demo() -> None:
    """Kiểm tra tối thiểu — chạy: python ingestion/schemas.py"""
    # Khớp tên cột bất kể hoa thường, dấu, gạch dưới
    cols = ["Address", "Area", "Frontage", "Access Road", "Số phòng ngủ", "Price", "Rác"]
    m = resolve_columns(cols)
    assert m["address_raw"] == "Address", m
    assert m["area"] == "Area", m
    assert m["access_road"] == "Access Road", m
    assert m["bedrooms"] == "Số phòng ngủ", m
    assert m["balcony_direction"] is None, m

    # Ép kiểu chịu được dữ liệu bẩn
    assert to_float("80 m²") == 80.0
    assert to_float("4,5") == 4.5
    assert to_float("12.5 tỷ") == 12.5
    assert to_float("") is None
    assert to_float("Không xác định") is None
    assert to_float(float("nan")) is None
    assert to_float(None) is None

    # Đơn giá: 12 tỷ / 80 m² = 150 triệu/m²
    assert compute_price_per_m2(12.0, 80.0) == 150.0
    assert compute_price_per_m2(12.0, 0) is None
    assert compute_price_per_m2(None, 80.0) is None

    # ID ổn định giữa các lần chạy
    a = make_listing_id("kaggle", "Quận 7", 80.0, 12.0, 3)
    b = make_listing_id("kaggle", "Quận 7", 80.0, 12.0, 3)
    assert a == b and a.startswith("kaggle_")
    assert a != make_listing_id("kaggle", "Quận 7", 80.0, 12.0, 4)

    # Chuyển đổi một dòng Kaggle đầy đủ
    row = {"Address": "Quận 7, Hồ Chí Minh", "Area": "80", "Price": "12", "Số phòng ngủ": "3"}
    rec = kaggle_row_to_canonical(row, m, 0)
    assert rec["price_per_m2"] == 150.0 and rec["bedrooms"] == 3.0
    assert rec["crawled_at"] is None, "dữ liệu Kaggle phải không có mốc thời gian"
    assert is_usable(rec)

    # Chợ Tốt: giá VND → tỷ VND
    ad = {"list_id": 123, "price": 12_000_000_000, "size": 80, "rooms": 3,
          "region_name": "Tp Hồ Chí Minh", "area_name": "Quận 7"}
    rec = chotot_ad_to_canonical(ad)
    assert rec["price"] == 12.0 and rec["price_per_m2"] == 150.0
    assert rec["crawled_at"] is not None, "dữ liệu crawl phải có mốc thời gian"
    assert is_usable(rec)

    # Bộ lọc loại rác nhưng giữ lại tin giá cao bất thường (việc của bài toán 5)
    assert not is_usable({**rec, "area": 2.0})
    assert not is_usable({**rec, "price": None})
    assert is_usable({**rec, "price": 500.0}), "tin đắt bất thường vẫn phải đi tiếp"

    print("schemas.py — tất cả kiểm tra đạt")


if __name__ == "__main__":
    demo()
