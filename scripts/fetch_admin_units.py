"""Tải danh mục đơn vị hành chính Việt Nam về data/geo/admin_units.json.

    python scripts/fetch_admin_units.py

Đây là bảng tham chiếu cho bộ chuẩn hóa địa chỉ (spark/udf_address.py). Không
có nó thì không map được "Q.7" → mã quận, và cả heatmap lẫn đặc trưng vị trí
đều hỏng.

GHI CHÚ VỀ HỆ QUY CHIẾU HÀNH CHÍNH — cần nêu trong báo cáo:
Việt Nam sáp nhập đơn vị hành chính cấp tỉnh/xã năm 2025. Dataset Kaggle 2024
dùng tên quận/huyện CŨ. Nguồn API này cũng phục vụ cấu trúc trước sáp nhập, nên
hai bên khớp nhau. Đồ án cố ý giữ hệ quy chiếu cũ để đảm bảo tính nhất quán
giữa dữ liệu và bảng tham chiếu; điều này được ghi rõ ở mục "Giới hạn đề tài".
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

# depth=3 trả về đủ 3 cấp: tỉnh → quận/huyện → phường/xã
PRIMARY_URL = "https://provinces.open-api.vn/api/v1/?depth=3"
FALLBACK_URL = "https://raw.githubusercontent.com/kenzouno1/DiaGioiHanhChinhVN/master/data.json"
OUT = Path("data/geo/admin_units.json")


def fetch(url: str, timeout: int = 120) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "UIT-IE221-Academic/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def from_open_api(data: list) -> list[dict]:
    """Chuẩn hóa cấu trúc của provinces.open-api.vn."""
    out = []
    for p in data:
        out.append({
            "code": str(p["code"]).zfill(2),
            "name": p["name"],
            "districts": [
                {
                    "code": str(d["code"]).zfill(3),
                    "name": d["name"],
                    "wards": [
                        {"code": str(w["code"]).zfill(5), "name": w["name"]}
                        for w in d.get("wards", [])
                    ],
                }
                for d in p.get("districts", [])
            ],
        })
    return out


def from_kenzouno(data: list) -> list[dict]:
    """Chuẩn hóa cấu trúc của kho DiaGioiHanhChinhVN (khóa tiếng Việt)."""
    out = []
    for p in data:
        out.append({
            "code": str(p.get("Id", "")).zfill(2),
            "name": p.get("Name", ""),
            "districts": [
                {
                    "code": str(d.get("Id", "")).zfill(3),
                    "name": d.get("Name", ""),
                    "wards": [
                        {"code": str(w.get("Id", "")).zfill(5), "name": w.get("Name", "")}
                        for w in d.get("Wards", [])
                    ],
                }
                for d in p.get("Districts", [])
            ],
        })
    return out


def main() -> int:
    provinces = None
    for url, parser in ((PRIMARY_URL, from_open_api), (FALLBACK_URL, from_kenzouno)):
        try:
            print(f"Đang tải: {url}")
            provinces = parser(json.loads(fetch(url)))
            if provinces:
                break
        except Exception as exc:                      # noqa: BLE001 — muốn thử nguồn kế tiếp
            print(f"  thất bại: {exc}")

    if not provinces:
        print("Không tải được danh mục hành chính từ cả hai nguồn.", file=sys.stderr)
        return 1

    n_d = sum(len(p["districts"]) for p in provinces)
    n_w = sum(len(d["wards"]) for p in provinces for d in p["districts"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(provinces, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"Đã ghi {OUT}")
    print(f"  {len(provinces)} tỉnh/thành · {n_d} quận/huyện · {n_w} phường/xã")
    if len(provinces) < 60:
        print("  ⚠️  Số tỉnh < 60 — nguồn có thể đã đổi sang cấu trúc sau sáp nhập, kiểm tra lại.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
