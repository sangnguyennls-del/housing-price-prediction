"""Xây cầu nối hành chính 2025: phường/xã MỚI → quận/huyện CŨ.

    python scripts/build_admin_bridge_2025.py

═══════════════════════════════════════════════════════════════════════════
VÌ SAO CẦN — đây là phát hiện quan trọng của đồ án, phải viết vào Chương 3.

Ngày 01/07/2025 Việt Nam bỏ cấp huyện, gộp 63 tỉnh còn 34, và gộp 2/3 số
phường/xã. Hệ quả trực tiếp cho đồ án:

  Dataset Kaggle 2024   → địa chỉ theo hệ CŨ:  "Quận 7, TP.HCM"      (3 cấp)
  Tin rao crawl 2026    → địa chỉ theo hệ MỚI: "Phường Tân Thuận, TP.HCM" (2 cấp)

Hai nguồn không quy chiếu về cùng một không gian. Đo thực tế trên tin crawl:
chỉ 33% khớp được cấp quận, và trong số khớp có ca SAI NGHIÊM TRỌNG — "Phường
Thanh Xuân" (nội thành) bị gán về "Huyện Sóc Sơn" (ngoại thành, cách 40km) chỉ
vì trùng tên với một xã cũ. Khớp sai âm thầm nguy hiểm hơn không khớp.

Giải pháp: dùng bảng ánh xạ chính thức dựng từ Nghị quyết 202/2025/QH15 và
1685/NQ-UBTVQH15, quy tin rao mới về hệ CŨ để thống nhất với Kaggle.

HẠN CHẾ phải nêu trong báo cáo: 9,5% phường mới được ghép từ các phường cũ
thuộc NHIỀU quận cũ khác nhau. Với nhóm này ta lấy quận chiếm đa số và ghi lại
độ tin cậy; ánh xạ là xấp xỉ, không phải song ánh.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import json
import re
import sys
import unicodedata
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

# Dữ liệu cộng đồng dựng từ các nghị quyết sáp nhập chính thức. Phải trích dẫn
# nguồn này trong TÀI LIỆU THAM KHẢO — nó không phải API của cơ quan nhà nước.
CONVERTER_URL = (
    "https://raw.githubusercontent.com/tranngocminhhieu/vietnamadminunits/"
    "main/vietnamadminunits/data/converter_2025.json"
)
ADMIN_UNITS = Path("data/geo/admin_units.json")
OUT = Path("data/geo/ward2025_to_old.json")


def key_name(s: str) -> str:
    """Tên → khóa dạng 'thanhphohanoi' (bỏ dấu, bỏ mọi ký tự không chữ số).

    Đây đúng là quy ước khóa mà converter_2025.json dùng.
    """
    s = s.replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]", "", s.lower())


def build_new_ward_to_old_districts(conv: dict) -> dict[str, dict[str, Counter]]:
    """{tỉnh mới: {phường mới: Counter(khóa quận cũ)}}.

    Gộp hai bảng của converter theo hai chiều ngược nhau:
      NO_DIVIDED  phường mới → [phường cũ]              (đã đúng chiều)
      DIVIDED     phường cũ  → [phường mới]             (phải đảo)
    Phường cũ bị tách sang nhiều phường mới sẽ đóng góp cho từng phường mới đó.
    """
    out: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))

    for prov, wards in conv["DICT_PROVINCE_WARD_NO_DIVIDED"].items():
        for new_ward, old_keys in wards.items():
            for k in old_keys:
                parts = k.split("_")
                # Khóa dạng "tinhx_quany_phuongz"; huyện đảo có phần quận rỗng
                if len(parts) >= 2 and parts[1]:
                    out[prov][new_ward][f"{parts[0]}|{parts[1]}"] += 1

    for prov, old_map in conv["DICT_PROVINCE_WARD_DIVIDED"].items():
        for old_key, new_wards in old_map.items():
            parts = old_key.split("_")
            if len(parts) < 2 or not parts[1]:
                continue
            old_district = f"{parts[0]}|{parts[1]}"
            for nw in new_wards:
                out[prov][nw["newWardKey"]][old_district] += 1

    return out


def index_old_units(provinces: list[dict]) -> dict[str, tuple[str, str, str, str]]:
    """{'tinhx|quany': (mã tỉnh, tên tỉnh, mã quận, tên quận)} theo hệ CŨ."""
    idx = {}
    for p in provinces:
        pk = key_name(p["name"])
        for d in p["districts"]:
            idx[f"{pk}|{key_name(d['name'])}"] = (p["code"], p["name"], d["code"], d["name"])
    return idx


def main() -> int:
    if not ADMIN_UNITS.exists():
        print(f"Thiếu {ADMIN_UNITS}. Chạy trước: python scripts/fetch_admin_units.py",
              file=sys.stderr)
        return 1

    print(f"Đang tải bảng ánh xạ: {CONVERTER_URL}")
    req = urllib.request.Request(CONVERTER_URL, headers={"User-Agent": "UIT-IE221-Academic/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        conv = json.loads(r.read())

    old_provinces = json.loads(ADMIN_UNITS.read_text(encoding="utf-8"))
    old_idx = index_old_units(old_provinces)
    mapping = build_new_ward_to_old_districts(conv)

    bridge: dict[str, dict[str, dict]] = {}
    n_total = n_ambiguous = n_unresolved = 0

    for new_prov, wards in mapping.items():
        entries = {}
        for new_ward, counter in wards.items():
            n_total += 1
            (top_key, top_n), = counter.most_common(1)
            total = sum(counter.values())
            hit = old_idx.get(top_key)
            if not hit:
                # Khóa quận cũ không tra được trong admin_units.json — bỏ qua
                # thay vì đoán. Thà thiếu còn hơn sai.
                n_unresolved += 1
                continue
            if len(counter) > 1:
                n_ambiguous += 1
            p_code, p_name, d_code, d_name = hit
            entries[new_ward] = {
                "province_code": p_code,
                "province": p_name,
                "district_code": d_code,
                "district": d_name,
                # Tỷ lệ phường cũ thuộc quận được chọn. 1.0 = ánh xạ chắc chắn.
                "confidence": round(top_n / total, 3),
                "n_candidates": len(counter),
            }
        if entries:
            bridge[new_prov] = entries

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(bridge, ensure_ascii=False, indent=1), encoding="utf-8")

    n_kept = sum(len(v) for v in bridge.values())
    print(f"\nĐã ghi {OUT}")
    print(f"  {len(bridge)} tỉnh mới · {n_kept:,} phường/xã mới ánh xạ được")
    print(f"  trải nhiều quận cũ : {n_ambiguous:,} ({n_ambiguous / n_total:.1%}) → lấy quận đa số")
    print(f"  không tra được     : {n_unresolved:,} ({n_unresolved / n_total:.1%}) → bỏ, không đoán")

    # Kiểm tra trên chính các ca đã phát hiện sai trước đó
    hn = bridge.get("thanhphohanoi", {})
    checks = [
        ("phuongthanhxuan", "Thanh Xuân"),   # trước đây bị gán nhầm sang Sóc Sơn
        ("phuongochodua", "Đống Đa"),
        ("phuongdaimo", "Nam Từ Liêm"),
        ("phuongyenhoa", "Cầu Giấy"),
        ("phuongnghiado", "Cầu Giấy"),
    ]
    print("\nKiểm tra các ca từng sai:")
    for ward, expect in checks:
        got = hn.get(ward)
        status = "✓" if got and expect in got["district"] else "✗"
        detail = f"{got['district']} (tin cậy {got['confidence']})" if got else "KHÔNG CÓ"
        print(f"  {status} {ward:20} → {detail}")
        assert got and expect in got["district"], f"{ward} phải ra {expect}, được {detail}"

    print("\nCầu nối sẵn sàng.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
