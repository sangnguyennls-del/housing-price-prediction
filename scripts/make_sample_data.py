"""Sinh dữ liệu tin rao GIẢ LẬP để kiểm thử pipeline.

    python scripts/make_sample_data.py --rows 5000

⚠️  KHÔNG dùng số liệu từ file này cho kết quả trong báo cáo. Mục đích duy nhất
là chứng minh hạ tầng chạy thông (Kafka → Spark → HDFS → Postgres) trước khi có
dataset Kaggle thật. Cột được đặt trùng tên với dataset Kaggle nên khi có dữ
liệu thật chỉ cần đổi đường dẫn, không phải sửa code.

Dữ liệu được cố tình làm "bẩn" giống thực tế: địa chỉ viết tắt lung tung, thiếu
dấu, ô trống, và một ít tin giá bất thường — để bộ chuẩn hóa địa chỉ và bài
toán phát hiện bất thường có thứ thật sự để xử lý.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from pathlib import Path

# Mặt bằng giá tham khảo (triệu VND/m²) — dùng để sinh phân phối, không phải
# số liệu thị trường chính thức.
DISTRICTS: list[tuple[str, str, float]] = [
    # (tỉnh/thành, quận/huyện, đơn giá trung vị tham chiếu)
    ("Hồ Chí Minh", "Quận 1", 420.0),
    ("Hồ Chí Minh", "Quận 3", 350.0),
    ("Hồ Chí Minh", "Quận 7", 180.0),
    ("Hồ Chí Minh", "Quận 10", 250.0),
    ("Hồ Chí Minh", "Bình Thạnh", 190.0),
    ("Hồ Chí Minh", "Tân Bình", 175.0),
    ("Hồ Chí Minh", "Gò Vấp", 140.0),
    ("Hồ Chí Minh", "Thủ Đức", 110.0),
    ("Hồ Chí Minh", "Bình Tân", 95.0),
    ("Hồ Chí Minh", "Nhà Bè", 70.0),
    ("Hà Nội", "Ba Đình", 380.0),
    ("Hà Nội", "Hoàn Kiếm", 480.0),
    ("Hà Nội", "Đống Đa", 300.0),
    ("Hà Nội", "Cầu Giấy", 260.0),
    ("Hà Nội", "Thanh Xuân", 220.0),
    ("Hà Nội", "Hai Bà Trưng", 280.0),
    ("Hà Nội", "Hoàng Mai", 150.0),
    ("Hà Nội", "Long Biên", 130.0),
    ("Hà Nội", "Hà Đông", 120.0),
    ("Đà Nẵng", "Hải Châu", 160.0),
    ("Đà Nẵng", "Thanh Khê", 110.0),
    ("Đà Nẵng", "Sơn Trà", 130.0),
    ("Đà Nẵng", "Ngũ Hành Sơn", 100.0),
    ("Bình Dương", "Thủ Dầu Một", 60.0),
    ("Bình Dương", "Dĩ An", 55.0),
    ("Đồng Nai", "Biên Hòa", 50.0),
]

WARDS = ["Phường 1", "Phường 2", "Phường 5", "Phường 12", "Phường Tân Phong",
         "Phường Bến Nghé", "Phường Thảo Điền", "Phường Trung Hòa", "Phường Dịch Vọng"]

STREETS = ["Nguyễn Huệ", "Lê Lợi", "Trần Hưng Đạo", "Nguyễn Văn Linh", "Phạm Văn Đồng",
           "Điện Biên Phủ", "Cách Mạng Tháng 8", "Xô Viết Nghệ Tĩnh", "Hoàng Diệu",
           "Nguyễn Thị Minh Khai", "Lạc Long Quân", "Trường Chinh"]

DIRECTIONS = ["Đông", "Tây", "Nam", "Bắc", "Đông Bắc", "Đông Nam", "Tây Bắc", "Tây Nam"]
LEGAL = ["Sổ đỏ", "Sổ hồng", "Hợp đồng mua bán", "Đang chờ sổ", "Giấy tờ hợp lệ"]
FURNITURE = ["Đầy đủ", "Cơ bản", "Cao cấp", "Bàn giao thô", "Không nội thất"]


def messy_district(name: str) -> str:
    """Viết tên quận theo kiểu người đăng tin thật sự gõ.

    Đây là thứ bộ chuẩn hóa địa chỉ phải xử lý được. Nếu dữ liệu mẫu quá sạch,
    ta sẽ tưởng module hoạt động tốt rồi vỡ khi gặp dữ liệu thật.
    """
    if name.startswith("Quận "):
        num = name.removeprefix("Quận ")
        return random.choice([name, f"Q.{num}", f"Q{num}", f"quan {num}", f"Quan {num}"])
    return random.choice([name, name.lower(), f"Q. {name}", f"Quận {name}",
                          name.replace("ậ", "a").replace("ầ", "a")])


def messy_province(name: str) -> str:
    variants = {
        "Hồ Chí Minh": ["Hồ Chí Minh", "TP.HCM", "TPHCM", "Tp Hồ Chí Minh", "Ho Chi Minh", "HCM"],
        "Hà Nội": ["Hà Nội", "Hanoi", "Ha Noi", "TP Hà Nội", "HN"],
        "Đà Nẵng": ["Đà Nẵng", "Da Nang", "TP Đà Nẵng"],
        "Bình Dương": ["Bình Dương", "Binh Duong"],
        "Đồng Nai": ["Đồng Nai", "Dong Nai"],
    }
    return random.choice(variants.get(name, [name]))


def maybe(value, missing_rate: float):
    """Trả None theo xác suất — mô phỏng ô trống trong tin rao thật."""
    return None if random.random() < missing_rate else value


def generate(n: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    random.seed(seed)
    rows = []

    for i in range(n):
        province, district, base_price_m2 = rng.choice(DISTRICTS)

        # Diện tích: log-normal, đa số 40-120 m², đuôi dài lên vài trăm
        area = round(min(rng.lognormvariate(4.3, 0.55), 900.0), 1)

        # Đơn giá: quanh mặt bằng quận, nhiễu log-normal.
        # Nhà nhỏ trong hẻm thường có đơn giá cao hơn nhà lớn cùng khu.
        size_effect = (80.0 / max(area, 20.0)) ** 0.12
        price_m2 = base_price_m2 * size_effect * rng.lognormvariate(0.0, 0.28)

        # Mặt tiền và đường vào ảnh hưởng mạnh tới giá ở Việt Nam
        access_road = round(rng.choice([2, 2.5, 3, 4, 5, 6, 8, 10, 12]) + rng.random(), 1)
        frontage = round(max(2.0, rng.gauss(area ** 0.5 * 0.55, 1.2)), 1)
        if access_road >= 8:
            price_m2 *= 1.25          # mặt tiền đường lớn
        elif access_road <= 3:
            price_m2 *= 0.85          # hẻm nhỏ

        legal = rng.choices(LEGAL, weights=[40, 30, 15, 10, 5])[0]
        if legal in ("Đang chờ sổ", "Hợp đồng mua bán"):
            price_m2 *= 0.88          # pháp lý chưa hoàn chỉnh làm giảm giá

        floors = rng.choices([1, 2, 3, 4, 5, 6], weights=[15, 30, 28, 15, 8, 4])[0]
        bedrooms = max(1, min(8, int(rng.gauss(area / 28.0, 1.0))))
        bathrooms = max(1, min(bedrooms, int(bedrooms * rng.uniform(0.6, 1.0)) + 1))

        # ~2% tin bất thường: giá lệch hẳn mặt bằng (giá mồi / tin ảo / gõ nhầm
        # đơn vị). Bài toán 5 phải bắt được nhóm này.
        is_anomaly = rng.random() < 0.02
        if is_anomaly:
            price_m2 *= rng.choice([0.12, 0.2, 4.5, 7.0])

        price_ty = round(price_m2 * area / 1000.0, 3)

        addr_parts = [
            f"{rng.randint(1, 400)} {rng.choice(STREETS)}",
            maybe(rng.choice(WARDS), 0.35),
            messy_district(district),
            messy_province(province),
        ]
        address = ", ".join(p for p in addr_parts if p)

        rows.append({
            "_is_anomaly": int(is_anomaly),   # nhãn thật, KHÔNG ghi vào CSV dữ liệu
            "Address": address,
            "Area": area,
            "Frontage": maybe(frontage, 0.22),
            "Access Road": maybe(access_road, 0.28),
            "House direction": maybe(rng.choice(DIRECTIONS), 0.45),
            "Balcony direction": maybe(rng.choice(DIRECTIONS), 0.60),
            "Floors": maybe(floors, 0.08),
            "Bedrooms": maybe(bedrooms, 0.06),
            "Bathrooms": maybe(bathrooms, 0.18),
            "Legal status": maybe(legal, 0.10),
            "Furniture state": maybe(rng.choice(FURNITURE), 0.40),
            "Price": price_ty,
        })

    return rows


def write_labels(rows: list[dict], path: Path) -> None:
    """Ghi listing_id → is_anomaly.

    listing_id phải tính GIỐNG HỆT cách replay_producer tính, nếu không sẽ
    không ghép được nhãn với kết quả dự đoán. Vì vậy dùng chung đúng hàm
    make_listing_id thay vì chép lại công thức.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
    from schemas import make_listing_id

    path.parent.mkdir(parents=True, exist_ok=True)
    n_anom = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["listing_id", "is_anomaly"])
        for idx, r in enumerate(rows):
            lid = make_listing_id("kaggle", r["Address"], r["Area"], r["Price"], idx)
            w.writerow([lid, r["_is_anomaly"]])
            n_anom += r["_is_anomaly"]
    print(f"Đã ghi {len(rows):,} nhãn → {path}  ({n_anom} tin bất thường, "
          f"{n_anom / len(rows):.1%})")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/raw/sample_vietnam_housing.csv")
    ap.add_argument("--labels", default="data/raw/sample_labels.csv",
                    help="file nhãn bất thường thật, dùng để CHẤM ĐIỂM bài toán 5")
    args = ap.parse_args()

    rows = generate(args.rows, args.seed)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    data_cols = [c for c in rows[0] if not c.startswith("_")]
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=data_cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r[k] is None else r[k]) for k in data_cols})

    # Nhãn ghi ra FILE RIÊNG, không bao giờ đi vào pipeline. Nhờ vậy bài toán 5
    # chấm điểm được bằng precision/recall thật thay vì "xem bằng mắt", mà mô
    # hình vẫn không hề nhìn thấy đáp án.
    write_labels(rows, Path(args.labels))

    prices = sorted(r["Price"] for r in rows)
    print(f"Đã ghi {len(rows):,} dòng → {out}")
    print(f"  Giá  (tỷ VND): min={prices[0]:.2f}  trung vị={prices[len(prices)//2]:.2f}  max={prices[-1]:.2f}")
    print("  ⚠️  Dữ liệu giả lập — chỉ dùng để kiểm thử pipeline, không đưa vào báo cáo.")


if __name__ == "__main__":
    main()
