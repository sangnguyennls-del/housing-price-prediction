"""Đẩy dataset tin rao (CSV) vào Kafka.

Chạy trong container ML (host dùng Python 3.14, chưa có wheel cho client Kafka):

    docker compose exec ml python ingestion/replay_producer.py --limit 1000
    docker compose exec ml python ingestion/replay_producer.py --rate 50   # giả lập luồng

Hai chế độ:

* **bootstrap** (mặc định) — nạp toàn bộ lịch sử nhanh nhất có thể. Dùng để mồi
  data lake trước khi huấn luyện.
* **--rate N** — phát N bản ghi/giây, giả lập tin rao về theo thời gian thực để
  demo hot path khi crawler chưa chạy.

Producer KHÔNG chuẩn hóa địa chỉ. Đó là việc của Spark ở tầng Bronze→Silver:
Kafka phải giữ dữ liệu càng gần nguyên bản càng tốt để còn xử lý lại được khi
logic làm sạch thay đổi.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schemas import is_usable, kaggle_row_to_canonical, resolve_columns  # noqa: E402

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "redpanda:9092")
DEFAULT_TOPIC = os.getenv("TOPIC_LISTINGS_RAW", "realestate.listings.raw")


def survey(df: pd.DataFrame, colmap: dict[str, str | None]) -> None:
    """In khảo sát dataset — bước bắt buộc trước khi tin vào bất cứ con số nào.

    Tên cột trên Kaggle không ổn định giữa các bản; hàm này cho thấy ngay cột
    nào khớp được, cột nào thiếu.
    """
    print("=" * 70)
    print(f"KHẢO SÁT DATASET — {len(df):,} dòng × {len(df.columns)} cột")
    print("=" * 70)
    print(f"Cột trong file: {list(df.columns)}\n")
    print("Ánh xạ sang schema chuẩn:")
    for canonical, actual in colmap.items():
        mark = "✓" if actual else "✗ THIẾU"
        print(f"  {mark:8} {canonical:20} ← {actual or '(không tìm thấy)'}")

    missing = [k for k, v in colmap.items() if v is None]
    if missing:
        print(f"\n⚠️  Thiếu {len(missing)} trường: {', '.join(missing)}")
        print("   Nếu dataset thật có tên cột khác, bổ sung vào KAGGLE_COLUMN_ALIASES")
        print("   trong ingestion/schemas.py — đừng sửa rải rác ở nơi khác.")

    for field in ("area", "price"):
        col = colmap.get(field)
        if col:
            s = pd.to_numeric(df[col], errors="coerce")
            print(f"\n{field} ({col}): thiếu {s.isna().sum():,} · "
                  f"min={s.min():.2f} · trung vị={s.median():.2f} · max={s.max():.2f}")
    print("=" * 70)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default="data/raw/sample_vietnam_housing.csv",
                    help="đường dẫn CSV nguồn")
    ap.add_argument("--limit", type=int, default=0, help="chỉ gửi N dòng đầu (0 = tất cả)")
    ap.add_argument("--rate", type=float, default=0.0,
                    help="bản ghi/giây; 0 = gửi nhanh nhất có thể")
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--survey-only", action="store_true",
                    help="chỉ khảo sát dataset, không gửi Kafka")
    args = ap.parse_args()

    path = Path(args.csv)
    if not path.exists():
        print(f"Không thấy {path}", file=sys.stderr)
        print("Tạo dữ liệu mẫu:  python scripts/make_sample_data.py", file=sys.stderr)
        print("Hoặc tải dữ liệu thật: xem scripts/download_data.py", file=sys.stderr)
        return 1

    df = pd.read_csv(path)
    colmap = resolve_columns(list(df.columns))
    survey(df, colmap)

    if colmap.get("price") is None or colmap.get("area") is None:
        print("\nThiếu cột giá hoặc diện tích — không thể tiếp tục.", file=sys.stderr)
        return 1
    if args.survey_only:
        return 0

    if args.limit:
        df = df.head(args.limit)

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=50,
        acks="all",              # không mất bản ghi khi broker đổi leader
        retries=5,
    )

    sent = skipped = 0
    delay = 1.0 / args.rate if args.rate > 0 else 0.0
    t0 = time.time()

    for idx, row in enumerate(df.to_dict("records")):
        rec = kaggle_row_to_canonical(row, colmap, idx)
        if not is_usable(rec):
            skipped += 1
            continue
        # Khóa = listing_id ⇒ cùng một tin luôn vào cùng partition, giữ được
        # thứ tự cập nhật của tin đó.
        producer.send(args.topic, key=rec["listing_id"], value=rec)
        sent += 1
        if delay:
            time.sleep(delay)
        elif sent % 2000 == 0:
            print(f"  đã gửi {sent:,}...")

    producer.flush()
    producer.close()

    dt = time.time() - t0
    print(f"\nĐã gửi {sent:,} bản ghi → topic '{args.topic}' trong {dt:.1f}s "
          f"({sent / dt:.0f}/s)")
    print(f"Bỏ qua {skipped:,} dòng không hợp lệ (thiếu giá/diện tích/địa chỉ)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
