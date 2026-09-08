"""Cold path, bước 2 — Bronze → Silver.

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        /app/spark/batch_bronze_to_silver.py

Tầng Silver là dữ liệu đã sạch, đã khử trùng lặp và ĐÃ CHUẨN HÓA ĐỊA CHỈ.
Đây là bước quyết định chất lượng của toàn bộ phần sau: mô hình giá, heatmap
và phân cụm đều dựa vào mã đơn vị hành chính sinh ra ở đây.

Nguyên tắc lọc: chỉ loại dữ liệu KHÔNG THỂ DÙNG (thiếu giá/diện tích, giá trị
phi vật lý). Tuyệt đối không loại tin "giá trông có vẻ sai" — phát hiện giá bất
thường là nhiệm vụ của bài toán 5; lọc ở đây sẽ xoá mất chính thứ cần học.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import BRONZE, SILVER, get_spark, log_data_quality, write_postgres  # noqa: E402
from udf_address import register_udf  # noqa: E402

# Ngưỡng vật lý — rộng có chủ ý. Đơn giá thực tế ở Việt Nam trải từ ~2 triệu/m²
# (đất vùng ven) tới ~1.500 triệu/m² (mặt tiền Quận 1). Ngưỡng dưới đây chỉ để
# bắt lỗi nhập liệu (nhầm đơn vị đồng/triệu/tỷ), không phải để lọc tin đắt.
MIN_PRICE_M2, MAX_PRICE_M2 = 1.0, 5_000.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default=BRONZE)
    ap.add_argument("--output", default=SILVER)
    ap.add_argument("--no-postgres", action="store_true",
                    help="chỉ ghi HDFS, bỏ qua serving layer")
    args = ap.parse_args()

    spark = get_spark("bronze-to-silver")
    spark.sparkContext.setLogLevel("WARN")
    # Ship module chuẩn hóa sang executor (bảng tham chiếu lấy theo
    # ADMIN_UNITS_PATH, xem docker-compose)
    spark.sparkContext.addPyFile(str(Path(__file__).resolve().parent / "udf_address.py"))

    bronze = spark.read.parquet(args.input)
    n_in = bronze.count()
    print(f"Bronze: {n_in:,} bản ghi")

    # ── Khử trùng lặp ─────────────────────────────────────────────────
    # Cùng listing_id có thể xuất hiện nhiều lần: replay chạy lại, hoặc crawler
    # gặp lại tin cũ với chỉ số mới. Giữ bản ghi mới nhất.
    latest = Window.partitionBy("listing_id").orderBy(F.col("ingested_at").desc())
    dedup = (bronze
             .withColumn("_rn", F.row_number().over(latest))
             .filter(F.col("_rn") == 1)
             .drop("_rn", "topic", "partition", "offset", "ingest_date"))
    n_dedup = dedup.count()
    print(f"Sau khử trùng lặp: {n_dedup:,} (loại {n_in - n_dedup:,} bản trùng)")

    # ── Chuẩn hóa địa chỉ ─────────────────────────────────────────────
    geo_udf = register_udf(spark)
    enriched = (dedup
                .withColumn("_geo", geo_udf(
                    F.col("address_raw"), F.col("province_raw"),
                    F.col("district_raw"), F.col("ward_raw")))
                .select("*", "_geo.*")
                .drop("_geo"))

    # ── Tính lại đơn giá và ép kiểu thời gian ─────────────────────────
    silver = (enriched
              .withColumn("price_per_m2", F.when(
                  F.col("price_per_m2").isNotNull(), F.col("price_per_m2"))
                  .otherwise(F.col("price") * 1000.0 / F.col("area")))
              .withColumn("crawled_at", F.to_timestamp("crawled_at"))
              .withColumn("posted_at", F.to_timestamp("posted_at")))

    # ── Lọc dữ liệu không dùng được ───────────────────────────────────
    before = silver.count()
    silver = silver.filter(
        F.col("area").isNotNull() & (F.col("area") > 0)
        & F.col("price").isNotNull() & (F.col("price") > 0)
        & F.col("price_per_m2").between(MIN_PRICE_M2, MAX_PRICE_M2)
    )
    n_out = silver.count()
    print(f"Sau lọc: {n_out:,} (loại {before - n_out:,} bản ghi phi vật lý)")

    # ── Chất lượng chuẩn hóa địa chỉ ──────────────────────────────────
    q = silver.select(
        F.avg(F.col("province_code").isNotNull().cast("double")).alias("p"),
        F.avg(F.col("district_code").isNotNull().cast("double")).alias("d"),
        F.avg(F.col("ward_code").isNotNull().cast("double")).alias("w"),
        F.avg("geo_match_score").alias("s"),
    ).first()

    print("\nTỷ lệ khớp địa chỉ:")
    print(f"  tỉnh/thành : {q['p']:6.2%}")
    print(f"  quận/huyện : {q['d']:6.2%}   ← ngưỡng chấp nhận > 90%")
    print(f"  phường/xã  : {q['w']:6.2%}")
    print(f"  điểm khớp trung bình: {q['s']:.1f}/100")
    if q["d"] < 0.90:
        print("  ⚠️  Dưới ngưỡng — bổ sung biến thể vào _ABBREV trong udf_address.py")

    # Tách theo cách suy ra mã quận. Con số này đi thẳng vào Chương 3 báo cáo:
    # nó cho biết bao nhiêu tin rao dùng hệ hành chính mới (sau sáp nhập 2025)
    # và phải quy đổi qua cầu nối.
    print("")
    print("Nguồn mã quận:")
    src_rows = (silver.groupBy("geo_source").count()
                .orderBy(F.col("count").desc()).collect())
    for r in src_rows:
        label = r["geo_source"] or "(chưa khớp)"
        print(f"  {label:14} {r['count']:>8,}  {r['count'] / n_out:6.2%}")
    n_bridge = sum(r["count"] for r in src_rows if r["geo_source"] == "bridge2025")

    # ── Ghi Silver ────────────────────────────────────────────────────
    cols = ["listing_id", "source", "crawled_at", "posted_at", "url", "address_raw",
            "province", "district", "ward", "province_code", "district_code",
            "ward_code", "geo_match_score", "geo_source", "area", "frontage", "access_road",
            "house_direction", "balcony_direction", "floors", "bedrooms",
            "bathrooms", "legal_status", "furniture_state", "property_type", "price",
            "price_per_m2", "ingested_at"]
    out = silver.select(*cols)

    # Phân vùng theo tỉnh: hầu hết truy vấn phân tích đều lọc theo địa bàn,
    # partition pruning bỏ qua được phần lớn dữ liệu.
    (out.write.mode("overwrite")
        .partitionBy("province_code")
        .parquet(args.output))
    print(f"\nSilver: {n_out:,} bản ghi → {args.output}")

    if not args.no_postgres:
        write_postgres(out, "silver_listings", mode="overwrite")
        log_data_quality(
            spark, "bronze_to_silver",
            rows_in=n_in, rows_out=n_out, rows_dropped=n_in - n_out,
            province_match_rate=float(q["p"]),
            district_match_rate=float(q["d"]),
            ward_match_rate=float(q["w"]),
            notes=f"diem khop TB={q['s']:.1f}; qua cau noi 2025={n_bridge}",
        )

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
