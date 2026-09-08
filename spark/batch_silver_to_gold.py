"""Cold path, bước 3 — Silver → Gold.

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        /app/spark/batch_silver_to_gold.py

Tầng Gold là dữ liệu đã tổng hợp sẵn cho dashboard. Kết quả ghi song song ra
HDFS (nguồn chân lý, để huấn luyện mô hình đọc) và PostgreSQL (serving layer,
để dashboard truy vấn nhanh).

Dùng TRUNG VỊ chứ không phải trung bình cho mọi chỉ số giá. Phân phối giá bất
động sản lệch phải rất mạnh: vài căn biệt thự trăm tỷ sẽ kéo trung bình của cả
quận lên sai lệch. Trung bình vẫn được tính và lưu, nhưng chỉ để đối chiếu.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import GOLD, SILVER, get_spark, log_data_quality, write_postgres  # noqa: E402

# Số tin tối thiểu để một đơn vị hành chính được đưa lên heatmap. Dưới ngưỡng
# này trung vị quá nhiễu, tô màu lên bản đồ sẽ gây hiểu sai. Ngưỡng phải được
# nêu rõ trong báo cáo.
MIN_LISTINGS = 30


def aggregate_level(df: DataFrame, level: str, code_col: str,
                    name_col: str, parent_col: str | None) -> DataFrame:
    """Tổng hợp chỉ số giá cho một cấp hành chính."""
    group = [code_col, name_col] + ([parent_col] if parent_col else [])
    agg = (df.filter(F.col(code_col).isNotNull())
             .groupBy(*group)
             .agg(
                 F.count("*").alias("n_listings"),
                 F.expr("percentile_approx(price_per_m2, 0.5)").alias("median_price_m2"),
                 F.avg("price_per_m2").alias("mean_price_m2"),
                 F.expr("percentile_approx(price_per_m2, 0.25)").alias("p25_price_m2"),
                 F.expr("percentile_approx(price_per_m2, 0.75)").alias("p75_price_m2"),
                 F.expr("percentile_approx(area, 0.5)").alias("median_area"),
                 F.expr("percentile_approx(price, 0.5)").alias("median_price"),
             )
             .withColumn("level", F.lit(level))
             .withColumnRenamed(code_col, "area_code")
             .withColumnRenamed(name_col, "area_name"))

    agg = (agg.withColumnRenamed(parent_col, "parent_code") if parent_col
           else agg.withColumn("parent_code", F.lit(None).cast("string")))

    return agg.select("level", "area_code", "area_name", "parent_code", "n_listings",
                      "median_price_m2", "mean_price_m2", "p25_price_m2",
                      "p75_price_m2", "median_area", "median_price")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default=SILVER)
    ap.add_argument("--output", default=GOLD)
    ap.add_argument("--min-listings", type=int, default=MIN_LISTINGS)
    ap.add_argument("--no-postgres", action="store_true")
    args = ap.parse_args()

    spark = get_spark("silver-to-gold")
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(args.input).cache()
    n_in = silver.count()
    print(f"Silver: {n_in:,} bản ghi")

    # ── Heatmap giá theo 3 cấp hành chính (bài toán 2) ────────────────
    parts = [
        aggregate_level(silver, "province", "province_code", "province", None),
        aggregate_level(silver, "district", "district_code", "district", "province_code"),
        aggregate_level(silver, "ward", "ward_code", "ward", "district_code"),
    ]
    area_price = parts[0].unionByName(parts[1]).unionByName(parts[2])

    # Đơn vị quá ít tin bị loại khỏi heatmap nhưng KHÔNG bị xoá khỏi Silver —
    # mô hình vẫn học được từ chúng.
    shown = area_price.filter(F.col("n_listings") >= args.min_listings)
    n_all, n_shown = area_price.count(), shown.count()
    print(f"Đơn vị hành chính: {n_all:,} tổng · {n_shown:,} đủ ≥{args.min_listings} tin để lên bản đồ")

    (shown.write.mode("overwrite").parquet(f"{args.output}/area_price"))

    # ── Đặc trưng cấp quận cho phân cụm (bài toán 4) ───────────────────
    # Tính ở đây thay vì trong ml/train_cluster.py: Spark làm tổng hợp trên
    # toàn bộ dữ liệu nhanh hơn, và kết quả tái dùng được cho dashboard.
    district_feat = (
        silver.filter(F.col("district_code").isNotNull())
        .groupBy("district_code", "district", "province")
        .agg(
            F.count("*").alias("n_listings"),
            F.expr("percentile_approx(price_per_m2, 0.5)").alias("median_price_m2"),
            F.avg("area").alias("mean_area"),
            F.avg("floors").alias("mean_floors"),
            # Nhận cả tiếng Việt lẫn tiếng Anh: dataset Kaggle ghi
            # "Have certificate", crawler ghi "Sổ đỏ / Sổ hồng". Chỉ bắt tiếng
            # Việt thì tỷ lệ này bằng 0 tuyệt đối trên dữ liệu Kaggle — và
            # bước sàng lọc ở train_cluster.py sẽ loại luôn đặc trưng.
            F.avg(F.when(F.col("legal_status").rlike(
                       "(?i)sổ đỏ|sổ hồng|so do|so hong|have certificate|certificate"), 1.0)
                   .when(F.col("legal_status").isNull(), None)
                   .otherwise(0.0)).alias("pct_red_book"),
            F.avg(F.when(F.col("access_road") >= 4.0, 1.0)
                   .when(F.col("access_road").isNull(), None)
                   .otherwise(0.0)).alias("pct_mat_tien"),
        )
        .filter(F.col("n_listings") >= args.min_listings)
    )
    (district_feat.write.mode("overwrite").parquet(f"{args.output}/district_features"))
    print(f"Đặc trưng quận: {district_feat.count():,} quận")
    if not args.no_postgres:
        write_postgres(district_feat, "gold_district_features", mode="overwrite")

    # ── Chỉ số tổng quan cho thẻ KPI ──────────────────────────────────
    stats = silver.select(
        F.count("*").alias("n"),
        F.expr("percentile_approx(price_per_m2, 0.5)").alias("med_m2"),
        F.expr("percentile_approx(price, 0.5)").alias("med_price"),
        F.expr("percentile_approx(area, 0.5)").alias("med_area"),
        F.countDistinct("district_code").alias("n_districts"),
        F.countDistinct("province_code").alias("n_provinces"),
    ).first()

    summary_rows = [
        ("total_listings", float(stats["n"]), f"{stats['n']:,} tin rao"),
        ("median_price_m2", float(stats["med_m2"] or 0), "triệu VND/m²"),
        ("median_price", float(stats["med_price"] or 0), "tỷ VND"),
        ("median_area", float(stats["med_area"] or 0), "m²"),
        ("n_districts", float(stats["n_districts"]), "quận/huyện có dữ liệu"),
        ("n_provinces", float(stats["n_provinces"]), "tỉnh/thành có dữ liệu"),
    ]
    summary = spark.createDataFrame(summary_rows, ["metric_key", "metric_value", "metric_text"])

    print("\nTổng quan thị trường:")
    for k, v, t in summary_rows:
        print(f"  {k:20} {v:12,.2f}  {t}")

    if not args.no_postgres:
        write_postgres(shown, "gold_area_price", mode="overwrite")
        write_postgres(summary, "gold_market_summary", mode="overwrite")
        log_data_quality(spark, "silver_to_gold",
                         rows_in=n_in, rows_out=n_shown, rows_dropped=n_all - n_shown,
                         notes=f"nguong toi thieu={args.min_listings} tin")

    silver.unpersist()
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
