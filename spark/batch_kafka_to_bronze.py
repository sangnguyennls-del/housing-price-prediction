"""Cold path, bước 1 — Kafka → HDFS Bronze.

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        /app/spark/batch_kafka_to_bronze.py

Tầng Bronze giữ dữ liệu gần như NGUYÊN BẢN: chỉ thêm metadata thu nạp, không
làm sạch, không chuẩn hóa, không loại bỏ gì. Đây là điều kiện để xử lý lại
(reprocess) toàn bộ lịch sử khi logic làm sạch ở tầng Silver thay đổi — và
logic làm sạch chắc chắn sẽ thay đổi.

Job đọc Kafka ở chế độ batch (earliest → latest) chứ không streaming. Streaming
là việc của hot path; cold path chạy định kỳ theo lịch nên batch phù hợp hơn và
dễ chạy lại khi lỗi.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import BRONZE, KAFKA_BOOTSTRAP, LISTING_SCHEMA, TOPIC_RAW, get_spark  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--topic", default=TOPIC_RAW)
    ap.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    ap.add_argument("--output", default=BRONZE)
    ap.add_argument("--starting-offsets", default="earliest",
                    help='"earliest" nạp lại toàn bộ; JSON offset để nạp tiếp')
    args = ap.parse_args()

    spark = get_spark("bronze-ingest")
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("endingOffsets", "latest")
        .load()
    )

    n_raw = raw.count()
    if n_raw == 0:
        print(f"Topic '{args.topic}' rỗng — chạy replay_producer.py trước.")
        spark.stop()
        return 1

    bronze = (
        raw.select(
            F.from_json(F.col("value").cast("string"), LISTING_SCHEMA).alias("d"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("d.*", "topic", "partition", "offset", "kafka_timestamp")
        .withColumn("ingested_at", F.current_timestamp())
        # Phân vùng theo ngày nạp: cho phép nạp thêm hằng ngày mà không phải
        # đọc lại toàn bộ, và job Silver có thể chỉ xử lý phần mới.
        .withColumn("ingest_date", F.to_date("ingested_at"))
    )

    # Bản ghi hỏng (JSON không parse được) → listing_id null. Đếm và loại,
    # nhưng KHÔNG im lặng: số này phải xuất hiện trong log.
    n_bad = bronze.filter(F.col("listing_id").isNull()).count()
    bronze = bronze.filter(F.col("listing_id").isNotNull())

    (bronze.write.mode("append")
        .partitionBy("ingest_date")
        .parquet(args.output))

    n_ok = bronze.count()
    print(f"\nBronze: {n_ok:,} bản ghi → {args.output}")
    if n_bad:
        print(f"  ⚠️  {n_bad:,} bản ghi không parse được JSON, đã loại")
    print(f"  đọc từ Kafka: {n_raw:,} message")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
