"""Hot path — Spark Structured Streaming: Kafka → Redis.

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        /app/spark/streaming_hot_path.py --once      # chạy hết dữ liệu rồi dừng
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        /app/spark/streaming_hot_path.py             # chạy thường trú

Đây là nhánh chứng minh chữ V thứ hai — VELOCITY. Cold path trả lời "thị trường
đang ở đâu"; hot path trả lời "vừa có gì thay đổi", trong vài giây.

Hai truy vấn chạy song song trên cùng một topic:

  A. VẬN TỐC ĐĂNG TIN — cửa sổ trượt 1 giờ, bước 5 phút, nhóm theo quận.
     Đếm tin mới và đơn giá trung bình trong cửa sổ. Ghi vào Redis dưới dạng
     hash + sorted set để dashboard đọc bằng một lệnh.

  B. CẢNH BÁO TIN BẤT THƯỜNG — mỗi tin mới được đối chiếu ngay với mặt bằng
     giá của quận (lấy từ tầng Gold, join tĩnh). Lệch quá ngưỡng thì đẩy cảnh
     báo vào Redis và vào topic Kafka `realestate.alerts`.

Vì sao tách làm hai truy vấn thay vì một: Structured Streaming không cho phép
nhiều phép tổng hợp nối tiếp trong cùng một luồng. Truy vấn B không tổng hợp
(chỉ join tĩnh + lọc) nên độ trễ của nó chỉ là thời gian micro-batch, không
phải chờ cửa sổ đóng — đúng thứ cần cho cảnh báo.

DÙNG TRUNG BÌNH chứ không phải trung vị ở đây, ngược với cold path. Không phải
tùy tiện: trạng thái của một phép tổng hợp streaming phải cập nhật được tăng
dần, mà trung vị thì không — Spark không hỗ trợ percentile trong streaming
aggregation. Trung vị vẫn là con số chính thức, tính ở cold path; trung bình
trong cửa sổ chỉ để bắt xu hướng ngắn hạn.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (  # noqa: E402
    HDFS,
    KAFKA_BOOTSTRAP,
    LISTING_SCHEMA,
    PG_PROPS,
    PG_URL,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_TTL,
    TOPIC_ALERTS,
    TOPIC_RAW,
    get_spark,
)
from udf_address import register_udf  # noqa: E402

CHECKPOINT = f"{HDFS}/lake/checkpoints/hot_path"

# Cửa sổ 1 giờ trượt 5 phút: đủ dài để có mẫu, đủ ngắn để thấy biến động trong
# ngày. Mỗi tin rơi vào 12 cửa sổ chồng nhau — chi phí chấp nhận được ở quy mô
# vài chục nghìn tin.
WINDOW = "1 hour"
SLIDE = "5 minutes"

# Tin đến muộn quá 2 giờ bị bỏ. Ngưỡng này giới hạn kích thước trạng thái
# streaming; không có nó, Spark phải giữ mọi cửa sổ trong bộ nhớ vĩnh viễn.
WATERMARK = "2 hours"

# Lệch bao nhiêu so với trung vị quận thì coi là đáng cảnh báo. 60% là ngưỡng
# rộng có chủ ý: hot path chỉ để lọc thô cho người xem, còn chấm điểm bất
# thường nghiêm túc là việc của ml/train_anomaly.py (có mô hình dư).
ALERT_DEVIATION = 0.60

MAX_ALERTS_KEPT = 200      # số cảnh báo giữ trong Redis


def redis_client():
    """Kết nối Redis. Import cục bộ để driver không cần redis khi --no-redis."""
    import redis

    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def load_baseline(spark) -> DataFrame | None:
    """Mặt bằng giá từng quận, lấy từ tầng Gold trong PostgreSQL.

    Trả về None nếu Gold chưa có dữ liệu — hot path vẫn chạy được, chỉ là chưa
    có gì để đối chiếu. Không cho phép cả pipeline chết vì cold path chưa chạy.
    """
    try:
        gold = spark.read.jdbc(url=PG_URL, table="gold_area_price", properties=PG_PROPS)
        base = (gold.filter(F.col("level") == "district")
                    .select(F.col("area_code").alias("district_code"),
                            F.col("median_price_m2").alias("base_price_m2")))
        n = base.count()
        if n == 0:
            print("⚠️  gold_area_price chưa có dữ liệu cấp quận — bỏ qua cảnh báo.")
            return None
        print(f"Mặt bằng giá đối chiếu: {n:,} quận")
        return base.cache()
    except Exception as e:                                    # noqa: BLE001
        print(f"⚠️  Không đọc được gold_area_price ({type(e).__name__}) — bỏ qua cảnh báo.")
        return None


def kafka_source(spark, topic: str, bootstrap: str, starting: str) -> DataFrame:
    """Luồng Kafka đã parse JSON + gắn thời gian sự kiện.

    Thời gian sự kiện lấy từ `crawled_at` của tin, KHÔNG phải thời gian Kafka
    nhận được. Khi replay lịch sử, hai mốc này lệch nhau hàng tháng; dùng nhầm
    thì mọi tin rơi vào cùng một cửa sổ và biểu đồ vận tốc thành vô nghĩa.
    """
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", starting)
           # Chặn một micro-batch nuốt trọn 30k tin replay: giữ batch nhỏ để
           # thấy được độ trễ thật, và để trạng thái cửa sổ không phình.
           .option("maxOffsetsPerTrigger", 2000)
           .load())

    return (raw
            .select(F.from_json(F.col("value").cast("string"), LISTING_SCHEMA).alias("d"),
                    F.col("timestamp").alias("kafka_ts"))
            .select("d.*", "kafka_ts")
            .filter(F.col("listing_id").isNotNull())
            .withColumn("event_time",
                        F.coalesce(F.to_timestamp("crawled_at"), F.col("kafka_ts")))
            .withColumn("price_per_m2",
                        F.when(F.col("price_per_m2").isNotNull(), F.col("price_per_m2"))
                         .otherwise(F.col("price") * 1000.0 / F.col("area"))))


# ══════════════════════════════════════════════════════════════════════
# Truy vấn A — vận tốc đăng tin theo cửa sổ trượt
# ══════════════════════════════════════════════════════════════════════
def build_velocity(stream: DataFrame, geo_udf) -> DataFrame:
    enriched = (stream
                .withColumn("_g", geo_udf(F.col("address_raw"), F.col("province_raw"),
                                          F.col("district_raw"), F.col("ward_raw")))
                .select("*", "_g.district_code", "_g.district")
                .drop("_g")
                .filter(F.col("district_code").isNotNull()))

    return (enriched
            .withWatermark("event_time", WATERMARK)
            .groupBy(F.window("event_time", WINDOW, SLIDE), "district_code", "district")
            .agg(F.count("*").alias("n_listings"),
                 F.avg("price_per_m2").alias("avg_price_m2"),
                 F.min("price_per_m2").alias("min_price_m2"),
                 F.max("price_per_m2").alias("max_price_m2"))
            .select(F.col("window.start").alias("win_start"),
                    F.col("window.end").alias("win_end"),
                    "district_code", "district", "n_listings",
                    "avg_price_m2", "min_price_m2", "max_price_m2"))


def sink_velocity(batch: DataFrame, batch_id: int) -> None:
    """Ghi kết quả cửa sổ vào Redis.

    Chỉ giữ cửa sổ MỚI NHẤT của mỗi quận. Dashboard cần biết "bây giờ thế nào",
    không cần lịch sử — lịch sử là việc của PostgreSQL.
    """
    rows = batch.collect()                    # ~85 quận × vài cửa sổ, an toàn
    if not rows:
        return

    latest: dict[str, dict] = {}
    for r in rows:
        d = r["district_code"]
        if d not in latest or r["win_end"] > latest[d]["win_end"]:
            latest[d] = r.asDict()

    r = redis_client()
    pipe = r.pipeline()
    for code, row in latest.items():
        key = f"hot:velocity:{code}"
        pipe.hset(key, mapping={
            "district": row["district"] or "",
            "win_start": str(row["win_start"]),
            "win_end": str(row["win_end"]),
            "n_listings": int(row["n_listings"]),
            "avg_price_m2": round(float(row["avg_price_m2"] or 0), 2),
            "min_price_m2": round(float(row["min_price_m2"] or 0), 2),
            "max_price_m2": round(float(row["max_price_m2"] or 0), 2),
        })
        pipe.expire(key, REDIS_TTL)
        # Sorted set để dashboard lấy top quận sôi động bằng MỘT lệnh ZREVRANGE
        # thay vì quét toàn bộ key hot:velocity:*
        pipe.zadd("hot:top_districts", {f"{code}|{row['district']}": int(row["n_listings"])})
    pipe.expire("hot:top_districts", REDIS_TTL)
    pipe.hset("hot:meta", mapping={"last_batch": batch_id,
                                   "n_districts": len(latest)})
    pipe.execute()
    print(f"  [velocity] batch {batch_id}: {len(latest)} quận → Redis")


# ══════════════════════════════════════════════════════════════════════
# Truy vấn B — cảnh báo tin lệch giá, độ trễ bằng một micro-batch
# ══════════════════════════════════════════════════════════════════════
def build_alerts(stream: DataFrame, geo_udf, baseline: DataFrame) -> DataFrame:
    enriched = (stream
                .withColumn("_g", geo_udf(F.col("address_raw"), F.col("province_raw"),
                                          F.col("district_raw"), F.col("ward_raw")))
                .select("*", "_g.district_code", "_g.district")
                .drop("_g")
                .filter(F.col("district_code").isNotNull()))

    # Join luồng với bảng TĨNH — không cần watermark, không giữ trạng thái.
    joined = (enriched.join(F.broadcast(baseline), on="district_code", how="inner")
              .withColumn("deviation",
                          (F.col("price_per_m2") - F.col("base_price_m2"))
                          / F.col("base_price_m2")))

    return (joined
            .filter(F.abs(F.col("deviation")) >= F.lit(ALERT_DEVIATION))
            .select("listing_id", "source", "url", "district_code", "district",
                    "address_raw", "area", "price", "price_per_m2",
                    "base_price_m2", "deviation", "event_time")
            .withColumn("severity",
                        F.when(F.abs(F.col("deviation")) >= 1.5, "cao")
                         .when(F.abs(F.col("deviation")) >= 1.0, "trung binh")
                         .otherwise("thap"))
            .withColumn("reason",
                        F.when(F.col("deviation") < 0,
                               F.concat(F.lit("Rẻ hơn mặt bằng quận "),
                                        F.round(F.abs(F.col("deviation")) * 100, 0).cast("int").cast("string"),
                                        F.lit("% — nghi giá mồi hoặc pháp lý xấu")))
                         .otherwise(
                               F.concat(F.lit("Đắt hơn mặt bằng quận "),
                                        F.round(F.col("deviation") * 100, 0).cast("int").cast("string"),
                                        F.lit("% — kiểm tra lại diện tích/đơn vị giá")))))


def sink_alerts(batch: DataFrame, batch_id: int, bootstrap: str,
                topic: str) -> None:
    rows = batch.limit(MAX_ALERTS_KEPT).collect()
    if not rows:
        return

    r = redis_client()
    pipe = r.pipeline()
    for row in rows:
        d = row.asDict()
        d["event_time"] = str(d["event_time"])
        for k in ("area", "price", "price_per_m2", "base_price_m2", "deviation"):
            d[k] = round(float(d[k]), 3) if d[k] is not None else None
        pipe.lpush("hot:alerts", json.dumps(d, ensure_ascii=False))
    # Cắt danh sách để Redis không phình vô hạn — đây là speed view, không
    # phải kho lưu trữ.
    pipe.ltrim("hot:alerts", 0, MAX_ALERTS_KEPT - 1)
    pipe.expire("hot:alerts", REDIS_TTL)
    pipe.incrby("hot:alerts:total", len(rows))
    pipe.execute()

    # Đẩy sang topic Kafka riêng: hệ thống khác (email, Telegram bot) có thể
    # tiêu thụ mà không cần biết gì về Redis hay Spark.
    (batch.select(F.col("listing_id").alias("key"),
                  F.to_json(F.struct("*")).alias("value"))
          .write.format("kafka")
          .option("kafka.bootstrap.servers", bootstrap)
          .option("topic", topic)
          .save())
    print(f"  [alerts]   batch {batch_id}: {len(rows)} cảnh báo → Redis + Kafka")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--topic", default=TOPIC_RAW)
    ap.add_argument("--alert-topic", default=TOPIC_ALERTS)
    ap.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    ap.add_argument("--starting-offsets", default="latest",
                    help='"latest" chờ tin mới; "earliest" replay toàn bộ topic')
    ap.add_argument("--once", action="store_true",
                    help="xử lý hết dữ liệu sẵn có rồi dừng (dùng để kiểm chứng)")
    ap.add_argument("--checkpoint", default=CHECKPOINT)
    ap.add_argument("--no-alerts", action="store_true")
    args = ap.parse_args()

    spark = get_spark("hot-path")
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile(str(Path(__file__).resolve().parent / "udf_address.py"))

    geo_udf = register_udf(spark)
    stream = kafka_source(spark, args.topic, args.bootstrap, args.starting_offsets)

    trigger = {"availableNow": True} if args.once else {"processingTime": "10 seconds"}
    queries = []

    q_vel = (build_velocity(stream, geo_udf).writeStream
             .outputMode("update")
             .foreachBatch(sink_velocity)
             .option("checkpointLocation", f"{args.checkpoint}/velocity")
             .trigger(**trigger)
             .start())
    queries.append(q_vel)

    baseline = None if args.no_alerts else load_baseline(spark)
    if baseline is not None:
        q_alert = (build_alerts(stream, geo_udf, baseline).writeStream
                   .outputMode("append")
                   .foreachBatch(lambda b, i: sink_alerts(b, i, args.bootstrap,
                                                          args.alert_topic))
                   .option("checkpointLocation", f"{args.checkpoint}/alerts")
                   .trigger(**trigger)
                   .start())
        queries.append(q_alert)

    print(f"\nHot path đang chạy — {len(queries)} truy vấn · "
          f"cửa sổ {WINDOW}/{SLIDE} · watermark {WATERMARK}")
    print(f"  Kiểm tra:  docker compose exec redis redis-cli KEYS 'hot:*'")

    if args.once:
        for q in queries:
            q.awaitTermination()
        print("\nĐã xử lý hết dữ liệu sẵn có.")
    else:
        spark.streams.awaitAnyTermination()

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
