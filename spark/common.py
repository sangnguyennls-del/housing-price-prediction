"""Tiện ích dùng chung cho mọi Spark job.

Gom SparkSession, đường dẫn HDFS và kết nối PostgreSQL về một chỗ, để các job
chỉ còn phần logic nghiệp vụ.
"""

from __future__ import annotations

import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Cấu hình lấy từ biến môi trường (.env nạp qua docker-compose) ──────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "redpanda:9092")
TOPIC_RAW = os.getenv("TOPIC_LISTINGS_RAW", "realestate.listings.raw")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "realestate.alerts")

HDFS = os.getenv("HDFS_NAMENODE", "hdfs://namenode:8020")
BRONZE = f"{HDFS}{os.getenv('HDFS_BRONZE', '/lake/bronze/listings')}"
SILVER = f"{HDFS}{os.getenv('HDFS_SILVER', '/lake/silver/listings')}"
GOLD = f"{HDFS}{os.getenv('HDFS_GOLD', '/lake/gold')}"

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "realestate")
PG_USER = os.getenv("POSTGRES_USER", "reuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "repass")
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_TTL = int(os.getenv("REDIS_TTL_SECONDS", "900"))


def get_spark(app_name: str, shuffle_partitions: int = 8) -> SparkSession:
    """SparkSession trỏ vào cụm standalone.

    shuffle_partitions mặc định 200 là quá lớn cho vài chục nghìn dòng — mỗi
    partition chỉ vài trăm bản ghi, chi phí điều phối lấn át phần tính toán.
    Đặt 8 cho khớp tổng số core của 2 worker.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.hadoop.fs.defaultFS", HDFS)
        # Ghi đè một partition mà không xoá các partition khác
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # KHÔNG suy kiểu cột phân vùng. Silver phân vùng theo province_code, mà
        # mã tỉnh có số 0 đứng đầu ("01" = Hà Nội). Mặc định Spark đọc thư mục
        # province_code=01 rồi suy ra kiểu INT → giá trị thành 1, mất số 0.
        # Hậu quả: tầng Gold ghi area_code="1" trong khi GeoJSON và bảng tham
        # chiếu dùng "01" → bản đồ cấp tỉnh trống trơn, KHÔNG có lỗi nào báo.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )


# ── Schema bản ghi trong Kafka ────────────────────────────────────────────
# Khai báo tường minh thay vì để Spark tự suy: dữ liệu từ Kafka có thể có
# batch toàn giá trị null cho một cột, và suy kiểu sẽ cho ra schema khác nhau
# giữa các lần chạy — dẫn tới lỗi khi ghi vào cùng thư mục Parquet.
LISTING_SCHEMA = StructType([
    StructField("listing_id", StringType()),
    StructField("source", StringType()),
    StructField("crawled_at", StringType()),
    StructField("posted_at", StringType()),
    StructField("url", StringType()),
    StructField("address_raw", StringType()),
    StructField("province_raw", StringType()),
    StructField("district_raw", StringType()),
    StructField("ward_raw", StringType()),
    StructField("area", DoubleType()),
    StructField("frontage", DoubleType()),
    StructField("access_road", DoubleType()),
    StructField("house_direction", StringType()),
    StructField("balcony_direction", StringType()),
    StructField("floors", DoubleType()),
    StructField("bedrooms", DoubleType()),
    StructField("bathrooms", DoubleType()),
    StructField("legal_status", StringType()),
    StructField("furniture_state", StringType()),
    StructField("property_type", StringType()),
    StructField("price", DoubleType()),
    StructField("price_per_m2", DoubleType()),
])


def write_postgres(df: DataFrame, table: str, mode: str = "overwrite") -> int:
    """Ghi DataFrame vào bảng PostgreSQL, trả về số dòng đã ghi.

    mode="overwrite" dùng truncate thay vì DROP TABLE để giữ nguyên schema và
    index đã định nghĩa trong scripts/init_postgres.sql.
    """
    n = df.count()
    writer = df.write.mode(mode).option("truncate", "true").option("batchsize", 5000)
    writer.jdbc(url=PG_URL, table=table, properties=PG_PROPS)
    print(f"  → PostgreSQL.{table}: {n:,} dòng ({mode})")
    return n


# Schema của bảng etl_data_quality. Khai báo tường minh thay vì để Spark suy
# kiểu: các tỷ lệ có thể là None ở một số stage, và suy kiểu từ None sẽ hỏng.
# run_id (BIGSERIAL) và run_at (DEFAULT now()) do PostgreSQL tự điền.
_DQ_SCHEMA = StructType([
    StructField("stage", StringType()),
    StructField("rows_in", LongType()),
    StructField("rows_out", LongType()),
    StructField("rows_dropped", LongType()),
    StructField("province_match_rate", DoubleType()),
    StructField("district_match_rate", DoubleType()),
    StructField("ward_match_rate", DoubleType()),
    StructField("notes", StringType()),
])


def log_data_quality(spark: SparkSession, stage: str, **fields) -> None:
    """Ghi một dòng vào etl_data_quality.

    Không phải để trang trí: tỷ lệ khớp địa chỉ qua từng lần chạy chính là bằng
    chứng định lượng cho mục "Chuẩn hóa địa chỉ" ở Chương 3 báo cáo.

    Dùng API Spark thuần — ảnh Spark cố ý không cài pandas để giữ image nhẹ.
    """
    row = tuple(fields.get(f.name) for f in _DQ_SCHEMA.fields[1:])
    sdf = spark.createDataFrame([(stage, *row)], schema=_DQ_SCHEMA)
    sdf.write.mode("append").jdbc(url=PG_URL, table="etl_data_quality", properties=PG_PROPS)
    summary = " · ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
    print(f"  → etl_data_quality[{stage}]: {summary}")
