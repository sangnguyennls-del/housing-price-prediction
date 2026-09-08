-- ═══════════════════════════════════════════════════════════════════════
-- Serving layer — IE221 Đồ án dự đoán giá bất động sản Việt Nam
--
-- Chỉ chứa dữ liệu ĐÃ TỔNG HỢP từ tầng Gold trên HDFS. Dashboard truy vấn
-- ở đây (mili-giây) thay vì đọc trực tiếp data lake.
--
-- MLflow dùng chung database này làm backend store; nó tự tạo bảng riêng
-- với tiền tố khác nên không xung đột.
-- ═══════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────
-- SILVER: tin rao đã làm sạch và chuẩn hóa địa chỉ.
-- Bản sao phục vụ (nguồn gốc vẫn nằm trên HDFS /lake/silver).
-- ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver_listings (
    listing_id        TEXT PRIMARY KEY,
    source            TEXT        NOT NULL,          -- kaggle | chotot | batdongsan
    crawled_at        TIMESTAMPTZ,                   -- NULL với dữ liệu Kaggle (không có mốc thời gian)
    posted_at         TIMESTAMPTZ,                   -- ngày ĐĂNG tin; nguồn chuỗi thời gian cho bài toán 3

    url               TEXT,                          -- link tin gốc (NULL với dữ liệu Kaggle)

    -- Địa chỉ gốc và kết quả chuẩn hóa
    address_raw       TEXT,
    province          TEXT,
    district          TEXT,
    ward              TEXT,
    province_code     TEXT,
    district_code     TEXT,
    ward_code         TEXT,
    geo_match_score   REAL,                          -- điểm fuzzy match 0-100, dùng đo chất lượng dữ liệu
    -- Cách suy ra mã quận. Cần để báo cáo tách kết quả theo nguồn:
    --   old        khớp trực tiếp hệ hành chính cũ (dữ liệu Kaggle 2024)
    --   bridge2025 quy từ phường mới về quận cũ qua bảng sáp nhập 01/07/2025
    --   unique     suy từ tên quận duy nhất toàn quốc
    geo_source        TEXT,

    -- Thuộc tính bất động sản
    area              REAL,
    frontage          REAL,
    access_road       REAL,
    house_direction   TEXT,
    balcony_direction TEXT,
    floors            REAL,
    bedrooms          REAL,
    bathrooms         REAL,
    legal_status      TEXT,
    furniture_state   TEXT,
    property_type     TEXT,                          -- Nhà mặt tiền / trong hẻm / biệt thự ...

    -- Giá (đơn vị: tỷ VND cho price, triệu VND/m2 cho price_per_m2)
    price             REAL,
    price_per_m2      REAL,

    ingested_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_silver_district ON silver_listings (district_code);
CREATE INDEX IF NOT EXISTS ix_silver_province ON silver_listings (province_code);
CREATE INDEX IF NOT EXISTS ix_silver_crawled  ON silver_listings (crawled_at);
CREATE INDEX IF NOT EXISTS ix_silver_posted   ON silver_listings (posted_at);

-- ───────────────────────────────────────────────────────────────────────
-- GOLD 1 — Heatmap giá theo đơn vị hành chính (bài toán 2)
-- level: province | district | ward
-- ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_area_price (
    level             TEXT        NOT NULL,
    area_code         TEXT        NOT NULL,
    area_name         TEXT        NOT NULL,
    parent_code       TEXT,
    n_listings        INTEGER     NOT NULL,
    median_price_m2   REAL,
    mean_price_m2     REAL,
    p25_price_m2      REAL,
    p75_price_m2      REAL,
    median_area       REAL,
    median_price      REAL,
    updated_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (level, area_code)
);

-- ───────────────────────────────────────────────────────────────────────
-- GOLD 2a — Đặc trưng cấp quận, đầu vào cho phân cụm (bài toán 4)
-- Tính bằng Spark trên toàn bộ Silver rồi vật chất hóa ở đây, để bước phân
-- cụm không phải lặp lại logic tổng hợp bằng pandas — hai bản sẽ trôi lệch.
-- ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_district_features (
    district_code     TEXT PRIMARY KEY,
    district          TEXT        NOT NULL,
    province          TEXT,
    n_listings        INTEGER     NOT NULL,
    median_price_m2   REAL,
    mean_area         REAL,
    mean_floors       REAL,
    pct_red_book      REAL,                          -- tỷ lệ tin có sổ đỏ
    pct_mat_tien      REAL,                          -- tỷ lệ tin đường vào >= 4m
    updated_at        TIMESTAMPTZ DEFAULT now()
);

-- ───────────────────────────────────────────────────────────────────────
-- GOLD 2b — Phân cụm khu vực (bài toán 4)
-- ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_area_cluster (
    area_code         TEXT PRIMARY KEY,
    area_name         TEXT        NOT NULL,
    province_name     TEXT,
    cluster_id        INTEGER     NOT NULL,
    cluster_label     TEXT        NOT NULL,          -- Luxury | Mid | Affordable | Emerging
    median_price_m2   REAL,
    mean_area         REAL,
    listing_density   REAL,
    pct_red_book      REAL,
    pct_mat_tien      REAL,
    pca_x             REAL,                          -- toạ độ PCA 2D để vẽ scatter trong báo cáo
    pca_y             REAL,
    updated_at        TIMESTAMPTZ DEFAULT now()
);

-- ───────────────────────────────────────────────────────────────────────
-- GOLD 3 — Chuỗi giá lịch sử và dự báo (bài toán 3)
-- ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_price_history (
    area_code         TEXT        NOT NULL,
    area_name         TEXT        NOT NULL,
    ds                DATE        NOT NULL,
    price_m2          REAL        NOT NULL,
    source            TEXT        NOT NULL,          -- kaggle_hcm | crawler_accumulated | bds_index
    PRIMARY KEY (area_code, ds, source)
);

CREATE TABLE IF NOT EXISTS gold_price_forecast (
    area_code         TEXT        NOT NULL,
    area_name         TEXT        NOT NULL,
    model_name        TEXT        NOT NULL,          -- prophet | lstm
    ds                DATE        NOT NULL,
    yhat              REAL        NOT NULL,
    yhat_lower        REAL,
    yhat_upper        REAL,
    is_forecast       BOOLEAN     NOT NULL DEFAULT TRUE,
    generated_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (area_code, model_name, ds)
);

-- Kết quả backtest cuốn chiếu — đưa thẳng vào bảng so sánh trong báo cáo
CREATE TABLE IF NOT EXISTS gold_forecast_metrics (
    area_code         TEXT        NOT NULL,
    model_name        TEXT        NOT NULL,
    horizon_months    INTEGER     NOT NULL,
    mape              REAL,
    rmse              REAL,
    mae               REAL,
    n_folds           INTEGER,
    generated_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (area_code, model_name, horizon_months)
);

-- ───────────────────────────────────────────────────────────────────────
-- GOLD 4 — Tin rao bất thường (bài toán 5)
-- ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_anomaly (
    listing_id        TEXT PRIMARY KEY,
    district_code     TEXT,
    district          TEXT,
    price             REAL,
    price_per_m2      REAL,
    predicted_price_m2 REAL,
    residual_ratio    REAL,                          -- |thực tế - dự đoán| / dự đoán
    iforest_score     REAL,
    lof_score         REAL,
    anomaly_score     REAL        NOT NULL,          -- điểm tổng hợp 0-100
    reason            TEXT,                          -- diễn giải cho người dùng
    detected_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_anomaly_score ON gold_anomaly (anomaly_score DESC);

-- ───────────────────────────────────────────────────────────────────────
-- GOLD 5 — Chỉ số tổng quan cho thẻ KPI trên dashboard
-- ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_market_summary (
    metric_key        TEXT PRIMARY KEY,
    metric_value      DOUBLE PRECISION,
    metric_text       TEXT,
    updated_at        TIMESTAMPTZ DEFAULT now()
);

-- ───────────────────────────────────────────────────────────────────────
-- Chất lượng dữ liệu — theo dõi tỷ lệ khớp địa chỉ qua từng lần chạy ETL.
-- Đây là bằng chứng định lượng cho mục "Chuẩn hóa địa chỉ" ở Chương 3.
-- ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_data_quality (
    run_id            BIGSERIAL PRIMARY KEY,
    run_at            TIMESTAMPTZ DEFAULT now(),
    stage             TEXT        NOT NULL,          -- bronze_to_silver | silver_to_gold
    rows_in           BIGINT,
    rows_out          BIGINT,
    rows_dropped      BIGINT,
    province_match_rate REAL,
    district_match_rate REAL,
    ward_match_rate     REAL,
    notes             TEXT
);
