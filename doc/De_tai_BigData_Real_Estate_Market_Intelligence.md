# Đề tài: Nền tảng Big Data phân tích xu hướng và dự báo giá bất động sản Việt Nam

## 1. Mục tiêu

Xây dựng nền tảng Big Data phục vụ thu thập, lưu trữ, xử lý và phân tích
dữ liệu bất động sản Việt Nam nhằm: - Dự báo giá bất động sản. - Phân
tích xu hướng thị trường. - Hỗ trợ ra quyết định cho người mua, nhà đầu
tư và doanh nghiệp.

------------------------------------------------------------------------

## 2. Các bài toán AI cần phát triển

### 2.1 Dự đoán giá bất động sản

**Mục tiêu** - Dự đoán giá dựa trên diện tích, vị trí, số phòng, pháp
lý...

**Thuật toán** - XGBoost - LightGBM - CatBoost

**Đánh giá** - RMSE - MAE - R² Score

------------------------------------------------------------------------

### 2.2 Heatmap giá bất động sản

Hiển thị:

-   Giá trung bình theo tỉnh/thành
-   Giá theo quận/huyện
-   Giá theo phường

Ví dụ:

    TP.HCM

    Quận 1     ██████████
    Quận 7     ██████
    Thủ Đức    ████

------------------------------------------------------------------------

### 2.3 Phân tích xu hướng thị trường

Ví dụ:

    Quận 9

    2023 ↑
    2024 ↑↑
    2025 ↓

Mô hình: - Prophet - LSTM

------------------------------------------------------------------------

### 2.4 Phân cụm khu vực

Thuật toán: - KMeans

Kết quả: - Luxury Area - Mid Area - Affordable Area

------------------------------------------------------------------------

### 2.5 Phát hiện bất thường

Ví dụ

    Diện tích: 50m²

    Giá: 200 triệu

    ↓

    AI phát hiện:
    Giá bất thường

Thuật toán: - Isolation Forest - Local Outlier Factor

------------------------------------------------------------------------

## 3. Dashboard

Dashboard gồm:

-   Heatmap giá
-   Giá trung bình theo quận
-   Giá theo loại hình
-   Phân bố diện tích
-   Dự báo giá
-   Xu hướng tăng/giảm
-   Top khu vực tăng mạnh
-   Top khu vực giảm mạnh
-   Thống kê theo thời gian

------------------------------------------------------------------------

## 4. Kiến trúc Big Data

``` text
Crawler / Dataset Kaggle
          │
          ▼
      Apache Kafka
          │
          ▼
 Spark SQL / Spark Streaming
          │
          ▼
      HDFS / MinIO
          │
          ▼
 Feature Engineering
          │
          ▼
 Machine Learning
(XGBoost / LightGBM / CatBoost)
          │
          ▼
 Spring Boot REST API
          │
          ▼
 React Dashboard + Map
```

------------------------------------------------------------------------

## 5. Nguồn dữ liệu

### Dữ liệu chính

-   Kaggle Vietnam Housing Dataset
-   Kaggle House Price Dataset

### Dữ liệu bổ sung

-   Dữ liệu rao bán từ batdongsan.vn (nếu tuân thủ điều khoản sử dụng)
-   Dữ liệu từ Chợ Tốt (nếu tuân thủ điều khoản sử dụng)
-   Lãi suất ngân hàng
-   Quy hoạch đô thị
-   Khoảng cách đến trường học, bệnh viện
-   Dữ liệu giao thông
-   Dữ liệu dân số

------------------------------------------------------------------------

## 6. Quy trình phát triển

1.  Thu thập dữ liệu
2.  ETL và làm sạch dữ liệu
3.  Lưu trữ Data Lake
4.  Xử lý bằng Spark
5.  Feature Engineering
6.  Huấn luyện mô hình AI
7.  Đánh giá mô hình
8.  Xây dựng REST API
9.  Phát triển Dashboard
10. Triển khai và demo

------------------------------------------------------------------------

## 7. Giá trị nổi bật

Không chỉ là **House Price Prediction** mà hướng đến **Real Estate
Market Intelligence**:

-   Dự báo giá
-   Phân tích xu hướng
-   Heatmap thị trường
-   Phân cụm khu vực
-   Phát hiện bất thường
-   Dashboard phân tích thời gian thực
-   Kiến trúc Big Data hoàn chỉnh

------------------------------------------------------------------------

## 8. Hướng mở rộng

-   Dự báo thanh khoản bất động sản
-   Phân tích tác động của lãi suất
-   Gợi ý khu vực đầu tư
-   Phân tích bong bóng bất động sản
-   Tích hợp dữ liệu vệ tinh và GIS
-   Triển khai Spark Streaming với dữ liệu cập nhật định kỳ

------------------------------------------------------------------------

## 9. Công nghệ đề xuất

  Thành phần        Công nghệ
  ----------------- --------------------------------------
  Data Collection   Python, Scrapy
  Streaming         Apache Kafka
  Processing        Apache Spark
  Storage           HDFS, MinIO
  Database          PostgreSQL
  AI                XGBoost, LightGBM, CatBoost, Prophet
  Backend           Spring Boot
  Frontend          React
  Visualization     Leaflet/Mapbox, ECharts
  Container         Docker
