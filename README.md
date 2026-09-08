# Nền tảng Big Data phân tích và dự báo giá bất động sản Việt Nam

Đồ án môn **IE221 – Công nghệ Dữ liệu lớn**, Trường Đại học Công nghệ Thông tin – ĐHQG TP.HCM.

Hệ thống thu thập, xử lý và phân tích tin rao bất động sản Việt Nam trên nền tảng
Kafka + Spark + HDFS, tích hợp học máy để dự đoán giá, phân cụm khu vực, dự báo xu
hướng và phát hiện tin rao bất thường.

---

## Khởi động nhanh

```bash
cp .env.example .env

# 1. Hạ tầng (Kafka, HDFS, Spark, PostgreSQL, Redis, MinIO, MLflow)
docker compose up -d
docker compose ps                      # tất cả phải healthy

# 2. Dữ liệu tham chiếu hành chính (bắt buộc — xem mục "Hai hệ quy chiếu")
python scripts/fetch_admin_units.py
python scripts/build_admin_bridge_2025.py

# 3. Dữ liệu
python scripts/download_data.py        # xem còn thiếu dataset nào
python scripts/make_sample_data.py     # hoặc sinh dữ liệu giả lập để chạy thử

# 4. Nạp vào Kafka
docker compose exec ml python ingestion/replay_producer.py \
    --csv data/raw/sample_vietnam_housing.csv

# 5. Cold path: Kafka → Bronze → Silver → Gold
docker compose exec spark-master /opt/spark/bin/spark-submit /app/spark/batch_kafka_to_bronze.py
docker compose exec spark-master /opt/spark/bin/spark-submit /app/spark/batch_bronze_to_silver.py
docker compose exec spark-master /opt/spark/bin/spark-submit /app/spark/batch_silver_to_gold.py

# 6. Huấn luyện
docker compose exec ml python ml/train_price.py --stage all
docker compose exec ml python ml/train_cluster.py
docker compose exec ml python ml/train_anomaly.py

# 7. Hot path — Velocity (Kafka → Spark Streaming → Redis)
docker compose exec spark-master /opt/spark/bin/spark-submit     /app/spark/streaming_hot_path.py --once --starting-offsets earliest
docker compose exec redis redis-cli KEYS 'hot:*'

# 8. Ranh giới hành chính cho bản đồ
python scripts/fetch_geojson.py

# 9. Tầng ứng dụng (FastAPI + Spring Boot + React)
docker compose --profile apps up -d --build
# → http://localhost:5173

# 10. Báo cáo Word
pip install python-docx
python scripts/build_report.py --figures
```

> Trên Git Bash (Windows), thêm `MSYS_NO_PATHCONV=1` trước lệnh `docker` có đường
> dẫn POSIX, nếu không đường dẫn `/opt/spark/...` sẽ bị chuyển thành đường dẫn
> Windows và container báo "no such file".

## Giao diện web

| Dịch vụ | Địa chỉ | Dùng để |
|---|---|---|
| Spark Master | http://localhost:8081 | Ảnh chụp 2 worker chạy song song cho báo cáo |
| HDFS NameNode | http://localhost:9870 | Duyệt 3 tầng Bronze/Silver/Gold |
| Redpanda Console | http://localhost:8080 | Xem topic và message Kafka |
| MLflow | http://localhost:5000 | So sánh 4 giai đoạn thực nghiệm |
| MinIO Console | http://localhost:9001 | Artifact của MLflow (`minioadmin` / `minioadmin123`) |
| **Dashboard** | http://localhost:5173 | Bản đồ giá, định giá, phân cụm, cảnh báo thời gian thực |
| API Gateway | http://localhost:8090/api/health | Spring Boot — gộp PostgreSQL + Redis + FastAPI |
| API mô hình | http://localhost:8000/docs | FastAPI — OpenAPI tự sinh |

> **Trước khi demo hoặc quay video**: chạy lại hot path. Speed view trong Redis có
> `TTL = 900s`, nên nếu job streaming chạy cách đây hơn 15 phút thì tab "Thời gian
> thực" trên dashboard sẽ trống — đúng thiết kế, nhưng không đẹp khi trình bày.
>
> ```bash
> docker compose exec spark-master /opt/spark/bin/spark-submit >     /app/spark/streaming_hot_path.py --once --starting-offsets earliest
> ```

---

## Kiến trúc

```
alonhadat.com.vn ──┐
Kaggle datasets ───┼──► Kafka (Redpanda) ──┬──► Spark Streaming ──► Redis (hot path)
                   │                        │
                   │                        └──► Spark Batch ──► HDFS Data Lake
                   │                                              Bronze → Silver → Gold
                   │                                                   │
                   │                             MLflow ◄── Training ──┤
                   │                             (MinIO)               │
                   │                                                   ▼
                   │                                            PostgreSQL
                   │                                                   │
                   └────────────────► FastAPI ──► Spring Boot ──► React Dashboard
```

Vai trò từng thành phần và lý do lựa chọn: xem docstring đầu mỗi file trong
[`spark/`](spark/) và [`ingestion/`](ingestion/).

---

## Hai vấn đề dữ liệu đã phát hiện và cách xử lý

Đây là hai phát hiện quan trọng nhất của đồ án, cần trình bày trong báo cáo.

### 1. Cải cách hành chính 01/07/2025 làm hai nguồn dữ liệu lệch hệ quy chiếu

Việt Nam bỏ cấp huyện, gộp 63 tỉnh còn 34. Hệ quả trực tiếp:

| Nguồn | Địa chỉ ghi | Số cấp |
|---|---|---|
| Kaggle 2024 | `Quận 7, TP.HCM` | 3 cấp (hệ **cũ**) |
| Tin rao 2026 | `Phường Tân Thuận, TP.HCM` | 2 cấp (hệ **mới**) |

Đo thực tế trên tin crawl: chỉ **33%** khớp được cấp quận. Tệ hơn, cách khớp ngây
thơ theo tên phường cho kết quả **sai một cách âm thầm** — "Phường Thanh Xuân"
(nội thành, 515 triệu/m²) bị gán về "Huyện Sóc Sơn" (ngoại thành, cách 40 km) vì
trùng tên với một xã cũ.

Chạy trên dataset thật còn lộ ra một mốc cải cách **thứ ba**: 712 tin (2,4%) ghi
"Quận 2" và "Quận 9" — hai đơn vị đã giải thể năm 2021 khi lập thành phố Thủ Đức
(Nghị quyết 1111/NQ-UBTVQH14), nên không còn trong bảng tham chiếu hiện hành.
Bảng `DISSOLVED_DISTRICTS` trong [`spark/udf_address.py`](spark/udf_address.py)
xử lý nhóm này.

**Kết quả chuẩn hóa trên 30.524 tin rao thật:**

| Cấp | Trước khi xử lý đơn vị giải thể | Sau |
|---|---|---|
| Tỉnh/thành | 99,99% | 99,99% |
| **Quận/huyện** | 97,56% | **99,90%** |
| Phường/xã | 89,74% | **91,63%** |

Nguồn suy ra mã quận: `old` 96,68% · `dissolved` 2,67% · `bridge2025` 0,54% ·
`unique` 0,01% · chưa khớp 0,10% (30 dòng rác thật sự).

Riêng `bridge2025` tăng từ 0,02% lên 0,54% sau khi thêm 297 tin crawl năm 2026 —
toàn bộ số tin đó ghi địa chỉ theo **hệ hành chính mới sau 01/07/2025**, và đều
được cầu nối quy đúng về quận cũ. Đây là bằng chứng bảng cầu nối hoạt động trên
dữ liệu thật chứ không chỉ trên ca kiểm thử.

**Hạn chế phải nêu**: gộp Quận 2 (Thảo Điền, An Phú — mặt bằng giá cao) với
Quận 9 (giá thấp hơn nhiều) vào Thủ Đức làm **mất độ phân giải giá**. Đây là hệ
quả của thực tế hành chính, không phải lỗi xử lý, nhưng người đọc heatmap cần
biết.

**Cách xử lý**: [`scripts/build_admin_bridge_2025.py`](scripts/build_admin_bridge_2025.py)
dựng bảng ánh xạ phường mới → quận cũ từ dữ liệu các nghị quyết sáp nhập, cho
3.308 phường/xã. [`spark/udf_address.py`](spark/udf_address.py) dùng bảng này làm
tầng dự phòng, và ghi lại nguồn suy ra mã quận vào cột `geo_source` để báo cáo
tách được kết quả theo từng cách khớp.

**Hạn chế phải nêu**: 9,5% phường mới ghép từ nhiều quận cũ khác nhau; với nhóm
này ta lấy quận chiếm đa số. Ánh xạ là xấp xỉ, không phải song ánh.

### 2. Nguồn crawl — đối chiếu robots.txt trước khi thu thập

| Site | robots.txt | Kết luận |
|---|---|---|
| batdongsan.com.vn | HTTP 403 (chặn cả bot đọc luật) | Loại |
| nhatot.com (Chợ Tốt) | `Content-Signal: ai-train=no` | Loại — chủ site từ chối cho dùng nội dung huấn luyện AI |
| **alonhadat.com.vn** | Cho phép `/nha-dat/can-ban/*`, không hạn chế AI | **Chọn** |

alonhadat còn công bố dữ liệu bằng **schema.org microdata** (`itemprop='price'
content='7000000000'`) — tức chủ động cung cấp cho máy đọc.

Crawler kiểm tra `robots.txt` bằng chương trình trước mỗi URL, tôn trọng
`Crawl-delay`, và **không thu thập thông tin cá nhân người đăng**.

**Giới hạn tốc độ đo được**: nhịp 5 giây/request cho ~40 trang liên tiếp khiến
site trả HTTP 429 chặn toàn bộ IP trong nhiều phút. Mặc định đã đặt lại thành
**20 giây**; nên chạy nhiều phiên nhỏ theo lịch thay vì một phiên lớn.

---

## Hạn chế của dữ liệu

Dataset `vietnam_housing_dataset.csv` (30.229 tin) **đã được lọc sẵn**: giá chỉ
trải từ **1 đến 11,5 tỷ VND**, trung vị 5,9 tỷ. Nghĩa là không có phân khúc cao
cấp, và mô hình **không tổng quát được cho bất động sản trên 11,5 tỷ**. Không có
cách khắc phục trong phạm vi dữ liệu hiện có; phải nêu rõ ở phần Giới hạn đề tài.

Ngoài ra đây là **giá rao bán**, không phải giá giao dịch thực. Chênh lệch giữa
hai loại giá này là một nguồn sai số cố hữu mà không mô hình nào loại bỏ được.

## Năm bài toán học máy

| # | Bài toán | Thuật toán | Trạng thái |
|---|---|---|---|
| 1 | Dự đoán giá | XGBoost / LightGBM / CatBoost + Optuna + Stacking | ✅ [`ml/train_price.py`](ml/train_price.py) |
| 2 | Heatmap giá theo tỉnh/quận/phường | Spark aggregation + Leaflet choropleth | ✅ [`spark/batch_silver_to_gold.py`](spark/batch_silver_to_gold.py) · [`dashboard/src/MapView.jsx`](dashboard/src/MapView.jsx) |
| 3 | Dự báo xu hướng | Prophet vs LSTM vs **Naive** | ✅ [`ml/train_forecast.py`](ml/train_forecast.py) — 9 chuỗi × 66 điểm tháng, naive thắng 8/9 |
| 4 | Phân cụm khu vực | KMeans + PCA, chọn k bằng Silhouette | ✅ [`ml/train_cluster.py`](ml/train_cluster.py) |
| 5 | Phát hiện tin bất thường | Phần dư mô hình giá + Isolation Forest + LOF | ✅ [`ml/train_anomaly.py`](ml/train_anomaly.py) — 916 tin |

Bài toán 1 dùng khung thực nghiệm 4 giai đoạn: sàng lọc cơ sở → naive ensemble →
tinh chỉnh Optuna → stacking. Mục đích là **chứng minh bằng số liệu** từng bước
phức tạp hóa có đáng hay không, chứ không chỉ chọn ra mô hình tốt nhất.

### Kết quả trên dữ liệu thật (30.524 tin rao)

| Mô hình | MAPE | MAE | R² (log) | R² (giá) | Độ trễ |
|---|---|---|---|---|---|
| LinearRegression | 24,14% | 1,257 tỷ | 0,776 | 0,406 | 0,65 ms |
| RandomForest | 19,81% | 1,069 tỷ | 0,825 | 0,435 | 38,4 ms |
| CatBoost + Optuna | 19,94% | 1,083 tỷ | 0,831 | 0,456 | 0,37 ms |
| LightGBM + Optuna | 19,51% | 1,055 tỷ | 0,836 | 0,484 | 1,28 ms |
| **XGBoost + Optuna** ← production | **19,43%** | 1,058 tỷ | **0,838** | 0,445 | 3,31 ms |
| Stacking | 19,40% | 1,052 tỷ | 0,838 | 0,486 | 36,1 ms → **loại** |

Stacking hơn đúng 0,027 điểm MAPE nhưng chậm 10,9 lần → bị quy tắc production
loại (chỉ nhận khi hơn > 0,5 điểm và chậm < 3 lần).

Kiểm tra độ bền trên quận **chưa từng thấy khi huấn luyện**: MAPE 23,61% (±2,23)
so với 19,43% khi chia ngẫu nhiên. Chênh lệch 4,2 điểm cho thấy mức phụ thuộc
thực tế vào đặc trưng vị trí.

Đặc trưng quan trọng nhất: `district_price_level` (0,359) → `log_area` (0,174) →
`floors` (0,094) → `district_x_legal` (0,083).

### Đồ án có đủ dữ liệu chưa?

Trả lời bằng đường cong học, không bằng cảm tính — huấn luyện lại trên các tập
con, đo trên cùng một tập kiểm tra cố định:

| Dữ liệu | Số dòng | MAPE | Lợi ích biên |
|---|---|---|---|
| 25% | 6.104 | 21,65% | |
| 50% | 12.209 | 20,61% | −1,04 điểm / +6.105 dòng |
| 75% | 18.314 | 19,70% | −0,91 điểm / +6.105 dòng |
| 100% | 24.419 | **19,42%** | **−0,28 điểm / +6.105 dòng** |

Cùng lượng dữ liệu thêm vào, lợi ích sụp từ 1,04 xuống 0,28 điểm. **Gấp đôi dữ
liệu chỉ đổi lấy ~0,3–0,6 điểm MAPE.**

Nút thắt không phải số dòng mà là **đặc trưng**: vị trí hiện chỉ là một con số
cho cả quận, trong khi biên độ giá p90/p10 *trong cùng một quận* trung bình là
**2,9 lần** (Quận 1: 3,8 lần). Hướng cải thiện đúng là thêm chiều không gian
(toạ độ, khoảng cách tới trung tâm/metro), không phải crawl thêm.

```bash
docker compose exec ml python scripts/learning_curve.py
```

### Bài toán 3 — naive thắng Prophet và LSTM ở 8/9 quận

Chuỗi giá theo ngày 9 quận TP.HCM, 2017-01 → 2022-06 (`HousePricingHCM_v2.csv`),
gộp về tháng bằng trung vị → 66 điểm/quận. Backtest cuốn chiếu, horizon 3 tháng,
24 fold mỗi địa bàn:

| Mô hình | MAPE trung bình | Độ lệch chuẩn | Thắng ở |
|---|---|---|---|
| **naive** (lặp giá trị cuối) | **5,08%** | **±1,96** | **8/9 quận** |
| LSTM | 8,67% | ±5,04 | 1/9 quận |
| Prophet | 11,54% | ±4,28 | 0/9 quận |

Naive vừa chính xác nhất vừa ổn định nhất. Kết luận **không phải** "chuỗi quá
ngắn" — 5,5 năm là thừa. Giá bất động sản rất gần **bước ngẫu nhiên có trôi**,
mà với bước ngẫu nhiên thì dự báo tối ưu chính là giá trị cuối cùng. Prophet và
LSTM cùng mắc một lỗi: ngoại suy đà tăng gần nhất đi quá xa.

Nếu chỉ báo cáo "Prophet đạt MAPE 11,54%" thì con số đó nghe hợp lý và không ai
chất vấn — trong khi một mô hình *không học gì cả* đạt 5,08%. Đó là lý do mốc
đối chứng naive nằm trong thiết kế ngay từ đầu.

**Một cái bẫy định dạng**: cột `Date` ghi kiểu Mỹ `M/D/YYYY` dù là dữ liệu Việt
Nam. Parse theo phản xạ `dayfirst=True` chỉ được 39,7% số dòng, và 40% còn lại
bị hiểu sai ngày (`01/03/2017` → 3 tháng 1 thay vì 1 tháng 3). Không lỗi, không
cảnh báo. Script nay thử cả hai quy ước và chọn cái parse được nhiều hơn.

```bash
docker compose exec ml python scripts/build_price_history.py
docker compose exec ml python ml/train_forecast.py --all --horizon 3
```

### Năm kết quả phản trực giác, đều rút ra từ số đo

Cả năm đáng đưa vào báo cáo — chúng cho thấy quy trình có kiểm chứng, không phải
chạy theo mặc định.

**1% dữ liệu thật làm RMSE tăng 75%.** Dataset Kaggle đã bị lọc còn 1–11,5 tỷ.
297 tin crawl thật từ alonhadat có dải giá **0,26–175 tỷ**. Thêm chúng vào (chưa
tới 1% dữ liệu):

| Chỉ số | Chỉ Kaggle | + 297 tin thật | |
|---|---|---|---|
| MAPE | 19,59% | **19,43%** | tốt hơn |
| R² (log) | 0,831 | **0,838** | tốt hơn |
| **R² (giá)** | **0,626** | **0,445** | **tệ hơn 29%** |
| **RMSE** | **1,351 tỷ** | **2,369 tỷ** | **tệ hơn 75%** |

Chỉ số tương đối (MAPE, R² log) tốt lên vì có thêm thông tin; chỉ số tuyệt đối
(RMSE, R² giá) sụp vì vài chục tin giá cao mà mô hình chưa từng thấy. R² 0,626
báo cáo trước đó **đúng về tính toán nhưng chỉ có nghĩa trong một thị trường
không tồn tại** — thị trường không có căn nào trên 11,5 tỷ.

**Hai đặc trưng chết âm thầm suốt quá trình huấn luyện.** Dataset ghi pháp lý và
nội thất bằng **tiếng Anh** (`Have certificate`, `Sale contract`, `Full`,
`Basic`) trong khi bảng tra thứ bậc chỉ có tiếng Việt. Hệ quả: `legal_rank` và
`furniture_rank` là NaN toàn bộ, `district_x_legal` chết theo — mà mô hình vẫn
chạy, vẫn cho ra MAPE trông hợp lý, không có gì báo lỗi. Sau khi sửa bảng tra,
MAPE 19,79% → **19,59%** và `district_x_legal` vươn lên đặc trưng quan trọng thứ
tư. Đã thêm cảnh báo trong [`ml/features.py`](ml/features.py) khi một đặc trưng
thứ bậc không khớp được giá trị nào.

**Stacking không đáng dùng.** Giai đoạn 4 hơn XGBoost đã tinh chỉnh đúng 0,027
điểm MAPE nhưng suy luận chậm hơn **10,9 lần**. Script tự động loại theo quy tắc
"chỉ nhận khi hơn > 0,5 điểm MAPE và chậm dưới 3 lần" — quy tắc đặt trước khi
biết kết quả.

**Phân cụm bằng đặc trưng không phân hoá cho kết quả sai.** `StandardScaler` gán
trọng số bằng nhau cho mọi đặc trưng, nên đặc trưng gần như hằng số giữa các
quận chỉ đóng góp nhiễu — nhưng nhiễu đó được phóng đại lên bằng đúng tín hiệu
giá. Trên dữ liệu giả lập, hệ quả là quận Ba Đình (408 triệu/m²) bị xếp cùng
nhóm với Biên Hòa (52 triệu/m²). Sau khi thêm bước sàng lọc theo hệ số biến
thiên, Silhouette tăng 0,226 → 0,628. Trên dữ liệu thật, bộ sàng lọc loại `pct_red_book`
(CV 0,148 — hầu hết quận đều trên 90% có sổ) và cho Silhouette **0,4758**, PCA 2
chiều giữ 84,0% phương sai, chồng lấn giá 19%.

Silhouette chọn k=3, nhưng **cụm thứ ba chỉ có một quận**: TP. Cà Mau, tách ra vì
diện tích trung bình 1.117 m² — gấp mười lần mọi địa bàn khác, gần như chắc chắn
là đất nền bị đăng vào danh mục nhà ở. Phân cụm ở đây vô tình hoạt động như một
bộ phát hiện lỗi dữ liệu.

**Tổ hợp ba tín hiệu bất thường làm hỏng kết quả.** Đo trên nhãn thật của dữ
liệu giả lập (dữ liệu thật không có nhãn "tin ảo"):

| Tín hiệu | PR-AUC | P@50 |
|---|---|---|
| Phần dư mô hình giá | **0,9924** | **100%** |
| Isolation Forest | 0,0270 | 2% |
| Local Outlier Factor | 0,0205 | 0% |
| Trung bình có trọng số (0.5/0.3/0.2) | 0,1740 | 26% |

Mức nền đoán ngẫu nhiên là 0,0192. Trộn hai tín hiệu gần như nhiễu vào một tín
hiệu gần hoàn hảo kéo PR-AUC từ 0,99 xuống 0,17. Điểm chính đã đổi sang dùng
riêng phần dư; IF/LOF giữ lại làm cờ bổ sung. Lưu ý: kết luận này rút từ dữ liệu
giả lập nơi mọi bất thường đều là nhiễu loạn giá thuần tuý — trên dữ liệu thật
có cả tin mô tả phi lý, khi đó IF/LOF sẽ có giá trị.

---

## Bàn giao

| Hạng mục | Vị trí |
|---|---|
| Báo cáo 6 chương (nguồn) | [`report/BaoCaoCuoiKy.md`](report/BaoCaoCuoiKy.md) |
| Báo cáo bản Word | `report/BaoCaoCuoiKy.docx` — sinh bằng `scripts/build_report.py` |
| Biểu đồ kết quả | [`report/figures/`](report/figures/) |

Báo cáo được viết ở Markdown rồi dựng ra Word bằng script, không soạn tay: nội
dung chứa hàng chục con số lấy từ kết quả chạy thật, mỗi lần huấn luyện lại là
số đổi. Giữ một nguồn duy nhất thì báo cáo và hệ thống không nói hai chuyện
khác nhau.

---

## Chạy kiểm tra

Mỗi module có bộ kiểm tra chạy độc lập, không cần framework:

```bash
python ingestion/schemas.py             # ép kiểu, ánh xạ cột, đơn vị giá
python ingestion/crawler_alonhadat.py --self-test
python spark/udf_address.py             # chuẩn hóa địa chỉ + cầu nối 2025
docker compose exec ml python ml/features.py
docker compose exec ml python ml/train_forecast.py --self-test
```

Kiểm chứng toàn hệ thống:

```bash
# Data lake có đủ 3 tầng
docker exec namenode hdfs dfs -du -h /lake

# Tỷ lệ khớp địa chỉ (ngưỡng chấp nhận > 90%)
docker exec postgres psql -U reuser -d realestate -c \
  "SELECT geo_source, COUNT(*) FROM silver_listings GROUP BY geo_source;"

# Giá trung vị theo quận
docker exec postgres psql -U reuser -d realestate -c \
  "SELECT area_name, ROUND(median_price_m2::numeric,1), n_listings
   FROM gold_area_price WHERE level='district' ORDER BY median_price_m2 DESC LIMIT 10;"
```

---

## Yêu cầu hệ thống

- Docker Desktop, **≥16 GB RAM** (cụm đầy đủ dùng ~12 GB)
- Python 3.11+ trên host (chỉ để chạy script tiện ích; toàn bộ ML chạy trong container)

Nếu máy thiếu RAM: giảm `SPARK_WORKER_MEMORY` trong `docker-compose.yml`, hoặc tắt
`spark-worker-2` khi phát triển và chỉ bật lại lúc quay video demo.

---

## Quy ước đơn vị

Thống nhất toàn hệ thống, đừng đổi:

| Trường | Đơn vị |
|---|---|
| `area`, `frontage`, `access_road` | m |
| `price` | tỷ VND |
| `price_per_m2` | triệu VND/m² |

---

## Tài liệu

- [`doc/`](doc/) — báo cáo mẫu và đề cương đề tài
- [`report/figures/`](report/figures/) — biểu đồ sinh tự động cho báo cáo
