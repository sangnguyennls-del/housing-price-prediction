# ĐẠI HỌC QUỐC GIA THÀNH PHỐ HỒ CHÍ MINH
# TRƯỜNG ĐẠI HỌC CÔNG NGHỆ THÔNG TIN
## KHOA HỆ THỐNG THÔNG TIN

---

# BÁO CÁO ĐỒ ÁN CUỐI KỲ
# MÔN IE221 — CÔNG NGHỆ DỮ LIỆU LỚN

## ĐỀ TÀI
# XÂY DỰNG NỀN TẢNG DỮ LIỆU LỚN PHÂN TÍCH VÀ DỰ BÁO GIÁ BẤT ĐỘNG SẢN VIỆT NAM

**Giảng viên hướng dẫn:** _(điền tên GVHD)_

**Nhóm thực hiện:** _(điền tên nhóm)_

**Thành viên:** _(điền MSSV — Họ tên)_

**TP. Hồ Chí Minh, tháng 9 năm 2026**

<!-- PAGEBREAK -->

## NHẬN XÉT CỦA GIẢNG VIÊN HƯỚNG DẪN

_(để trống cho giảng viên)_

.................................................................................

.................................................................................

.................................................................................

.................................................................................

.................................................................................

.................................................................................

<!-- PAGEBREAK -->

## BẢNG PHÂN CÔNG VÀ ĐÁNH GIÁ THÀNH VIÊN

| STT | MSSV | Họ và tên | Nội dung phụ trách | Mức độ hoàn thành |
|---|---|---|---|---|
| 1 | | | Hạ tầng Docker, Kafka, HDFS, Spark cluster · Tầng ingestion và crawler · Chuẩn hóa địa chỉ | 100% |
| 2 | | | Feature engineering và 5 bài toán học máy · MLflow · Tầng serving (FastAPI, Spring Boot, React) · Báo cáo | 100% |

<!-- PAGEBREAK -->

## LỜI MỞ ĐẦU

Thị trường bất động sản Việt Nam là một trong những thị trường có giá trị giao dịch lớn nhất nền kinh tế, nhưng đồng thời cũng là thị trường thiếu minh bạch về giá bậc nhất. Người mua nhà không có một công cụ định giá khách quan nào; họ dựa vào cảm tính, vào lời môi giới, và vào việc so sánh thủ công vài chục tin rao trên các trang thương mại điện tử. Tin rao ảo và giá mồi — đăng giá thấp bất thường để thu hút liên hệ rồi báo "căn đó vừa bán" — là hiện tượng phổ biến mà chưa có cơ chế phát hiện tự động nào.

Đồng thời, dữ liệu để giải bài toán này thì có sẵn và rất lớn: mỗi ngày hàng chục nghìn tin rao mới xuất hiện trên các sàn trực tuyến, mang theo đầy đủ thuộc tính về vị trí, diện tích, pháp lý và giá chào bán. Vấn đề không phải là thiếu dữ liệu mà là thiếu một hạ tầng đủ sức thu nhận liên tục, làm sạch, chuẩn hóa và biến khối dữ liệu đó thành tri thức có thể sử dụng được.

Đó chính là chỗ mà công nghệ dữ liệu lớn có vai trò. Đồ án này xây dựng một nền tảng end-to-end theo kiến trúc Lambda: một nhánh xử lý theo lô để phân tích toàn bộ lịch sử, một nhánh xử lý dòng để phản ứng với tin mới trong vài giây, và một tầng học máy giải năm bài toán phân tích cụ thể trên nền dữ liệu đó.

Trong quá trình thực hiện, nhóm gặp và giải quyết một số vấn đề mà tài liệu học thuật hiếm khi đề cập: sự tồn tại đồng thời của **ba hệ quy chiếu hành chính Việt Nam** trong cùng một tập dữ liệu, việc một đặc trưng quan trọng **chết âm thầm** mà không có bất kỳ dấu hiệu lỗi nào, và việc **tổ hợp nhiều tín hiệu phát hiện bất thường lại cho kết quả tệ hơn dùng một tín hiệu**. Báo cáo trình bày đầy đủ cả những hướng đi đã thử mà không hiệu quả, vì nhóm cho rằng đó mới là phần có giá trị khoa học thật.

Nhóm xin chân thành cảm ơn thầy/cô đã hướng dẫn và góp ý trong suốt quá trình thực hiện đồ án.

<!-- PAGEBREAK -->

## DANH MỤC THUẬT NGỮ

| Thuật ngữ | Giải thích |
|---|---|
| Kiến trúc Lambda | Mô hình kiến trúc dữ liệu lớn chia xử lý thành hai nhánh: batch layer (chính xác, độ trễ cao) và speed layer (gần đúng, độ trễ thấp), hợp nhất ở serving layer |
| Medallion Architecture | Cách tổ chức data lake theo ba tầng chất lượng tăng dần: Bronze (thô), Silver (đã làm sạch), Gold (đã tổng hợp) |
| Data Lake | Kho lưu trữ dữ liệu ở dạng gần nguyên bản, không ép vào lược đồ cố định trước khi ghi |
| Structured Streaming | Mô hình xử lý dòng của Spark, coi luồng dữ liệu như một bảng vô hạn được nối thêm liên tục |
| Watermark | Ngưỡng thời gian mà sau đó dữ liệu đến muộn bị bỏ qua, dùng để giới hạn kích thước trạng thái của phép tổng hợp streaming |
| Cửa sổ trượt (sliding window) | Cửa sổ thời gian có độ dài cố định, dịch chuyển theo một bước nhỏ hơn độ dài của nó, nên các cửa sổ chồng lấn nhau |
| Target encoding | Kỹ thuật mã hóa biến phân loại bằng thống kê của biến mục tiêu trên từng nhóm |
| Rò rỉ dữ liệu (data leakage) | Hiện tượng thông tin của tập kiểm tra lọt vào quá trình huấn luyện, làm chỉ số đánh giá tốt giả tạo |
| Out-of-fold | Cách tính đặc trưng theo K-fold sao cho giá trị của mỗi dòng chỉ dựa vào các fold khác, nhằm chống rò rỉ |
| Gradient Boosting | Họ thuật toán học máy xây dựng tuần tự nhiều cây quyết định, mỗi cây sửa sai số của tổ hợp cây trước |
| Bayesian Optimization | Phương pháp tối ưu siêu tham số dựa trên mô hình xác suất về mặt phẳng mục tiêu, hiệu quả hơn tìm kiếm lưới |
| Stacking | Kỹ thuật hợp thành, dùng một mô hình cấp hai học cách kết hợp dự đoán của nhiều mô hình cấp một |
| Silhouette score | Chỉ số đo chất lượng phân cụm, so sánh khoảng cách trong cụm với khoảng cách tới cụm gần nhất; giá trị càng gần 1 càng tốt |
| Backtest cuốn chiếu | Cách đánh giá mô hình chuỗi thời gian bằng nhiều lần cắt train/test liên tiếp theo trục thời gian |
| Choropleth | Bản đồ tô màu vùng theo giá trị của một đại lượng thống kê |
| schema.org microdata | Chuẩn đánh dấu ngữ nghĩa nhúng trong HTML, cho phép máy đọc hiểu nội dung trang web |

## DANH MỤC TỪ VIẾT TẮT

| Viết tắt | Đầy đủ |
|---|---|
| API | Application Programming Interface |
| BĐS | Bất động sản |
| CV | Coefficient of Variation — hệ số biến thiên |
| ETL | Extract — Transform — Load |
| GSO | General Statistics Office — Tổng cục Thống kê |
| HDFS | Hadoop Distributed File System |
| IF | Isolation Forest |
| JDBC | Java Database Connectivity |
| KPI | Key Performance Indicator |
| LOF | Local Outlier Factor |
| LSTM | Long Short-Term Memory |
| MAE | Mean Absolute Error |
| MAPE | Mean Absolute Percentage Error |
| PCA | Principal Component Analysis |
| PR-AUC | Area Under the Precision-Recall Curve |
| RMSE | Root Mean Squared Error |
| SPA | Single Page Application |
| TTL | Time To Live |
| UDF | User Defined Function |

<!-- PAGEBREAK -->

## MỤC LỤC

1. Tổng quan đề tài
2. Cơ sở lý thuyết và nghiên cứu liên quan
3. Phương pháp nghiên cứu và quy trình thực thi
4. Xây dựng và tối ưu hóa mô hình học máy
5. Thực nghiệm và đánh giá kết quả
6. Kết luận và hướng phát triển

Tài liệu tham khảo

<!-- PAGEBREAK -->

# CHƯƠNG 1. TỔNG QUAN ĐỀ TÀI

## 1.1. Đặt vấn đề

Ba vấn đề thực tế của thị trường bất động sản Việt Nam là động lực của đề tài này.

**Thứ nhất, người mua không có công cụ định giá khách quan.** Khi xem một tin rao "nhà 80 m², 3 phòng ngủ, Quận 7, giá 8 tỷ", người mua không có cách nào biết mức giá đó là hợp lý, đắt hay rẻ so với mặt bằng. Việc so sánh thủ công vài chục tin rao là không khả thi vì mỗi bất động sản khác nhau ở hàng chục thuộc tính, và ảnh hưởng của từng thuộc tính lên giá là phi tuyến.

**Thứ hai, thông tin giá bị phân mảnh giữa nhiều sàn.** Cùng một khu vực có thể có hàng nghìn tin rao trải trên năm bảy trang khác nhau, mỗi trang một định dạng, một cách viết địa chỉ. Không có nơi nào tổng hợp được bức tranh giá theo địa bàn.

**Thứ ba, tin rao ảo và giá mồi phổ biến nhưng không có cơ chế phát hiện.** Đây là dạng gian lận gây tổn thất thời gian trực tiếp cho người mua, và làm nhiễu mọi thống kê giá tính từ dữ liệu tin rao.

Về mặt kỹ thuật, giải bài toán này ở quy mô toàn quốc đặt ra đúng ba thách thức đặc trưng của dữ liệu lớn:

- **Volume** — hàng trăm nghìn tin rao lịch sử, cần lưu trữ có tổ chức và xử lý lại được khi logic làm sạch thay đổi.
- **Velocity** — tin mới xuất hiện liên tục; giá trị của một cảnh báo tin bất thường giảm rất nhanh theo thời gian.
- **Variety** — mỗi nguồn có cấu trúc riêng, và ngay trong một nguồn, địa chỉ được người đăng gõ tự do với vô số biến thể.

## 1.2. Mục tiêu đề tài

**Mục tiêu tổng quát:** xây dựng một nền tảng dữ liệu lớn hoàn chỉnh, chạy được và demo được, thu thập — xử lý — phân tích tin rao bất động sản Việt Nam, đồng thời giải năm bài toán phân tích cụ thể trên nền tảng đó.

**Mục tiêu cụ thể:**

1. Triển khai kiến trúc Lambda đầy đủ với Kafka, Spark (batch và streaming), HDFS, PostgreSQL và Redis, đóng gói bằng Docker Compose.
2. Xây dựng module chuẩn hóa địa chỉ tiếng Việt đạt tỷ lệ khớp cấp quận/huyện trên 90%.
3. Dự đoán giá bất động sản với MAPE dưới 25%, theo một quy trình thực nghiệm nhiều giai đoạn có kiểm chứng.
4. Trực quan hóa mặt bằng giá theo đơn vị hành chính trên bản đồ choropleth.
5. Phân cụm khu vực, phát hiện tin rao bất thường, và dự báo xu hướng giá.
6. Xây dựng tầng phục vụ ba lớp (FastAPI — Spring Boot — React) để kết quả sử dụng được, không chỉ nằm trong notebook.

## 1.3. Phạm vi đề tài

**Phạm vi dữ liệu.** Tin rao **bán** bất động sản nhà ở tại Việt Nam. Không bao gồm cho thuê, bất động sản công nghiệp và văn phòng.

**Phạm vi công nghệ.** Toàn bộ hạ tầng chạy trên một máy đơn qua Docker Compose. Không triển khai lên cloud; không cấu hình HA (high availability) hay bảo mật cấp production.

**Phạm vi địa lý.** Toàn quốc ở mức thu thập và huấn luyện. Riêng bài toán dự báo xu hướng bị giới hạn hẹp hơn do ràng buộc dữ liệu, xem mục 1.4.

## 1.4. Giới hạn của đề tài

Nhóm nêu các giới hạn ngay ở chương đầu, vì chúng ảnh hưởng tới cách đọc mọi con số ở các chương sau.

**(1) Giá rao không phải giá giao dịch.** Toàn bộ dữ liệu của đồ án là giá **chào bán**. Chênh lệch giữa giá rao và giá chốt là một nguồn sai số cố hữu, không mô hình nào loại bỏ được vì thông tin đó không tồn tại trong dữ liệu. Mọi kết quả phải được đọc là "ước lượng mặt bằng giá chào bán".

**(2) Dataset chính đã bị lọc trước.** Tập `vietnam_housing_dataset.csv` có giá chỉ trải từ 1 đến 11,5 tỷ VND. Nghĩa là phân khúc cao cấp (biệt thự, nhà phố trung tâm trên 11,5 tỷ) hoàn toàn vắng mặt. **Mô hình không tổng quát được ra ngoài khoảng giá này**, và không có cách khắc phục trong phạm vi dữ liệu hiện có.

**(3) Hệ quy chiếu hành chính.** Đồ án chọn hệ **trước ngày 01/07/2025** (63 tỉnh/thành, có cấp quận/huyện) làm hệ quy chiếu chính, vì phần lớn dữ liệu được ghi theo hệ này. Dữ liệu ghi theo hệ mới được quy đổi ngược lại qua một bảng cầu nối. Chi tiết ở mục 3.5; đây là vấn đề kỹ thuật lớn nhất của đồ án.

**(4) Dự báo xu hướng bị giới hạn bởi dữ liệu.** Dataset tin rao chính **không có cột thời gian**. Nhóm chủ động **không** sinh mốc thời gian giả để bài toán 3 "chạy được"; chi tiết và hệ quả ở mục 4.5.

**(5) Đánh giá phát hiện bất thường là bán định lượng.** Không có nhãn "tin ảo" thật. Nhóm đánh giá bằng nhãn sinh trên dữ liệu giả lập cộng với kiểm tra thủ công trên dữ liệu thật; xem mục 4.7.

<!-- PAGEBREAK -->

# CHƯƠNG 2. CƠ SỞ LÝ THUYẾT VÀ NGHIÊN CỨU LIÊN QUAN

## 2.1. Nghiên cứu liên quan

### 2.1.1. Định giá bất động sản Hà Nội bằng học máy (Springer, 2025)

Nghiên cứu *Analysis and Prediction of Real Estate Prices in Hanoi Using Machine Learning* sử dụng chính dữ liệu batdongsan.com.vn — cùng nguồn với dataset của đồ án này — và áp dụng các mô hình hồi quy cây. Đây là công trình gần đề tài nhất về cả dữ liệu lẫn bài toán.

**Điểm nhóm kế thừa:** cách xử lý phân phối giá lệch phải bằng biến đổi log, và việc dùng MAPE làm chỉ số chính bên cạnh RMSE.

**Điểm nhóm làm khác:**

| Khía cạnh | Nghiên cứu tham chiếu | Đồ án này |
|---|---|---|
| Hạ tầng | Notebook, dữ liệu tĩnh | Nền tảng Big Data end-to-end, dữ liệu chảy liên tục |
| Thời gian thực | Không có | Hot path Spark Streaming, độ trễ dưới một micro-batch |
| Chuẩn hóa địa chỉ | Xử lý thủ công, phạm vi một thành phố | Module riêng, đối chiếu ba hệ quy chiếu, toàn quốc |
| Biến mục tiêu | Tổng giá | Đơn giá theo m² (loại bỏ tương quan hiển nhiên với diện tích) |
| Phạm vi | Chỉ dự đoán giá | Năm bài toán, có phát hiện bất thường và phân cụm |

### 2.1.2. Mô hình hedonic trong kinh tế học bất động sản

Cách tiếp cận kinh điển coi giá bất động sản là hàm của các thuộc tính cấu thành: vị trí, diện tích, chất lượng xây dựng, tiện ích xung quanh. Mô hình hedonic tuyến tính có ưu điểm là hệ số diễn giải trực tiếp được ("thêm 1 m² mặt tiền làm giá tăng x triệu"), nhưng giả định tuyến tính và không tương tác là quá mạnh với thị trường Việt Nam, nơi ảnh hưởng của bề rộng đường vào lên giá có bước nhảy rõ rệt tại ngưỡng 4 m (ô tô vào được).

Đồ án dùng gradient boosting làm mô hình chính, nhưng vẫn giữ Linear Regression làm mốc cơ sở để đo đúng phần giá trị mà tính phi tuyến mang lại.

### 2.1.3. Đặc thù thị trường Việt Nam

Ba yếu tố có ảnh hưởng mạnh tới giá mà các bài toán chuẩn quốc tế (Boston Housing, Ames Housing) không có:

- **Bề rộng đường vào.** Ngưỡng 4 m (ô tô vào được) và 8 m (ô tô tránh nhau) tạo bước nhảy giá rõ rệt.
- **Tình trạng pháp lý.** Sổ đỏ/sổ hồng, hợp đồng mua bán, giấy tờ viết tay — tạo mức chiết khấu giá rất khác nhau. Đây là biến **thứ bậc**, không phải biến phân loại rời rạc.
- **Hướng nhà (phong thủy).** Ảnh hưởng thực tế lên giá giao dịch tại Việt Nam. Về mặt kỹ thuật đây là **biến vòng**: Bắc (0°) và Tây Bắc (315°) chỉ cách nhau 45°.

## 2.2. Kiến trúc Lambda

Kiến trúc Lambda chia hệ thống thành ba tầng:

- **Batch layer** — lưu toàn bộ dữ liệu thô bất biến và tính lại các khung nhìn tổng hợp theo lô. Chính xác nhưng độ trễ cao.
- **Speed layer** — xử lý dòng dữ liệu mới, cho kết quả gần đúng nhưng gần như tức thời.
- **Serving layer** — hợp nhất hai nguồn trên để trả lời truy vấn.

Lý do chọn kiến trúc này cho đề tài: hai loại câu hỏi của bài toán bất động sản có yêu cầu độ trễ hoàn toàn khác nhau. "Mặt bằng giá Quận 7 là bao nhiêu" cần chính xác, chấp nhận độ trễ hàng giờ. "Có tin nào vừa đăng với giá bất thường không" cần trong vài giây và chấp nhận gần đúng.

## 2.3. Kiến trúc Medallion

Medallion tổ chức data lake thành ba tầng chất lượng:

| Tầng | Nội dung | Nguyên tắc |
|---|---|---|
| **Bronze** | Dữ liệu gần nguyên bản, chỉ thêm metadata thu nạp | Không xoá, không sửa, không lọc |
| **Silver** | Đã khử trùng lặp, làm sạch, chuẩn hóa địa chỉ | Chỉ loại dữ liệu **không thể dùng được** |
| **Gold** | Đã tổng hợp theo chiều phân tích | Tối ưu cho truy vấn, không cho lưu trữ |

Giá trị cốt lõi của Bronze là khả năng **xử lý lại**. Logic làm sạch chắc chắn sẽ thay đổi — trong đồ án này nó đã thay đổi bốn lần khi phát hiện các đơn vị hành chính đã giải thể. Nếu không giữ Bronze, mỗi lần sửa logic phải crawl lại từ đầu.

## 2.4. Gradient Boosting

Gradient boosting xây dựng tuần tự một tổ hợp cây quyết định, trong đó cây thứ *m* được huấn luyện để dự đoán phần dư của tổ hợp *m-1* cây trước:

$$F_m(x) = F_{m-1}(x) + \nu \cdot h_m(x)$$

với $\nu$ là learning rate và $h_m$ là cây khớp gradient âm của hàm mất mát.

Ba hiện thực được so sánh trong đồ án:

| Thư viện | Đặc điểm nổi bật |
|---|---|
| **XGBoost** | Chính quy hóa bậc hai trong hàm mục tiêu; xử lý giá trị khuyết bằng hướng mặc định học được |
| **LightGBM** | Tăng trưởng cây theo lá (leaf-wise) thay vì theo mức; nhanh hơn trên dữ liệu lớn |
| **CatBoost** | Ordered boosting chống rò rỉ trong target encoding; xử lý biến phân loại tự nhiên |

Việc mô hình cây xử lý được giá trị khuyết trực tiếp là lý do đồ án **không điền khuyết** cho các thuộc tính như hướng nhà hay pháp lý: "không biết hướng" là một thông tin khác với "hướng Bắc", và điền bằng giá trị phổ biến nhất sẽ xoá mất sự khác biệt đó.

## 2.5. Tối ưu siêu tham số Bayes

Tìm kiếm lưới đánh giá mọi tổ hợp trong một lưới định trước; số lần đánh giá tăng theo hàm mũ với số siêu tham số. Tối ưu Bayes thay vào đó xây dựng một mô hình xác suất về mối quan hệ giữa siêu tham số và chất lượng, rồi chọn điểm thử tiếp theo để cân bằng giữa khai thác và thăm dò.

Đồ án dùng Optuna với thuật toán TPE (Tree-structured Parzen Estimator). Với 50 lần thử cho mỗi mô hình, TPE khảo sát được không gian siêu tham số rộng hơn nhiều so với một lưới cùng ngân sách.

## 2.6. Phát hiện bất thường

Ba hướng tiếp cận được cài đặt và so sánh:

- **Isolation Forest** — dựa trên nhận xét rằng điểm bất thường bị cô lập bởi ít lần chia ngẫu nhiên hơn điểm bình thường. Không giám sát, không cần nhãn.
- **Local Outlier Factor** — so sánh mật độ cục bộ quanh một điểm với mật độ quanh các láng giềng của nó. Bắt được bất thường **cục bộ** mà phương pháp toàn cục bỏ sót.
- **Dựa trên phần dư mô hình** — dùng chính mô hình dự đoán giá: tin nào có $|y_{thực} - \hat{y}| / \hat{y}$ vượt ngưỡng thì đáng nghi.

Hướng thứ ba tận dụng được toàn bộ tri thức mà mô hình giá đã học, và quan trọng hơn — nó **giải thích được cho người dùng cuối**: "căn này rẻ hơn 71% so với mức mô hình ước tính cho cùng vị trí và diện tích". Isolation Forest chỉ trả về một điểm số không diễn giải được.

<!-- PAGEBREAK -->

# CHƯƠNG 3. PHƯƠNG PHÁP NGHIÊN CỨU VÀ QUY TRÌNH THỰC THI

## 3.1. Kiến trúc tổng quan

```
alonhadat.com.vn ──┐
                   ├──► Kafka (Redpanda) ──┬──► Spark Structured Streaming ──► Redis
Kaggle datasets ───┘                       │         (hot path, speed view)
                                           │
                                           └──► Spark Batch (cold path)
                                                        │
                                                HDFS Data Lake
                                             Bronze → Silver → Gold
                                                        │
                                    ┌───────────────────┼───────────────────┐
                                    ▼                   ▼                   ▼
                              Model Training      PostgreSQL          (Parquet gốc)
                              XGB/LGBM/CatBoost   (serving layer)
                              KMeans/IForest             │
                                    │                    │
                                    ▼                    │
                              MLflow + MinIO             │
                                    │                    │
                                    └────► FastAPI ◄─────┘
                                              │
                                              ▼
                                    Spring Boot API Gateway
                                              │
                                              ▼
                                 React + Leaflet + ECharts
```

## 3.2. Vai trò và lý do lựa chọn từng thành phần

Mỗi thành phần phải có lý do tồn tại rõ ràng; một thành phần chỉ để cho "trông có vẻ Big Data" là gánh nặng vận hành chứ không phải điểm cộng.

| Thành phần | Vai trò | Lý do lựa chọn |
|---|---|---|
| **Kafka (Redpanda)** | Vùng đệm giữa nguồn và xử lý | Tách rời crawler khỏi Spark: crawler bị chặn hay chậm cũng không làm sập pipeline. Redpanda không cần Zookeeper, nhẹ hơn đáng kể trên máy đơn, API tương thích Kafka 100% |
| **Spark Structured Streaming** | Hot path — vận tốc đăng tin, cảnh báo tức thời | Chứng minh chữ V "Velocity". Dùng chung engine với batch nên không phải học và vận hành hai hệ thống |
| **Spark Batch** | Cold path — ETL Medallion | Chứng minh chữ V "Volume". Xử lý lại toàn bộ lịch sử khi logic thay đổi |
| **HDFS** | Data Lake (Bronze/Silver/Gold) | Môn học là hệ sinh thái Hadoop; dùng đúng công cụ gốc thay vì thay thế bằng object storage |
| **MinIO** | **Chỉ** làm artifact store cho MLflow | MLflow hỗ trợ S3 native. Không lưu data lake ở đây để tránh trùng vai trò với HDFS |
| **PostgreSQL** | Serving layer | Bảng Gold đã tổng hợp sẵn, dashboard truy vấn trong mili-giây thay vì đọc trực tiếp HDFS |
| **Redis** | Speed view của hot path, TTL 900 giây | Cấu trúc dữ liệu phù hợp sẵn: hash cho chỉ số từng quận, sorted set cho xếp hạng, list có giới hạn cho hàng đợi cảnh báo |
| **MLflow** | Theo dõi thực nghiệm + Model Registry | Bốn giai đoạn thực nghiệm của bài toán 1 sinh ra hàng chục lần chạy; so sánh thủ công là không khả thi |
| **FastAPI** | Suy luận mô hình | **Bắt buộc là Python**: mô hình là XGBoost/scikit-learn. Nạp ở JVM đòi xuất PMML/ONNX và mất đúng pipeline tiền xử lý |
| **Spring Boot** | API Gateway | Gộp ba nguồn (PostgreSQL + Redis + FastAPI) thành một response cho mỗi màn hình; cache tầng Gold; che topology hạ tầng khỏi frontend |
| **React + Leaflet + ECharts** | Dashboard | Choropleth và biểu đồ tương tác |
| **Docker Compose** | Đóng gói hạ tầng | Toàn bộ 16 dịch vụ khởi động bằng một lệnh, tái lập được trên máy khác |

## 3.3. Nguồn dữ liệu và đạo đức thu thập

### 3.3.1. Đối chiếu nguồn crawl trước khi thu thập

Trước khi viết bất kỳ dòng crawler nào, nhóm đọc `robots.txt` của từng ứng viên **bằng chương trình** và lập bảng đối chiếu:

| Site | robots.txt | Kết luận |
|---|---|---|
| batdongsan.com.vn | Trả HTTP 403 — chặn cả bot đọc chính file luật | **Loại.** Không đọc được luật thì không có cơ sở để tuân thủ |
| nhatot.com (Chợ Tốt) | `Content-Signal: ai-train=no` | **Loại.** Chủ site tuyên bố rõ không cho dùng nội dung để huấn luyện AI |
| **alonhadat.com.vn** | Cho phép `/nha-dat/can-ban/*`, không có tín hiệu hạn chế AI, không có nhóm `User-agent` riêng cho bot AI | **Chọn** |

Yếu tố quyết định thêm: alonhadat công bố dữ liệu bằng **schema.org microdata** (`itemprop='price' content='7000000000'`), tức chủ động cung cấp cho máy đọc. Bóc theo microdata cũng ổn định hơn nhiều so với bóc theo class CSS.

Chỉ thị `Content-Signal` của Chợ Tốt là một chuẩn mới, chưa có ràng buộc pháp lý. Nhóm vẫn coi đó là **ràng buộc cứng**: đây là tuyên bố ý chí rõ ràng của chủ sở hữu nội dung, và việc tồn tại kẽ hở pháp lý không làm cho việc lách trở nên đúng đắn.

### 3.3.2. Các nguyên tắc tuân thủ đã cài đặt

1. Kiểm tra `robots.txt` bằng `urllib.robotparser` **trước mỗi URL**, không phải đọc bằng mắt một lần rồi tin.
2. Tôn trọng `Crawl-delay` nếu site khai báo; nếu không, mặc định **20 giây/request**.
3. `User-Agent` trung thực, nêu rõ mục đích học thuật.
4. **Không thu thập thông tin cá nhân người đăng** — tên, số điện thoại, ảnh đại diện đều bị bỏ qua. Chỉ lấy thuộc tính bất động sản và giá.
5. Tải **trang danh sách** thay vì trang chi tiết: mỗi trang danh sách cho ~20 tin, giảm khoảng 20 lần số request cho cùng lượng dữ liệu.
6. Lùi theo cấp số nhân khi gặp HTTP 429/503, và **giãn vĩnh viễn** nhịp gửi cho phần còn lại của phiên.

**Kết quả đo thực tế:** nhịp 5 giây/request cho khoảng 40 trang liên tiếp khiến site trả HTTP 429 **chặn toàn bộ IP** trong nhiều phút. Đây là lý do mặc định được đặt lại thành 20 giây. Con số này không suy ra từ lý thuyết mà từ việc bị chặn thật.

### 3.3.3. Dataset huấn luyện

| Nguồn | Vai trò | Quy mô |
|---|---|---|
| `vietnam_housing_dataset.csv` (Kaggle) | Tập huấn luyện chính | 30.229 tin, gốc batdongsan.com.vn |
| alonhadat.com.vn (crawler) | Dòng dữ liệu mới cho hot path, và nguồn `posted_at` cho chuỗi thời gian | Tích lũy liên tục |

Dataset Kaggle có 12 cột: Address, Area, Frontage, Access Road, House direction, Balcony direction, Floors, Bedrooms, Bathrooms, Legal status, Furniture state, Price.

## 3.4. Lược đồ chuẩn và tầng ingestion

Mọi nguồn được ép về một lược đồ chuẩn 22 trường trước khi vào Kafka. Nhờ vậy Spark chỉ phải hiểu một lược đồ duy nhất, và thêm nguồn mới sau này chỉ tốn một hàm chuyển đổi.

**Quy ước đơn vị — thống nhất toàn hệ thống:**

| Trường | Đơn vị |
|---|---|
| `area`, `frontage`, `access_road` | m |
| `price` | tỷ VND |
| `price_per_m2` | triệu VND/m² |

Hai trường thời gian được phân biệt rõ ràng, và sự phân biệt này quyết định bài toán 3 có dữ liệu hay không:

- `crawled_at` — thời điểm **hệ thống tải** tin về. Một phiên crawl cho giá trị giống hệt nhau ở mọi tin.
- `posted_at` — thời điểm tin được **đăng**, do site công bố. Kho tin của site trải nhiều tuần, nên một phiên crawl đã dựng được một chuỗi thời gian.

Dữ liệu Kaggle có `crawled_at = NULL` một cách **tường minh**, vì nguồn thực sự không có mốc thời gian. Đây là quyết định thiết kế có chủ ý: để `NULL` thì mọi bước sau đều biết rằng thông tin này không tồn tại, thay vì điền thời điểm nạp và tạo ra một trục thời gian giả.

## 3.5. Chuẩn hóa địa chỉ tiếng Việt

Đây là module kỹ thuật khó nhất của đồ án, và cũng là module quyết định chất lượng của mọi thứ phía sau: vị trí là đặc trưng có sức dự đoán mạnh nhất của bài toán giá nhà (xem mục 4.4), còn heatmap thì đơn giản là không vẽ được nếu không quy được địa chỉ về mã hành chính.

### 3.5.1. Bản chất vấn đề

Người đăng tin gõ địa chỉ tự do. Các biến thể **thực tế đã gặp** cho cùng một đơn vị hành chính:

```
"Quận 7"   "Q.7"   "Q7"   "quan 7"
"Hồ Chí Minh"   "TP.HCM"   "TPHCM"   "Ho Chi Minh"   "Sài Gòn"   "HCM"
"Bình Thạnh"   "binh thanh"   "Q. Bình Thạnh"
```

### 3.5.2. Ba hệ quy chiếu hành chính cùng tồn tại

Phát hiện quan trọng nhất của giai đoạn ETL: dữ liệu không nằm trong hai hệ quy chiếu như dự đoán ban đầu, mà **ba**.

| Mốc | Đặc điểm | Xuất hiện trong dữ liệu |
|---|---|---|
| **Trước 2021** | Quận 2, Quận 9, quận Thủ Đức tồn tại riêng | 815 tin (2,67%) |
| **2021 – 06/2025** | 63 tỉnh, có cấp quận/huyện. **Hệ quy chiếu chính của đồ án** | 29.511 tin (96,68%) |
| **Từ 01/07/2025** | 34 tỉnh, **bỏ cấp huyện**, địa chỉ chỉ còn 2 cấp | 165 tin (0,54%) |

Nhóm thứ ba nhỏ nhưng **đang lớn dần**: toàn bộ 297 tin crawl năm 2026 đều thuộc nhóm này. Dữ liệu Kaggle (thu thập 2024) gần như không có, nhưng mọi tin mới từ nay trở đi đều sẽ ghi theo hệ mới. Tầng cầu nối không phải giải pháp tạm thời cho dữ liệu cũ mà là **thành phần thường trực** của pipeline.

Nghị quyết 1111/NQ-UBTVQH14 (2021) hợp nhất Quận 2, Quận 9 và quận Thủ Đức thành thành phố Thủ Đức. Các tin rao cũ vẫn ghi "Quận 2" nhưng đơn vị đó không còn trong bảng tham chiếu hiện hành.

Cải cách 01/07/2025 nghiêm trọng hơn: nó **bỏ hẳn cấp trung gian** mà toàn bộ mô hình và heatmap của đồ án đang dùng làm đơn vị phân tích.

### 3.5.3. Chiến lược khớp ba tầng

Địa chỉ được tách theo dấu phẩy và quét **từ phải sang trái**, vì thành phần hành chính ở Việt Nam nằm ở cuối chuỗi theo thứ tự phường → quận → tỉnh.

| Tầng | Cách khớp | Kết quả |
|---|---|---|
| 1. `old` | Khớp trực tiếp bảng tham chiếu hiện hành, sau khi bỏ dấu và bỏ tiền tố "quận/huyện/thành phố" | 96,68% |
| 1b. `dissolved` | Tra bảng đơn vị đã giải thể (Quận 2, Quận 9 → Thủ Đức) | 2,67% |
| 2. `bridge2025` | Từ tên phường mới, tra bảng cầu nối để suy ngược ra quận cũ | 0,54% |
| 3. `unique` | Tên quận duy nhất trên toàn quốc, dù không xác định được tỉnh | 0,01% |
| — | Không khớp | 0,10% (30 dòng rác thật sự) |

Cột `geo_source` ghi lại tầng nào đã khớp cho từng dòng. Đây không phải chi tiết thừa: nó cho phép báo cáo tách kết quả theo từng cách khớp, và cho phép phát hiện khi một tầng đột nhiên xử lý quá nhiều dòng — dấu hiệu bảng tham chiếu đã lỗi thời.

### 3.5.4. Một cách làm sai đã bị loại bỏ

Phiên bản đầu của bảng cầu nối 2025 được dựng bằng cách khớp **tên phường mới với tên đơn vị cũ**. Cách này cho ra một ánh xạ **sai một cách tự tin**: "Phường Thanh Xuân" (nội thành Hà Nội, mặt bằng 515 triệu/m²) bị gán về "Huyện Sóc Sơn" (ngoại thành, cách 40 km) chỉ vì trùng tên với một xã cũ ở đó.

Đây là dạng lỗi nguy hiểm nhất trong xử lý dữ liệu: hệ thống không báo lỗi, tỷ lệ khớp tăng lên trông rất đẹp, nhưng dữ liệu bị nhiễm bẩn. Bảng cầu nối được thay bằng bảng chuyển đổi chính thức xây từ các nghị quyết sáp nhập, phủ 3.308 phường/xã.

**Hạn chế còn lại:** 9,5% phường mới được ghép từ nhiều quận cũ khác nhau; với nhóm này nhóm lấy quận chiếm đa số và ghi lại độ tin cậy. Ánh xạ là **xấp xỉ, không phải song ánh**.

### 3.5.5. Kết quả

| Cấp | Trước khi xử lý đơn vị giải thể | Sau |
|---|---|---|
| Tỉnh/thành | 99,99% | 99,99% |
| **Quận/huyện** | 97,56% | **99,90%** |
| Phường/xã | 89,74% | **91,63%** |

Ngưỡng chấp nhận đặt ra ban đầu là 90% cho cấp quận/huyện; kết quả đạt **99,90%**, với điểm khớp trung bình 99,9/100.

**Một hệ quả phải nêu:** việc gộp Quận 2 (Thảo Điền, An Phú — mặt bằng giá rất cao) với Quận 9 (thấp hơn nhiều) vào Thủ Đức làm **mất độ phân giải giá** ở khu vực đó. Đây là hệ quả của thực tế hành chính chứ không phải lỗi xử lý, nhưng người đọc heatmap cần biết.

## 3.6. Cold path — ETL theo Medallion

### 3.6.1. Kafka → Bronze

Đọc Kafka ở chế độ **batch** (earliest → latest) chứ không streaming: cold path chạy theo lịch nên batch phù hợp hơn và dễ chạy lại khi lỗi. Bronze được phân vùng theo ngày nạp, cho phép nạp thêm hằng ngày mà không đọc lại toàn bộ.

Lược đồ Kafka được **khai báo tường minh** thay vì để Spark tự suy: dữ liệu từ Kafka có thể có batch mà toàn bộ một cột đều `NULL`, và suy kiểu sẽ cho lược đồ khác nhau giữa các lần chạy, dẫn tới lỗi khi ghi vào cùng thư mục Parquet.

### 3.6.2. Bronze → Silver

Bốn bước: khử trùng lặp theo `listing_id` (giữ bản mới nhất) → chuẩn hóa địa chỉ bằng UDF → tính lại đơn giá → lọc dữ liệu phi vật lý.

**Nguyên tắc lọc:** chỉ loại dữ liệu **không thể dùng được** (thiếu giá hoặc diện tích, đơn giá ngoài khoảng 1–5.000 triệu/m² — ngưỡng rộng chỉ để bắt lỗi nhầm đơn vị đồng/triệu/tỷ). **Tuyệt đối không loại tin "giá trông có vẻ sai"**: phát hiện giá bất thường là nhiệm vụ của bài toán 5, lọc ở đây sẽ xoá mất chính thứ cần học.

### 3.6.3. Silver → Gold

Tổng hợp theo ba cấp hành chính, dùng **trung vị** chứ không phải trung bình cho mọi chỉ số giá. Phân phối giá bất động sản lệch phải rất mạnh: vài căn giá trăm tỷ sẽ kéo trung bình của cả quận lên sai lệch. Trung bình vẫn được tính và lưu, nhưng chỉ để đối chiếu.

Đơn vị hành chính có dưới **30 tin rao** bị loại khỏi heatmap (trung vị trên mẫu quá nhỏ sẽ gây hiểu sai) nhưng **không bị xoá khỏi Silver** — mô hình vẫn học được từ chúng. Kết quả: 400 trong 1.215 đơn vị hành chính xuất hiện trong dữ liệu đủ điều kiện lên bản đồ.

### 3.6.4. Một lỗi tinh vi của cột phân vùng

Silver được phân vùng theo `province_code`. Mã tỉnh Việt Nam có **số 0 đứng đầu** ("01" = Hà Nội). Mặc định Spark đọc thư mục `province_code=01`, suy kiểu ra INT, và giá trị trở thành `1`.

Hệ quả: tầng Gold ghi `area_code = "1"` trong khi GeoJSON và bảng tham chiếu dùng `"01"` → **bản đồ cấp tỉnh trống trơn, và không có lỗi nào được báo**. Khắc phục bằng `spark.sql.sources.partitionColumnTypeInference.enabled=false`, đặt ở hàm khởi tạo SparkSession dùng chung để mọi job đọc Silver đều được bảo vệ.

## 3.7. Hot path — Spark Structured Streaming

Hai truy vấn chạy song song trên cùng một topic Kafka.

**Truy vấn A — vận tốc đăng tin.** Cửa sổ trượt 1 giờ, bước 5 phút, nhóm theo quận. Đếm tin mới và tính đơn giá trung bình trong cửa sổ. Watermark 2 giờ để giới hạn kích thước trạng thái.

**Truy vấn B — cảnh báo tin lệch giá.** Mỗi tin mới được join với bảng **tĩnh** chứa mặt bằng giá từng quận (lấy từ tầng Gold). Lệch quá 60% thì đẩy cảnh báo vào Redis và vào topic Kafka `realestate.alerts`.

Tách làm hai truy vấn vì Structured Streaming không cho phép nhiều phép tổng hợp nối tiếp trong cùng một luồng. Quan trọng hơn: truy vấn B **không tổng hợp** nên độ trễ của nó chỉ bằng thời gian một micro-batch, không phải chờ cửa sổ đóng — đúng thứ cần cho cảnh báo.

**Một khác biệt có chủ ý so với cold path:** hot path dùng **trung bình**, cold path dùng **trung vị**. Không phải tùy tiện: trạng thái của phép tổng hợp streaming phải cập nhật được tăng dần, mà trung vị thì không — Spark không hỗ trợ percentile trong streaming aggregation. Trung vị vẫn là con số chính thức tính ở cold path; trung bình trong cửa sổ chỉ để bắt xu hướng ngắn hạn.

**Thời gian sự kiện** lấy từ `crawled_at` của tin, không phải thời điểm Kafka nhận được. Khi replay lịch sử, hai mốc này lệch nhau hàng tháng; dùng nhầm thì mọi tin rơi vào cùng một cửa sổ và biểu đồ vận tốc thành vô nghĩa.

**Cấu trúc dữ liệu trong Redis:**

| Khóa | Kiểu | Nội dung |
|---|---|---|
| `hot:velocity:{mã quận}` | Hash | Số tin, đơn giá trung bình/nhỏ nhất/lớn nhất, mốc cửa sổ |
| `hot:top_districts` | Sorted Set | Xếp hạng quận theo số tin — dashboard lấy top bằng **một** lệnh |
| `hot:alerts` | List (giới hạn 200) | Hàng đợi cảnh báo, mới nhất trước |
| `hot:alerts:total` | Counter | Tổng số cảnh báo đã phát |

## 3.8. Tầng phục vụ ba lớp

**FastAPI (cổng 8000)** — suy luận mô hình. Bắt buộc là Python vì mô hình là XGBoost/scikit-learn.

Nguyên tắc quan trọng nhất của tầng này: **một nguồn đặc trưng duy nhất**. API **không** tự tính đặc trưng mà gọi đúng module `ml/features.py` đã dùng lúc huấn luyện. Viết lại logic đặc trưng ở tầng serving là cách kinh điển để mô hình lệch âm thầm giữa train và production — mọi thứ vẫn chạy, chỉ là dự đoán sai.

**Spring Boot Gateway (cổng 8090)** — ba việc mà không tầng nào khác làm được:

1. **Gộp nguồn.** Endpoint `/api/dashboard/overview` gộp dữ liệu từ PostgreSQL (KPI, top quận, phân cụm) và Redis (hot path) thành một response.
2. **Cache.** Tầng Gold chỉ đổi khi job Spark chạy; cache 60 giây ở gateway phục vụ mọi client.
3. **Che topology.** Frontend không cần biết Redis hay FastAPI tồn tại.

Gateway được thiết kế **suy biến có kiểm soát**: nếu Redis hỏng, khối hot path trả về rỗng kèm cờ báo, còn phần Gold vẫn hiện đủ. Một dịch vụ hỏng không được làm trắng cả màn hình.

**React Dashboard (cổng 5173)** — bảy tab tương ứng năm bài toán, hot path và chất lượng dữ liệu. Bundle tĩnh phục vụ bằng nginx, `/api` proxy sang gateway.

## 3.9. Dữ liệu ranh giới hành chính cho bản đồ

Nguồn: bộ dữ liệu ranh giới hai cấp `dvhcvn`, trích từ hệ thống bản đồ hành chính nhà nước.

**Lý do chọn nguồn này** thay vì GADM hay các bộ GIS phổ biến hơn: mã đơn vị của nó là **mã Tổng cục Thống kê**, đúng bộ mã mà tầng Silver đang dùng. Các nguồn khác đánh mã riêng (GID_1/GID_2 của GADM) và phải ghép theo **tên** — mà ghép theo tên tiếng Việt có dấu giữa hai bộ dữ liệu độc lập chính là cái bẫy đã mô tả ở mục 3.5.4.

Dữ liệu gốc khoảng 30 MB, quá nặng cho trình duyệt. Nhóm cài đặt thuật toán giản lược Douglas–Peucker (bản lặp, không đệ quy — bờ biển Việt Nam có vòng ranh giới hàng chục nghìn đỉnh sẽ làm tràn ngăn xếp).

| Tệp | Số đối tượng | Dung lượng | Dung sai |
|---|---|---|---|
| `provinces.geojson` | 63 tỉnh/thành | 164 KB | 0,01° (~1,1 km) |
| `districts.geojson` | 705 quận/huyện | 1.166 KB | 0,004° (~440 m) |

Số đỉnh cấp tỉnh giảm từ 170.425 xuống 5.072 (giữ 3,0%). Đối chiếu mã: **694/696 mã quận khớp (99,7%)**.

Bước đối chiếu mã này là kiểm tra quan trọng nhất của script sinh GeoJSON: nếu hai bộ mã lệch nhau, bản đồ sẽ hiện ra trắng trơn mà không có lỗi nào.

<!-- PAGEBREAK -->

# CHƯƠNG 4. XÂY DỰNG VÀ TỐI ƯU HÓA MÔ HÌNH HỌC MÁY

## 4.1. Bài toán 1 — Dự đoán giá: thiết kế

### 4.1.1. Chọn biến mục tiêu

Mô hình dự đoán **log(đơn giá theo m²)**, không phải tổng giá. Hai lý do độc lập:

**Lấy log.** Giá bất động sản có phân phối đuôi dài rất nặng. Huấn luyện trực tiếp trên giá khiến hàm mất mát bị chi phối bởi nhóm đắt nhất; log đưa phân phối về gần chuẩn và biến sai số tuyệt đối thành **sai số tương đối** — đúng thứ người mua nhà quan tâm ("sai 10%" có nghĩa, "sai 800 triệu" thì tùy căn).

**Dùng đơn giá thay vì tổng giá.** Vì $price \approx area \times \text{đơn giá}$, nếu dự đoán tổng giá thì diện tích chiếm gần hết sức giải thích và mô hình chỉ học lại một phép nhân. Dự đoán đơn giá buộc mô hình học phần khó: vị trí, pháp lý, hình dạng lô đất. Nhân ngược với diện tích để ra giá cuối.

### 4.1.2. Feature engineering — 20 đặc trưng, 6 nhóm

| Nhóm | Đặc trưng | Ghi chú thiết kế |
|---|---|---|
| **1. Vị trí** | `district_price_level` | Target encoding theo trung vị đơn giá quận, tính **out-of-fold** |
| **2. Quy mô** | `log_area`, `floors`, `bedrooms`, `bathrooms`, `area_per_bedroom`, `rooms_total` | `area_per_bedroom` tách được nhà trọ cho thuê (30 phòng/120 m²) khỏi nhà ở |
| **3. Hình học lô đất** | `frontage`, `access_road`, `frontage_ratio`, `is_car_access`, `is_wide_road` | Nhóm **đặc thù Việt Nam** |
| **4. Pháp lý & tiện nghi** | `legal_rank`, `furniture_rank` | Mã hóa **thứ bậc**, không one-hot |
| **5. Phong thủy** | `house_dir_sin/cos`, `balcony_dir_sin/cos` | Mã hóa **vòng** |
| **6. Tương tác chéo** | `district_x_area`, `district_x_legal` | Cho sẵn tương tác mà cây phải tốn nhiều nhánh mới diễn tả được |

**Ba quyết định thiết kế đáng nêu:**

**(a) Target encoding out-of-fold.** Nếu mã hóa mỗi quận bằng trung vị tính trên **toàn bộ** dữ liệu thì giá của chính dòng đang huấn luyện đã góp phần tạo ra đặc trưng của nó. Mô hình sẽ học được một "đặc trưng vị trí" chứa sẵn đáp án, cho R² đẹp giả tạo lúc validate rồi sụp khi gặp dữ liệu thật. Tính theo K-fold (mỗi dòng lấy giá trị từ các fold **khác**) loại bỏ rò rỉ này. Bảng tra dùng lúc suy luận thì tính trên toàn bộ train — lúc đó không còn rò rỉ vì dữ liệu suy luận nằm ngoài tập này.

**(b) Mã hóa vòng cho hướng nhà.** Hướng là biến vòng: Bắc (0°) và Tây Bắc (315°) chỉ cách nhau 45°, nhưng one-hot coi chúng xa nhau như mọi cặp khác. Mã hóa $(\sin\theta, \cos\theta)$ giữ được cấu trúc vòng và chỉ tốn 2 cột thay vì 8.

**(c) Mã hóa thứ bậc cho pháp lý.** Pháp lý ở Việt Nam có thứ tự tự nhiên về mức độ an toàn: sổ đỏ/sổ hồng (4) > giấy tờ hợp lệ (3) > hợp đồng mua bán (2) > đang chờ sổ (1) > giấy tay/chưa sổ (0). Mã hóa thứ bậc truyền thông tin đó bằng **một** cột thay vì bắt mô hình tự suy từ one-hot.

**Ngưỡng 4 m và 8 m** trong `is_car_access` / `is_wide_road` không phải chọn tùy ý: dưới 4 m ô tô không vào được, dưới 8 m hai ô tô không tránh nhau — hai ranh giới giá rất rõ trên thị trường Việt Nam.

### 4.1.3. Khung thực nghiệm bốn giai đoạn

Mục đích của khung này không phải "tìm mô hình tốt nhất" mà là **chứng minh bằng số liệu** từng bước phức tạp hóa có đáng hay không.

| Giai đoạn | Nội dung |
|---|---|
| 1. Baseline | Linear Regression, Random Forest, XGBoost, LightGBM, CatBoost — tham số mặc định |
| 2. Naive Ensemble | Trung bình đơn giản của ba mô hình gradient boosting |
| 3. Deep Tuning | Optuna (TPE), 50 lần thử mỗi mô hình, log toàn bộ vào MLflow |
| 4. Advanced Ensemble | Stacking với meta-model Ridge trên các mô hình đã tinh chỉnh |

### 4.1.4. Chiến lược đánh giá

Bốn chỉ số: RMSE, MAE, R² và **MAPE**. MAPE được thêm vào vì nó là chỉ số duy nhất người mua nhà hiểu được ngay: "sai số trung bình 19,6%".

Ngoài chia ngẫu nhiên thông thường, nhóm còn chạy **kiểm tra theo nhóm quận** (GroupSplit): các quận trong tập kiểm tra hoàn toàn không xuất hiện khi huấn luyện. Phép thử này trả lời một câu hỏi mà chia ngẫu nhiên không trả lời được: mô hình có thực sự học được quy luật định giá, hay chỉ ghi nhớ mặt bằng giá của từng quận?

## 4.2. Bài toán 1 — Kết quả

### 4.2.1. Bảng so sánh trên 30.524 tin rao thật

Tập huấn luyện 24.419 dòng, tập kiểm tra 6.105 dòng, chia ngẫu nhiên với seed cố định.

| GĐ | Mô hình | MAPE | MAE | R² (log) | R² (giá) | RMSE | Độ trễ |
|---|---|---|---|---|---|---|---|
| 1 | LinearRegression | 24,14% | 1,257 tỷ | 0,776 | 0,406 | 2,452 tỷ | 0,65 ms |
| 1 | RandomForest | 19,81% | 1,069 tỷ | 0,825 | 0,435 | 2,391 tỷ | 38,4 ms |
| 1 | XGBoost | 20,27% | 1,105 tỷ | 0,826 | 0,298 | 2,666 tỷ | 3,78 ms |
| 1 | LightGBM | 20,23% | 1,096 tỷ | 0,828 | 0,453 | 2,353 tỷ | 0,58 ms |
| 1 | CatBoost | 20,01% | 1,081 tỷ | 0,833 | 0,522 | 2,198 tỷ | 0,34 ms |
| 2 | Naive ensemble | 19,79% | 1,074 tỷ | 0,835 | 0,460 | 2,337 tỷ | — |
| 3 | CatBoost + Optuna | 19,94% | 1,083 tỷ | 0,831 | 0,456 | 2,345 tỷ | 0,37 ms |
| 3 | LightGBM + Optuna | 19,51% | 1,055 tỷ | 0,836 | 0,484 | 2,285 tỷ | 1,28 ms |
| **3** | **XGBoost + Optuna** ← production | **19,43%** | 1,058 tỷ | **0,838** | 0,445 | 2,369 tỷ | 3,31 ms |
| 4 | Stacking (Ridge meta) | 19,40% | 1,052 tỷ | 0,838 | 0,486 | 2,281 tỷ | 36,1 ms |

**Mô hình được chọn cho production: XGBoost + Optuna** (đăng ký MLflow Registry là `housing-price-model` v4).

Bảng cho thấy rõ giá trị của từng giai đoạn: tinh chỉnh Optuna kéo XGBoost từ 20,27% xuống 19,43% — cải thiện 0,84 điểm, lớn hơn nhiều so với chênh lệch giữa các thuật toán ở giai đoạn 1. Nói cách khác, **chọn siêu tham số quan trọng hơn chọn thư viện**.

Hai cột R² được báo cáo riêng, và khoảng cách giữa chúng chính là nội dung mục 4.2.5.

### 4.2.2. Quyết định loại Stacking — và quy tắc đằng sau nó

Stacking cho MAPE tốt hơn XGBoost đã tinh chỉnh đúng **0,027 điểm** (19,40% so với 19,43%), đồng thời suy luận **chậm hơn 10,9 lần** (36,1 ms so với 3,31 ms). Đây là một kết quả đáng giá: giai đoạn phức tạp nhất của khung thực nghiệm mang lại một cải thiện nhỏ hơn cả dao động giữa hai lần chạy.

Script tự động áp dụng quy tắc: **chỉ nhận mô hình phức tạp hơn khi nó hơn trên 0,5 điểm MAPE và chậm dưới 3 lần**. Quy tắc này được đặt **trước** khi biết kết quả, để tránh việc nhìn số rồi hợp lý hóa lựa chọn. Stacking trượt cả hai điều kiện.

### 4.2.3. Kiểm tra độ bền trên quận chưa từng thấy

| Cách chia | MAPE |
|---|---|
| Ngẫu nhiên | 19,43% |
| **Theo nhóm quận (quận mới hoàn toàn)** | **23,61% (±2,23)** |

Chênh lệch 4,2 điểm cho thấy mức phụ thuộc thực tế vào đặc trưng vị trí. Con số này quan trọng hơn MAPE ngẫu nhiên khi trả lời câu hỏi "hệ thống dùng được cho địa bàn chưa có dữ liệu không": câu trả lời là **có, với sai số cao hơn khoảng 4 điểm phần trăm**.

Độ lệch chuẩn giữa các fold (±2,23) cũng đáng chú ý: sai số dao động khá mạnh tùy nhóm quận nào bị giữ lại — dấu hiệu cho thấy một số địa bàn có quy luật định giá riêng mà mô hình không suy ra được từ các quận khác.

### 4.2.4. Tầm quan trọng của đặc trưng

| Hạng | Đặc trưng | Độ quan trọng | Nhóm |
|---|---|---|---|
| 1 | `district_price_level` | 0,359 | Vị trí |
| 2 | `log_area` | 0,174 | Quy mô |
| 3 | `floors` | 0,094 | Quy mô |
| 4 | `district_x_legal` | 0,083 | Tương tác chéo |
| 5 | `district_x_area` | 0,037 | Tương tác chéo |
| 6 | `area_per_bedroom` | 0,030 | Quy mô |
| 7 | `legal_rank` | 0,026 | Pháp lý |
| 8 | `bathrooms` | 0,026 | Quy mô |

Vị trí chiếm hơn một phần ba toàn bộ sức giải thích — kết quả này xác nhận câu châm ngôn của ngành bất động sản, và cũng giải thích vì sao module chuẩn hóa địa chỉ ở mục 3.5 lại quan trọng đến vậy.

Đáng chú ý: **cả hai đặc trưng tương tác chéo đều lọt top 5**, cộng lại 0,120 — nhiều hơn `floors` đứng hạng ba. Việc cho sẵn tương tác "vị trí × pháp lý" và "vị trí × diện tích" thực sự tiết kiệm cho mô hình rất nhiều nhánh cây, đúng như giả thiết thiết kế ở mục 4.1.2.

### 4.2.5. Một phát hiện: 1% dữ liệu thật làm RMSE tăng 75%

Đây là kết quả có giá trị nhất của chương này, và nó chỉ lộ ra khi đưa dữ liệu crawl thật vào cùng tập huấn luyện.

| Nguồn | Số tin | Khoảng giá |
|---|---|---|
| Kaggle (đã lọc sẵn) | 30.227 | 1,00 – 11,50 tỷ |
| **alonhadat (crawl thật)** | **297** | **0,26 – 175,00 tỷ** |

Chỉ **297 tin — chưa tới 1% dữ liệu** — nhưng chúng mang theo dải giá thật của thị trường. Huấn luyện lại với chúng:

| Chỉ số | Chỉ Kaggle (30.227) | Thêm 297 tin thật (30.524) | Thay đổi |
|---|---|---|---|
| MAPE | 19,59% | **19,43%** | tốt hơn 0,16 điểm |
| R² trên thang log | 0,831 | **0,838** | tốt hơn |
| **R² trên thang giá** | **0,626** | **0,445** | **tệ hơn 29%** |
| **RMSE** | **1,351 tỷ** | **2,369 tỷ** | **tệ hơn 75%** |
| MAPE trên quận chưa thấy | 22,72% | 23,61% | tệ hơn 0,89 điểm |

Hai nhóm chỉ số đi ngược chiều nhau, và đó chính là điểm mấu chốt:

- **MAPE và R²(log) là chỉ số tương đối.** Sai 20% trên căn 5 tỷ và sai 20% trên căn 100 tỷ đóng góp như nhau. Chúng thậm chí **tốt lên**, vì dữ liệu thật cho mô hình thêm thông tin.
- **RMSE và R²(giá) là chỉ số tuyệt đối.** Chúng bị chi phối bởi vài chục tin giá rất cao mà mô hình chưa từng thấy dạng đó trong 30.000 mẫu Kaggle.

Ba kết luận:

1. **Giới hạn (2) ở mục 1.4 không phải cảnh báo lý thuyết mà là hiệu ứng đo được.** Dataset Kaggle bị lọc còn 1–11,5 tỷ; mô hình huấn luyện trên đó có R²(giá) 0,626 nhìn rất đẹp, nhưng con số đó chỉ đúng trong một thị trường không tồn tại — thị trường không có căn nào trên 11,5 tỷ.
2. **Báo cáo một chỉ số duy nhất là không đủ.** Nếu chỉ báo cáo MAPE, việc thêm dữ liệu thật trông như cải thiện thuần túy. Nếu chỉ báo cáo RMSE, nó trông như thảm họa. Cả hai đều đúng, cho hai câu hỏi khác nhau.
3. **Cách sửa nằm ở dữ liệu, không nằm ở mô hình.** Không siêu tham số nào bù được cho một khoảng giá vắng mặt trong tập huấn luyện.

### 4.2.6. Đồ án có đủ dữ liệu chưa? — trả lời bằng đường cong học

"30.524 tin rao có đủ không?" là câu hỏi không trả lời được nếu hỏi trống không. Đủ hay không phụ thuộc vào một câu hỏi cụ thể hơn: **nếu có gấp đôi dữ liệu thì MAPE giảm bao nhiêu?**

Đường cong học trả lời trực tiếp. Huấn luyện lại XGBoost với siêu tham số **giữ cố định** trên 10%, 25%, 50%, 75% và 100% tập train, đo trên **cùng một tập kiểm tra không đổi**. Mã hóa vị trí được tính lại từ đầu ở từng mốc — dùng chung bảng mã hóa của toàn bộ train sẽ khiến mốc 10% hưởng lợi từ 90% còn lại và đường cong phẳng một cách giả tạo.

| Dữ liệu huấn luyện | Số dòng | Số quận nhìn thấy | MAPE |
|---|---|---|---|
| 10% | 2.441 | 134 | 24,00% |
| 25% | 6.104 | 162 | 21,65% |
| 50% | 12.209 | 180 | 20,61% |
| 75% | 18.314 | 196 | 19,70% |
| **100%** | **24.419** | **208** | **19,42%** |

Bảng trên đã cho thấy đà giảm, nhưng cách đọc thuyết phục nhất là nhìn **lợi ích biên trên cùng một lượng dữ liệu thêm vào**. Ba chặng cuối mỗi chặng thêm đúng 6.105 dòng:

| Chặng | Dữ liệu thêm | MAPE giảm |
|---|---|---|
| 25% → 50% | +6.105 dòng | 1,04 điểm |
| 50% → 75% | +6.105 dòng | 0,91 điểm |
| 75% → 100% | +6.105 dòng | **0,28 điểm** |

Cùng một lượng dữ liệu bổ sung, lợi ích sụp từ 1,04 xuống 0,28 điểm. Ngoại suy theo đà này, **gấp đôi dữ liệu chỉ đổi lấy khoảng 0,3–0,6 điểm MAPE**.

**Kết luận, và nó quyết định hướng phát triển ở Chương 6:**

1. **Số lượng dữ liệu KHÔNG còn là nút thắt** của bài toán dự đoán giá. Thu thập thêm tin rao *cùng loại* là công sức bỏ ra không tương xứng.
2. **Nút thắt nằm ở ĐẶC TRƯNG.** Vị trí — đặc trưng mạnh nhất, chiếm 0,359 độ quan trọng — hiện chỉ là **một con số cho cả quận**. Nhưng đo trên dữ liệu thật, tỷ lệ p90/p10 của đơn giá *trong cùng một quận* trung bình là **2,9 lần** (Quận 1 lên tới 3,8 lần). Mô hình về mặt cấu trúc không thể giải thích phần biến thiên đó: mọi bất động sản trong Quận 1 đều nhận cùng một giá trị vị trí.
3. Vì vậy hướng cải thiện đúng là **thêm chiều không gian** (toạ độ, khoảng cách tới trung tâm / metro / trường học), không phải thêm dòng. Đây là cơ sở định lượng cho đề xuất ở mục 6.3.

Cần phân biệt kết luận này với mục 4.2.5. Ở đó, thêm 297 tin đã làm RMSE xấu đi 75% — nhưng vì chúng mở rộng **dải giá**, chứ không phải vì chúng thêm số lượng. Hai mục trả lời hai câu khác nhau: *"thêm dòng cùng loại"* (mục này — vô ích) và *"thêm dòng khác phân khúc"* (4.2.5 — lộ ra giới hạn thật của tập dữ liệu).

## 4.3. Bài toán 2 — Heatmap giá

Tầng Gold tổng hợp trung vị đơn giá, trung bình, tứ phân vị 25/75, trung vị diện tích và số tin theo ba cấp hành chính. Dashboard vẽ choropleth bằng Leaflet, ghép GeoJSON với dữ liệu giá **bằng mã hành chính**, không bằng tên.

**Hai quyết định trực quan hóa cần giải thích:**

**Chia lớp màu theo phân vị, không chia đều khoảng giá.** Giá bất động sản lệch phải rất mạnh; chia đều khoảng sẽ dồn 95% quận vào lớp màu nhạt nhất và để vài quận trung tâm chiếm hết dải màu — bản đồ trông như cả nước đồng giá. Chia theo phân vị đảm bảo mỗi lớp có số quận tương đương.

**Đơn vị dưới 30 tin để màu xám, không phải màu thấp nhất.** "Không có dữ liệu" khác hẳn "giá thấp"; dùng cùng thang màu cho hai trạng thái này là một lỗi trình bày nghiêm trọng.

**Top 5 quận theo trung vị đơn giá:**

| Quận/huyện | Trung vị (triệu/m²) | Số tin |
|---|---|---|
| Quận 5 (TP.HCM) | 179,0 | 69 |
| Quận Cầu Giấy (Hà Nội) | 178,3 | 573 |
| Quận 1 (TP.HCM) | 178,1 | 120 |
| Quận Hoàn Kiếm (Hà Nội) | 177,8 | 52 |
| Quận Ba Đình (Hà Nội) | 176,7 | 550 |

**Chỉ số tổng quan thị trường:**

| Chỉ số | Giá trị |
|---|---|
| Tin rao đã xử lý | 30.524 |
| Trung vị đơn giá | 101,82 triệu/m² |
| Trung vị giá | 5,90 tỷ VND |
| Trung vị diện tích | 56 m² |
| Quận/huyện có dữ liệu | 213 |
| Tỉnh/thành có dữ liệu | 59 |

## 4.4. Bài toán 4 — Phân cụm khu vực

### 4.4.1. Thiết kế

KMeans trên vector đặc trưng cấp quận: trung vị đơn giá, diện tích trung bình, số tầng trung bình, tỷ lệ có sổ đỏ, tỷ lệ mặt tiền đường ≥ 4 m.

`n_listings` **cố ý bị loại** khỏi tập đặc trưng: nó phản ánh mức độ phủ của nguồn dữ liệu chứ không phải đặc điểm thị trường. Đưa vào sẽ gom nhóm theo "quận nào nhiều tin rao".

Số cụm được chọn bằng **Silhouette** làm tiêu chí chính, có tham chiếu Elbow và Calinski-Harabasz. Elbow trực quan nhưng chủ quan — "khuỷu tay" nằm ở đâu là do người đọc biểu đồ quyết định. Silhouette định lượng và có tối ưu rõ ràng.

### 4.4.2. Một lỗi đã phát hiện: đặc trưng không phân hoá làm hỏng phân cụm

`StandardScaler` gán trọng số **bằng nhau** cho mọi đặc trưng. Một đặc trưng gần như hằng số giữa các quận chỉ đóng góp nhiễu — nhưng sau khi chuẩn hóa, nhiễu đó được phóng đại lên đúng bằng tín hiệu giá.

Hệ quả cụ thể trên dữ liệu thử: quận Ba Đình (408 triệu/m²) bị xếp cùng nhóm với Biên Hòa (52 triệu/m²).

Khắc phục bằng bước **sàng lọc theo hệ số biến thiên**: loại đặc trưng có CV < 0,15. Sau khi thêm bước này, Silhouette tăng từ 0,226 lên 0,628 trên dữ liệu thử.

Trên dữ liệu thật, bộ sàng lọc giữ lại **4 trong 5** đặc trưng:

| Đặc trưng | CV | Kết luận |
|---|---|---|
| `mean_area` | 1,200 | giữ |
| `median_price_m2` | 0,590 | giữ |
| `pct_mat_tien` | 0,313 | giữ |
| `mean_floors` | 0,276 | giữ |
| `pct_red_book` | **0,148** | **loại — gần như hằng số** |

`pct_red_book` bị loại là hợp lý về mặt thị trường: hầu hết quận đều có tỷ lệ tin có sổ đỏ trên 90%, nên đặc trưng này gần như không phân biệt được địa bàn nào với địa bàn nào.

### 4.4.3. Kết quả trên 86 quận/huyện

**Chọn k — bảng đầy đủ để bảo vệ được lựa chọn:**

| k | Inertia | Silhouette | Calinski-Harabasz |
|---|---|---|---|
| 2 | 190,92 | 0,4579 | 67,35 |
| **3** | 118,91 | **0,4758** | 78,55 |
| 4 | 81,26 | 0,4057 | 88,38 |
| 5 | 62,84 | 0,4247 | 90,59 |
| 6 | 49,04 | 0,4602 | 96,22 |

Ba tiêu chí **không đồng thuận**, và đây là tình huống phải xử lý minh bạch chứ không giấu đi:

- **Inertia** giảm đơn điệu theo k — theo định nghĩa, nên tự nó không bao giờ chọn được k.
- **Calinski-Harabasz** tăng đơn điệu tới k=6, tức nó sẽ luôn đề nghị chia nhỏ thêm.
- **Silhouette** có cực đại rõ tại k=3, và đó là tiêu chí được chọn — vì nó là tiêu chí duy nhất phạt cả việc chia quá nhỏ.

| Chỉ số | Giá trị |
|---|---|
| Số cụm được chọn (k) | **3** |
| Silhouette score | **0,4758** |
| Phương sai PCA 2 chiều giữ lại | 84,0% |
| Chồng lấn giá giữa các cụm liền kề | 19% |
| Số quận/huyện | 86 |

| Cụm | Số quận | Trung vị đơn giá | Diện tích TB | Tỷ lệ có sổ |
|---|---|---|---|---|
| **Cao cấp** | 28 | 147 triệu/m² | 47 m² | 98% |
| **Trung cấp** | 57 | 56 triệu/m² | 108 m² | 90% |
| **Bình dân** | **1** | 20 triệu/m² | **1.117 m²** | 100% |

**Cụm thứ ba chỉ có đúng một quận — và đó là một phát hiện, không phải một kết quả.** Thành phố Cà Mau bị tách riêng vì diện tích trung bình 1.117 m², cao gấp mười lần mọi địa bàn khác. Con số đó gần như chắc chắn phản ánh **đất nền bị đăng vào danh mục nhà ở**, chứ không phải nhà ở Cà Mau rộng gấp mười lần nơi khác.

Hai điều rút ra:

1. KMeans rất nhạy với điểm ngoại lai: một quận đủ khác biệt sẽ tự chiếm nguyên một cụm và làm Silhouette tăng lên một cách giả tạo.
2. Phân cụm ở đây đang **hoạt động như một bộ phát hiện lỗi dữ liệu** — cụm singleton chỉ thẳng vào địa bàn cần kiểm tra lại ở tầng Silver. Đây là cầu nối tự nhiên sang bài toán 5.

So với lần chạy trước (chỉ dữ liệu Kaggle, 85 quận): k=2, Silhouette 0,4281. Thêm 297 tin thật làm xuất hiện cụm thứ ba nhưng cụm đó chỉ có một phần tử — tức **cấu trúc thị trường thực chất vẫn là hai nhóm**, kết luận này ổn định qua cả hai lần chạy.

## 4.5. Bài toán 3 — Dự báo xu hướng giá

### 4.5.1. Vấn đề dữ liệu, và một cám dỗ đã bị từ chối

Dataset tin rao chính **không có cột thời gian**. Cách dễ nhất để bài toán 3 "chạy được" là rải ngẫu nhiên 30.000 tin vào một trục 24 tháng rồi chạy Prophet. Biểu đồ sẽ rất đẹp, có xu hướng, có mùa vụ, có khoảng tin cậy — và **hoàn toàn vô nghĩa**, vì mọi cấu trúc thấy được đều do bộ sinh số ngẫu nhiên tạo ra.

Nhóm từ chối cách này. Script dựng chuỗi chỉ nhận ba nguồn có mốc thời gian **thật**:

| Nguồn | Mô tả | Trạng thái |
|---|---|---|
| `crawler_accumulated` | `posted_at` — ngày đăng tin do site công bố | Đang tích lũy |
| `kaggle_hcm` | Dataset chuỗi giá căn hộ TP.HCM theo tháng | Chưa tải được |
| `bds_index` | Chỉ số giá công bố công khai, nhập tay có trích dẫn | Chưa nhập |

Nếu không nguồn nào khả dụng, script **dừng và báo rõ thiếu gì**, chứ không tự chế dữ liệu.

### 4.5.2. Thiết kế mô hình

Ba mô hình, và mô hình thứ ba là điểm quan trọng nhất của thiết kế:

| Mô hình | Mô tả |
|---|---|
| **Naive** | Dự báo = giá trị quan sát cuối cùng. Không học gì cả |
| **Prophet** | Phân rã xu hướng + mùa vụ, có điểm gãy (changepoint) |
| **LSTM** | Mạng hồi quy 1 lớp 16 đơn vị, dự báo đệ quy |

**Naive không phải để cho đủ bảng.** Trên chuỗi giá tài sản ngắn, nó là đối thủ thật sự khó đánh bại: giá bất động sản gần với bước ngẫu nhiên, mà với bước ngẫu nhiên thì dự báo tối ưu **chính là** giá trị cuối cùng. Một báo cáo đưa ra "Prophet đạt MAPE 6%" mà không nói naive đạt bao nhiêu thì con số đó vô nghĩa — có thể naive đạt 5%.

**Cấu hình Prophet cho chuỗi ngắn:** tắt mùa vụ năm và tuần (chuỗi vài chục điểm không đủ để ước lượng chu kỳ năm; bật lên chỉ khiến mô hình khớp nhiễu và tự tin sai), hạ `changepoint_prior_scale` xuống 0,05 để tránh ngoại suy dốc đứng từ vài điểm cuối.

**Cấu hình LSTM:** cố ý rất nhỏ. Với vài chục điểm huấn luyện, mạng lớn hơn chỉ học thuộc lòng. Chuẩn hóa min-max tính theo tập **train của từng fold** — dùng min/max toàn chuỗi là rò rỉ thông tin tương lai vào quá khứ.

### 4.5.3. Đánh giá bằng backtest cuốn chiếu

Với chuỗi vài chục điểm, một lần chia train/test cho ra con số phụ thuộc hoàn toàn vào việc điểm cắt rơi vào đâu. Backtest cuốn chiếu cắt ở nhiều mốc liên tiếp, mỗi lần huấn luyện lại, rồi lấy trung bình — và luôn chỉ dùng quá khứ để dự báo tương lai.

Cửa sổ **mở rộng** (expanding) chứ không trượt: dữ liệu bất động sản ít, vứt bỏ phần đầu chuỗi để giữ cửa sổ cố định là lãng phí thông tin hiếm.

### 4.5.4. Trạng thái hiện tại

Toàn bộ khung mô hình và backtest đã cài đặt và kiểm thử (kiểm tra tự động xác nhận: trên chuỗi có xu hướng tăng rõ, Prophet phải thắng naive — nếu không thì hoặc backtest đang rò rỉ tương lai, hoặc mô hình không học được gì).

Trường `posted_at` đã được bổ sung xuyên suốt pipeline (crawler → lược đồ Kafka → Silver → PostgreSQL) và bóc được trên dữ liệu thật. Đo tại thời điểm viết báo cáo:

| Chỉ số | Giá trị đo được |
|---|---|
| Tin có `posted_at` trong Silver | 64 / 30.524 |
| Phạm vi thời gian | 16/07/2026 – 05/09/2026 (52 ngày) |
| Số ngày phân biệt | 18 |
| Chuỗi tuần sau khi lọc ≥5 tin/kỳ | **6 điểm, 3 địa bàn** |
| Chuỗi dài nhất (toàn quốc) | **4 điểm** — cần ≥ 12 |

Ba nguyên nhân, đều đo được chứ không phải phỏng đoán:

1. Trang danh sách của site **chỉ hiện ngày đăng trên khoảng 35% số tin**.
2. Trang danh sách chỉ phủ vài tuần gần nhất, không phải toàn bộ kho lưu trữ.
3. Site giới hạn tốc độ rất chặt: một phiên 99 trang ở nhịp 20 giây bị HTTP 429 chặn sau khoảng 16 trang. Tích lũy đủ chuỗi đòi hỏi nhiều phiên nhỏ chạy theo lịch trong vài tuần, không phải một phiên dài.

**Kết luận trung thực cho báo cáo:** bài toán 3 đã hoàn tất về phương pháp, cài đặt và đường dẫn dữ liệu — script dựng chuỗi chạy đúng, nhận ra chuỗi chưa đủ dài và **từ chối huấn luyện** thay vì cho ra một con số vô nghĩa. Nhưng **chưa có kết quả định lượng đáng tin cậy** do ràng buộc dữ liệu. Nhóm chọn báo cáo đúng như vậy thay vì đưa ra một MAPE tính trên dữ liệu bịa. Hạ tầng đã sẵn sàng; thứ còn thiếu duy nhất là thời gian tích lũy.

## 4.6. Bài toán 5 — Phát hiện tin rao bất thường

### 4.6.1. Ba tín hiệu và một phát hiện phản trực giác

Thiết kế ban đầu: kết hợp ba tín hiệu (phần dư mô hình giá, Isolation Forest, LOF) thành một điểm tổng hợp bằng trung bình có trọng số 0,5/0,3/0,2.

Để đo được chất lượng thay vì "nhìn bằng mắt", nhóm cho bộ sinh dữ liệu giả lập xuất **nhãn sự thật** ra một file **riêng biệt** — file này không bao giờ đi vào pipeline, nên không thể rò rỉ vào mô hình.

| Tín hiệu | PR-AUC | Precision@50 |
|---|---|---|
| **Phần dư mô hình giá** | **0,9924** | **100%** |
| Isolation Forest | 0,0270 | 2% |
| Local Outlier Factor | 0,0205 | 0% |
| Trung bình có trọng số (0,5/0,3/0,2) | 0,1740 | 26% |

Mức nền của đoán ngẫu nhiên là 0,0192.

Kết quả này rất rõ ràng: **trộn hai tín hiệu gần như nhiễu vào một tín hiệu gần hoàn hảo kéo PR-AUC từ 0,99 xuống 0,17**. Trực giác "kết hợp nhiều tín hiệu thì mạnh hơn" chỉ đúng khi các tín hiệu đều mang thông tin.

Điểm chính đã được đổi sang dùng **riêng phần dư**; IF và LOF giữ lại làm **cờ bổ sung** ở ngưỡng phân vị 97, không tham gia vào điểm chính.

**Giới hạn của kết luận này:** nó rút từ dữ liệu giả lập, nơi mọi bất thường đều là nhiễu loạn giá thuần túy. Trên dữ liệu thật còn có tin mô tả phi lý (nhà 30 phòng ngủ trên 80 m², đất 5.500 m² rao như nhà ở) — với dạng đó IF/LOF sẽ có giá trị. Vì vậy hai tín hiệu này được giữ lại chứ không xoá bỏ.

### 4.6.2. Kết quả trên dữ liệu thật

| Chỉ số | Giá trị |
|---|---|
| Tin bị gắn cờ | **916 / 30.524 (3,0%)** |
| Khoảng điểm bất thường | 97,0 – 100,0 |

**Năm tin đứng đầu danh sách:**

| Điểm | Giá rao | Mô hình dự đoán | Địa bàn | Diễn giải |
|---|---|---|---|---|
| 100,0 | 2,1 tỷ | 20,5 tỷ | TP. Đồng Xoài | thấp hơn 884% |
| 100,0 | 137,8 tỷ | 17,3 tỷ | Quận Sơn Trà | cao hơn 695% |
| 100,0 | 3,6 tỷ | 23,8 tỷ | TP. Bắc Ninh | thấp hơn 556% |
| 100,0 | 19,6 tỷ | 110,3 tỷ | Quận Tân Bình | thấp hơn 463% |
| 100,0 | 168,8 tỷ | 37,6 tỷ | TP. Bắc Ninh | cao hơn 349% |

Danh sách này minh họa đúng điểm mạnh của cách chấm theo phần dư: mỗi dòng đều **giải thích được** — người dùng thấy ngay giá rao lệch bao nhiêu so với mức mô hình ước tính cho cùng vị trí và cùng thuộc tính. Isolation Forest chỉ trả về một điểm số không nói được gì.

Ví dụ thực tế do hot path phát hiện: một tin rao **5.535 m² đất gần sân bay Phan Thiết**, đơn giá 1,1 triệu/m² so với mặt bằng 31 triệu/m² của địa bàn — lệch 96,5%. Đây là đất nền bị đăng nhầm vào danh mục nhà ở, tức một trường hợp **làm sạch dữ liệu ngược** chứ không phải gian lận.

### 4.6.3. Ứng dụng thực tế

Hai ứng dụng, và ứng dụng thứ hai quan trọng không kém:

1. **Lọc tin cho người mua** — cảnh báo tin có dấu hiệu giá mồi.
2. **Làm sạch dữ liệu ngược cho tầng Silver** — các tin bị gắn cờ vì lỗi nhập liệu (nhầm đơn vị, nhầm loại hình) chỉ ra chỗ cần bổ sung luật làm sạch.

## 4.7. Mục "Thử nghiệm chưa thành công"

Bốn hướng đã thử và không mang lại kết quả như kỳ vọng. Nhóm giữ lại toàn bộ vì chúng cho thấy quy trình có kiểm chứng.

### 4.7.1. Hai đặc trưng chết âm thầm suốt quá trình huấn luyện

Đây là lỗi nghiêm trọng nhất, và cũng khó phát hiện nhất.

Dataset ghi tình trạng pháp lý và nội thất bằng **tiếng Anh** (`Have certificate`, `Sale contract`, `Full`, `Basic`) trong khi bảng tra thứ bậc của nhóm chỉ có tiếng Việt. Hệ quả: `legal_rank` và `furniture_rank` là **NaN toàn bộ**, và `district_x_legal` chết theo.

Điều đáng sợ là: **mô hình vẫn chạy, vẫn cho ra MAPE trông hợp lý, và không có gì báo lỗi.** XGBoost xử lý giá trị khuyết một cách tự nhiên nên một cột toàn NaN chỉ đơn giản bị bỏ qua.

| | Trước khi sửa | Sau khi sửa |
|---|---|---|
| MAPE | 19,79% | **19,59%** |
| Hạng của `district_x_legal` | không có đóng góp | **thứ 4** (0,095) |

Khắc phục: bổ sung bảng tra song ngữ, và thêm **cảnh báo tự động** — khi một đặc trưng thứ bậc có giá trị đầu vào không rỗng nhưng không khớp được mục nào trong bảng tra, hàm in ra cảnh báo kèm năm giá trị hay gặp nhất. Lá chắn này ngăn lỗi cùng loại tái diễn âm thầm.

### 4.7.2. Stacking không đáng dùng

Đã trình bày ở mục 4.2.2. Giai đoạn 4 — phức tạp nhất của khung thực nghiệm — không cải thiện được gì so với giai đoạn 3.

### 4.7.3. Phân cụm với đặc trưng không phân hoá

Đã trình bày ở mục 4.4.2. `StandardScaler` phóng đại nhiễu của đặc trưng gần hằng số lên bằng tín hiệu thật.

### 4.7.4. Tổ hợp ba tín hiệu bất thường

Đã trình bày ở mục 4.6.1. Kết hợp tín hiệu chỉ có lợi khi các tín hiệu đều mang thông tin.

### 4.7.5. Một chỉ số đẹp hóa ra là ảo

Đã trình bày ở mục 4.2.5. R²(giá) 0,626 báo cáo ở lần chạy đầu là con số **đúng về mặt tính toán nhưng sai về mặt ý nghĩa**: nó đo trên một tập dữ liệu đã bị cắt mất phân khúc cao cấp. Thêm chưa tới 1% dữ liệu thật là nó tụt xuống 0,445.

Bài học rút ra không phải "mô hình tệ hơn ta tưởng" mà là: **một chỉ số chỉ có nghĩa trong phạm vi phân phối của tập kiểm tra sinh ra nó**. Đây là lý do mục 1.4 phải liệt kê giới hạn dữ liệu ngay từ chương đầu, chứ không để xuống phần kết luận.

<!-- PAGEBREAK -->

# CHƯƠNG 5. THỰC NGHIỆM VÀ ĐÁNH GIÁ KẾT QUẢ

## 5.1. Môi trường triển khai

| Hạng mục | Cấu hình |
|---|---|
| Nền tảng | Docker Compose — 16 định nghĩa dịch vụ, 15 container thường trú (`minio-init` là job chạy một lần rồi thoát) |
| Yêu cầu tối thiểu | 16 GB RAM (cụm đầy đủ dùng ~12 GB) |
| Spark | 3.5.9 standalone — 1 master + 2 worker, mỗi worker 4 core / 6 GB |
| Kafka | Redpanda v24.3.1, 1 broker |
| HDFS | Apache Hadoop 3.4.1, 1 namenode + 1 datanode |
| PostgreSQL | 16-alpine |
| MLflow | 2.19.0, backend PostgreSQL, artifact MinIO (S3) |

**Hai worker Spark** không phải để tăng tốc — trên máy đơn chúng chia nhau cùng CPU. Mục đích là chứng minh cơ chế phân tán hoạt động: job thực sự được chia thành task và phân phối tới nhiều executor.

## 5.2. Kịch bản kiểm chứng toàn hệ thống

| Bước | Lệnh | Kết quả kỳ vọng | Đạt |
|---|---|---|---|
| 1 | `docker compose up -d` | 12 container hạ tầng thường trú, healthy | ✅ |
| 2 | `replay_producer.py` | Kafka topic có message | ✅ 30.543 message |
| 3 | `batch_kafka_to_bronze.py` | HDFS `/lake/bronze` có Parquet | ✅ |
| 4 | `batch_bronze_to_silver.py` | Tỷ lệ khớp quận > 90% | ✅ **99,91%** |
| 5 | `batch_silver_to_gold.py` | Bảng Gold trong PostgreSQL | ✅ 400 đơn vị |
| 6 | `train_price.py --stage all` | MLflow có đủ 4 giai đoạn, 10 lần chạy | ✅ MAPE **19,43%** |
| 7 | `train_cluster.py` | Phân cụm có Silhouette | ✅ k=3, **0,4758** |
| 8 | `train_anomaly.py` | Danh sách tin bất thường | ✅ **916** tin |
| 9 | `streaming_hot_path.py --once` | Redis có key `hot:*` | ✅ 215 quận, 2.176 cảnh báo |
| 10 | `curl POST :8000/predict` | Trả giá dự đoán | ✅ 12 ms |
| 11 | `curl :8090/api/dashboard/overview` | Gộp PostgreSQL + Redis | ✅ |
| 12 | Mở `localhost:5173` | Bản đồ có màu, form trả kết quả | ✅ |

## 5.3. Kiểm thử đơn vị

Mỗi module có bộ kiểm tra chạy độc lập, không cần framework:

| Module | Nội dung kiểm tra |
|---|---|
| `ingestion/schemas.py` | Ép kiểu, ánh xạ cột Kaggle, quy đổi đơn vị giá |
| `ingestion/crawler_alonhadat.py` | Bóc microdata trên fixture HTML, hai dạng markup ngày đăng, trang lỗi trả rỗng |
| `spark/udf_address.py` | Mọi cách viết của cùng một quận phải ra cùng một mã; cầu nối 2025 |
| `ml/features.py` | 20 đặc trưng đúng thứ tự; mã hóa vòng giữ khoảng cách góc; giá trị tiếng Anh khớp bảng tra; thiếu tham số phải báo lỗi rõ ràng |
| `ml/train_forecast.py` | Backtest không rò rỉ tương lai; naive trả đúng h giá trị |

Một kiểm tra đáng nêu trong `features.py`: sau khi phát hiện lỗi ở mục 4.7.1, nhóm thêm hẳn một ca kiểm thử cho các giá trị **tiếng Anh** của dataset. Bài học được đóng băng thành kiểm thử để không lặp lại.

## 5.4. Hiệu năng

### 5.4.1. Độ trễ suy luận

| Lời gọi | Độ trễ |
|---|---|
| `/predict` lần đầu (nạp lazy) | 161,5 ms |
| `/predict` khi đã ấm | **11,5 – 12,0 ms** |
| Riêng phần mô hình XGBoost | 3,31 ms |

Phần chênh giữa 12 ms và 3,31 ms là chuẩn hóa địa chỉ, dựng đặc trưng và một truy vấn PostgreSQL lấy trung vị quận để so sánh.

### 5.4.2. Hot path

| Chỉ số | Giá trị |
|---|---|
| Cửa sổ | 1 giờ, trượt 5 phút |
| Watermark | 2 giờ |
| Kích thước micro-batch | 2.000 message |
| Số quận cập nhật mỗi batch | 91 – 147 |
| Số cảnh báo mỗi batch | 100 – 200 |
| Tổng cảnh báo trên 30.543 message | 2.176 |
| Số quận trong sorted set | 215 |

### 5.4.3. Dung lượng và truyền tải

| Hạng mục | Giá trị |
|---|---|
| GeoJSON quận/huyện (gốc) | 1.194 KB |
| GeoJSON quận/huyện (gzip) | **378 KB** |
| GeoJSON tỉnh/thành | 164 KB |
| Cache tầng Gold ở gateway | 60 giây |

## 5.5. Chất lượng dữ liệu qua từng lần chạy ETL

Bảng `etl_data_quality` ghi lại chỉ số của mỗi lần chạy. Đây là bằng chứng định lượng, không phải trang trí: nếu tỷ lệ khớp địa chỉ tụt đột ngột giữa hai lần chạy, đó là dấu hiệu bảng tham chiếu đã lỗi thời hoặc nguồn dữ liệu đã đổi định dạng.

| Giai đoạn | Vào | Ra | Loại | Khớp tỉnh | Khớp quận | Khớp phường |
|---|---|---|---|---|---|---|
| bronze_to_silver (chỉ Kaggle) | 30.229 | 30.227 | 2 | 99,99% | **99,91%** | 92,09% |
| bronze_to_silver (Kaggle + crawl) | 60.770 | 30.524 | 30.246¹ | 99,99% | **99,90%** | 91,63% |
| silver_to_gold | 30.524 | 400 | 824² | — | — | — |

¹ Phần lớn là bản trùng do nạp lại Bronze (30.227 bản), không phải dữ liệu hỏng.
² Đơn vị hành chính không đủ 30 tin để lên bản đồ, không phải dữ liệu bị xoá.

Lần chạy thứ hai có thêm 297 tin crawl từ alonhadat — toàn bộ ghi địa chỉ theo **hệ hành chính mới sau 01/07/2025**. Số tin khớp qua tầng cầu nối `bridge2025` tăng từ 6 lên **165**, xác nhận rằng bảng cầu nối ở mục 3.5 hoạt động đúng trên dữ liệu thật chứ không chỉ trên ca kiểm thử.

## 5.6. Nhận định thị trường rút ra được

Bốn nhận định rút trực tiếp từ dữ liệu, có thể kiểm chứng lại từ hệ thống:

**(1) Mặt bằng giá hai đô thị lớn gần như ngang nhau ở nhóm dẫn đầu.** Trong top 5 quận đắt nhất, TP.HCM và Hà Nội chia gần đều, và chênh lệch giữa hạng 1 và hạng 5 chỉ **1,3%** — nhóm dẫn đầu thực chất là một mặt bằng chung chứ không có quận nào vượt trội. Nhưng độ sâu thị trường thì rất khác: Quận Cầu Giấy có 573 tin trong khi Quận Hoàn Kiếm chỉ có 52 — cùng một mức giá nhưng mức độ chào bán chênh hơn 11 lần.

**(2) Thị trường phân hoá thành hai nhóm thực chất, không phải phổ liên tục.** Phân cụm chọn k=3 nhưng cụm thứ ba chỉ có một quận (và là do lỗi phân loại dữ liệu, xem 4.4.3). Hai nhóm thật có trung vị 147 so với 56 triệu/m² — chênh **2,6 lần** — với chồng lấn chỉ 19%. Kết luận này ổn định qua hai lần chạy trên hai tập dữ liệu khác nhau.

**(3) Vị trí giải thích gần một phần ba biến thiên giá.** `district_price_level` chiếm 0,310 độ quan trọng, gấp 1,7 lần đặc trưng đứng thứ hai.

**(4) Khoảng 3% tin rao có giá lệch xa mức hợp lý.** Trong đó một phần đáng kể không phải gian lận mà là **lỗi phân loại**: đất nền đăng vào danh mục nhà ở, nhà trọ cho thuê đăng như nhà ở.

## 5.7. Ảnh chụp minh chứng

Danh mục ảnh cần chèn vào báo cáo bản Word:

| Hình | Nội dung | Nguồn |
|---|---|---|
| 5.1 | Spark Master UI — 2 worker chạy song song | `localhost:8081` |
| 5.2 | HDFS NameNode — ba tầng Bronze/Silver/Gold | `localhost:9870` |
| 5.3 | Redpanda Console — topic và message | `localhost:8080` |
| 5.4 | MLflow — so sánh các lần chạy | `localhost:5000` |
| 5.5 | MinIO — artifact của MLflow | `localhost:9001` |
| 5.6 | Dashboard — bản đồ choropleth | `localhost:5173` |
| 5.7 | Dashboard — form định giá và kết quả | `localhost:5173` |
| 5.8 | Dashboard — bảng cảnh báo thời gian thực | `localhost:5173` |
| 5.9 | So sánh mô hình | `report/figures/model_comparison.png` |
| 5.10 | Tầm quan trọng đặc trưng | `report/figures/feature_importance.png` |
| 5.11 | Chọn số cụm (Elbow + Silhouette) | `report/figures/cluster_selection.png` |
| 5.12 | Phân cụm trên không gian PCA | `report/figures/cluster_scatter.png` |
| 5.13 | Phân bố điểm bất thường | `report/figures/anomaly_detection.png` |
| 5.14 | Đường cong học | `report/figures/learning_curve.png` |

<!-- PAGEBREAK -->

# CHƯƠNG 6. KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN

## 6.1. Kết quả đạt được

**Về hạ tầng.** Nền tảng dữ liệu lớn hoàn chỉnh 16 dịch vụ, khởi động bằng một lệnh, chứng minh đủ ba chữ V: Volume (data lake Medallion trên HDFS, 30.524 bản ghi qua ba tầng), Velocity (Spark Structured Streaming, cửa sổ trượt, cảnh báo trong một micro-batch), Variety (hai nguồn khác hẳn nhau về cấu trúc, ép về một lược đồ chuẩn).

**Về xử lý dữ liệu.** Module chuẩn hóa địa chỉ đạt **99,91%** ở cấp quận/huyện, vượt xa ngưỡng 90% đặt ra, trên một bài toán có tới **ba hệ quy chiếu hành chính** cùng tồn tại.

**Về học máy.** Năm bài toán, trong đó bốn đã có kết quả định lượng:

| # | Bài toán | Kết quả |
|---|---|---|
| 1 | Dự đoán giá | MAPE **19,43%**, R²(log) 0,838; 23,61% trên quận chưa từng thấy |
| 2 | Heatmap giá | 400 đơn vị hành chính, choropleth 3 cấp, ghép mã 99,7% |
| 3 | Dự báo xu hướng | Khung mô hình xong; chuỗi mới 4 điểm, chưa đủ để kết luận |
| 4 | Phân cụm khu vực | k=3, Silhouette **0,4758**, PCA giữ 84,0% |
| 5 | Tin bất thường | **916** tin; PR-AUC **0,9924** trên nhãn kiểm chứng |

**Về tính sử dụng được.** Tầng phục vụ ba lớp hoạt động đầy đủ: dự đoán giá trả về trong 12 ms qua toàn bộ chuỗi React → nginx → Spring Boot → FastAPI → mô hình MLflow.

**Về phương pháp.** Năm kết quả phản trực giác được phát hiện **bằng số đo** chứ không phải bằng trực giác, và tất cả đều dẫn tới thay đổi thiết kế hoặc thay đổi cách diễn giải kết quả. Nhóm coi đây là kết quả có giá trị nhất của đồ án: nó cho thấy quy trình có kiểm chứng thực sự, không phải chạy theo giá trị mặc định rồi báo cáo con số đầu tiên nhìn thấy.

## 6.2. Hạn chế

**(1) Phạm vi giá bị chặn.** Dataset Kaggle chỉ trải 1–11,5 tỷ VND. Mức độ nghiêm trọng đã được **đo cụ thể** ở mục 4.2.5: chỉ cần thêm 297 tin thật (dải 0,26–175 tỷ) là RMSE tăng 75% và R²(giá) tụt từ 0,626 xuống 0,445. Mô hình không dùng được cho phân khúc cao cấp.

**(2) Giá rao khác giá giao dịch.** Sai số cố hữu, không loại bỏ được trong phạm vi dữ liệu hiện có.

**(3) Bài toán 3 chưa có kết quả định lượng.** Chuỗi dài nhất mới 4 điểm, cần tối thiểu 12.

**(4) Đánh giá phát hiện bất thường là bán định lượng.** PR-AUC 0,9924 đo trên nhãn của dữ liệu giả lập; trên dữ liệu thật chỉ có kiểm tra thủ công.

**(5) Mất độ phân giải tại Thủ Đức.** Hệ quả của việc gộp Quận 2 và Quận 9 — thực tế hành chính, không phải lỗi xử lý.

**(6) Độ phủ dữ liệu không đều — nhưng tổng lượng thì đủ.** 213 quận/huyện có dữ liệu, chỉ 86 quận đủ 30 tin để lên bản đồ và tham gia phân cụm; cấp phường chỉ 287/943 đơn vị đạt ngưỡng. Đây là vấn đề **phân bố**, không phải vấn đề khối lượng: đường cong học (mục 4.2.6) chứng minh tổng số dòng đã tới ngưỡng bão hoà.

**(7) Chưa triển khai lên môi trường phân tán thật.** Toàn bộ chạy trên một máy; các vấn đề của cụm thật (network partition, skew dữ liệu giữa node) chưa gặp phải.

## 6.3. Hướng phát triển

**Ngắn hạn — bổ khuyết dữ liệu:**

- Chạy crawler theo lịch trong vài tuần để tích lũy đủ chuỗi `posted_at` cho bài toán 3. Hạ tầng đã sẵn sàng, chỉ thiếu thời gian tích lũy.
- Bổ sung nguồn dữ liệu có phân khúc cao cấp để nới trần giá của mô hình.
- Gán nhãn thủ công một mẫu 200–500 tin để đánh giá phát hiện bất thường trên dữ liệu thật.

**Trung hạn — làm giàu đặc trưng:**

- **Đặc trưng không gian:** khoảng cách tới trung tâm, tới trường học, bệnh viện, ga metro. Đây là hướng cải thiện MAPE mạnh nhất, và khẳng định này **có cơ sở định lượng** chứ không phải phỏng đoán: đường cong học ở mục 4.2.6 cho thấy gấp đôi số dòng chỉ đổi lấy 0,3–0,6 điểm MAPE, trong khi biên độ giá *trong cùng một quận* là 2,9 lần mà đặc trưng vị trí hiện tại không nắm bắt được chút nào.
- **Yếu tố vĩ mô:** lãi suất vay mua nhà, chỉ số giá vật liệu xây dựng.
- **Ảnh vệ tinh:** mật độ xây dựng, tỷ lệ cây xanh quanh vị trí.

**Dài hạn — mở rộng bài toán:**

- **Dự báo thanh khoản** — không chỉ "giá bao nhiêu" mà "bao lâu thì bán được".
- **Hệ khuyến nghị** — gợi ý bất động sản phù hợp ngân sách và nhu cầu.
- **Giải thích mô hình bằng SHAP** — cho người dùng thấy từng thuộc tính đóng góp bao nhiêu vào mức giá ước tính. Với một hệ thống định giá, khả năng giải thích quan trọng ngang độ chính xác.

**Về hạ tầng:**

- Chuyển Parquet sang **Delta Lake** để có ACID và time travel.
- Điều phối bằng **Airflow** thay vì chạy tay từng job.
- Triển khai lên cụm nhiều node thật.

<!-- PAGEBREAK -->

# TÀI LIỆU THAM KHẢO

[1] *Analysis and Prediction of Real Estate Prices in Hanoi Using Machine Learning*, Springer, 2025. https://link.springer.com/content/pdf/10.1007/978-981-96-5693-6_37.pdf

[2] N. Marz and J. Warren, *Big Data: Principles and Best Practices of Scalable Realtime Data Systems*, Manning Publications, 2015.

[3] T. Chen and C. Guestrin, "XGBoost: A Scalable Tree Boosting System", *Proceedings of the 22nd ACM SIGKDD*, 2016.

[4] G. Ke et al., "LightGBM: A Highly Efficient Gradient Boosting Decision Tree", *NeurIPS*, 2017.

[5] L. Prokhorenkova et al., "CatBoost: unbiased boosting with categorical features", *NeurIPS*, 2018.

[6] T. Akiba et al., "Optuna: A Next-generation Hyperparameter Optimization Framework", *Proceedings of the 25th ACM SIGKDD*, 2019.

[7] S. J. Taylor and B. Letham, "Forecasting at Scale", *The American Statistician*, vol. 72, no. 1, 2018.

[8] F. T. Liu, K. M. Ting and Z.-H. Zhou, "Isolation Forest", *IEEE ICDM*, 2008.

[9] M. M. Breunig et al., "LOF: Identifying Density-Based Local Outliers", *ACM SIGMOD*, 2000.

[10] P. J. Rousseeuw, "Silhouettes: a graphical aid to the interpretation and validation of cluster analysis", *Journal of Computational and Applied Mathematics*, vol. 20, 1987.

[11] Apache Spark Documentation — Structured Streaming Programming Guide. https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

[12] Quốc hội, *Nghị quyết 1111/NQ-UBTVQH14 về việc thành lập thành phố Thủ Đức thuộc Thành phố Hồ Chí Minh*, 2020.

[13] Tổng cục Thống kê, *Danh mục đơn vị hành chính Việt Nam*. https://provinces.open-api.vn

[14] Bộ dữ liệu ranh giới hành chính Việt Nam `dvhcvn`. https://github.com/daohoangson/dvhcvn

[15] Dataset *Vietnam Housing Dataset*, Kaggle. https://www.kaggle.com/datasets/nguyentiennhan/vietnam-housing-dataset-2024
