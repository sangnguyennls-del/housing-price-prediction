"""Feature engineering cho bài toán dự đoán giá bất động sản.

Dùng chung cho huấn luyện (ml/train_price.py) và suy luận (api-ml). Một nguồn
duy nhất — nếu tách đôi, sớm muộn hai bên sẽ lệch nhau và mô hình sẽ dự đoán
sai trên production mà không ai biết vì sao.

    python ml/features.py        # chạy bộ kiểm tra

═══════════════════════════════════════════════════════════════════════════
BIẾN MỤC TIÊU: log(price_per_m2), không phải price.

Hai lý do, đều cần nêu trong Chương 4 báo cáo:

1. Lấy log — Giá bất động sản có phân phối đuôi dài rất nặng: đa số tin vài
   tỷ, một số ít vài trăm tỷ. Huấn luyện trực tiếp trên giá khiến hàm mất mát
   bị chi phối bởi nhóm đắt nhất; log đưa phân phối về gần chuẩn và biến sai
   số tuyệt đối thành sai số tương đối — đúng thứ người mua nhà quan tâm.

2. Dùng ĐƠN GIÁ thay vì tổng giá — price ≈ area × đơn_giá, nên nếu dự đoán
   price thì diện tích chiếm gần hết sức giải thích và mô hình chỉ học lại một
   phép nhân. Dự đoán đơn giá buộc mô hình học phần khó: vị trí, pháp lý, hình
   dạng lô đất. Nhân ngược với diện tích để ra giá cuối.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ── Nhóm 5: Phong thủy ────────────────────────────────────────────────────
# Hướng nhà là biến VÒNG: Bắc (0°) và Tây Bắc (315°) chỉ cách nhau 45°, nhưng
# one-hot coi chúng xa nhau như mọi cặp khác. Mã hóa sin/cos giữ được cấu trúc
# vòng, và chỉ tốn 2 cột thay vì 8.
DIRECTION_DEGREES = {
    "bac": 0, "dong bac": 45, "dong": 90, "dong nam": 135,
    "nam": 180, "tay nam": 225, "tay": 270, "tay bac": 315,
}

# ── Nhóm 4: Pháp lý — thang thứ bậc ───────────────────────────────────────
# Pháp lý ở Việt Nam có thứ tự tự nhiên về mức độ an toàn, không phải nhãn rời
# rạc. Sổ đỏ/sổ hồng bán được ngay; hợp đồng mua bán còn rủi ro; chưa sổ rủi ro
# nhất và bị chiết khấu mạnh. Mã hóa thứ bậc truyền được thông tin đó cho mô
# hình bằng MỘT cột, thay vì bắt nó tự suy từ one-hot.
# Phải nhận CẢ tiếng Việt lẫn tiếng Anh. Dataset Kaggle tuy là dữ liệu Việt Nam
# nhưng ghi giá trị bằng tiếng Anh ("Have certificate", "Sale contract"), trong
# khi crawler lấy từ site Việt nên ghi tiếng Việt ("Sổ đỏ", "Hợp đồng mua bán").
# Thiếu một trong hai nhánh thì đặc trưng thành NaN toàn bộ mà KHÔNG báo lỗi —
# mô hình vẫn chạy, vẫn cho ra số, chỉ là mù một đặc trưng quan trọng.
LEGAL_RANK = {
    # tiếng Việt
    "so do": 4, "so hong": 4, "sodo": 4, "sohong": 4,
    "so hong so do": 4, "giay to hop le": 3,
    "hop dong mua ban": 2, "hdmb": 2,
    "dang cho so": 1, "cho so": 1,
    "giay to viet tay": 0, "chua so": 0,
    # tiếng Anh (dataset Kaggle)
    "have certificate": 4, "certificate": 4, "red book": 4, "pink book": 4,
    "valid documents": 3,
    "sale contract": 2, "contract": 2,
    "waiting for certificate": 1,
    "no certificate": 0, "handwritten documents": 0,
}

FURNITURE_RANK = {
    # tiếng Việt
    "cao cap": 3, "day du": 2, "co ban": 1,
    "ban giao tho": 0, "khong noi that": 0, "nha tho": 0,
    # tiếng Anh (dataset Kaggle)
    "luxury": 3, "high end": 3,
    "full": 2, "fully furnished": 2,
    "basic": 1, "partly": 1,
    "unfurnished": 0, "bare shell": 0, "none": 0,
}


def _norm(s: pd.Series) -> pd.Series:
    """Chuẩn hóa chuỗi để tra bảng: bỏ dấu, thường hóa, gộp khoảng trắng.

    Phải thay đ/Đ TRƯỚC khi chuẩn hóa NFD: "đ" là ký tự độc lập (U+0111), không
    phải "d" + dấu, nên NFD không tách nó và encode("ascii") sẽ XOÁ MẤT chữ cái
    — "Đông" thành "ong", không tra được bảng hướng.
    """
    return (s.fillna("")
             .astype(str)
             .str.replace("đ", "d", regex=False)
             .str.replace("Đ", "D", regex=False)
             .str.normalize("NFD")
             .str.encode("ascii", "ignore").str.decode("ascii")
             .str.lower()
             .str.replace(r"[^a-z0-9]+", " ", regex=True)
             .str.strip())


def _rank(s: pd.Series, table: dict[str, int], name: str = "") -> pd.Series:
    """Tra bảng thứ bậc; giá trị lạ → NaN để mô hình tự xử lý khuyết.

    CẢNH BÁO khi KHÔNG khớp được giá trị nào. Đây là lá chắn cho một lỗi đã
    thực sự xảy ra trong đồ án này: dataset Kaggle ghi pháp lý bằng tiếng Anh
    ("Have certificate") còn bảng tra chỉ có tiếng Việt, nên legal_rank thành
    NaN toàn bộ. Mô hình vẫn chạy, vẫn cho ra MAPE trông hợp lý — chỉ là mù hẳn
    một đặc trưng quan trọng, và không có gì báo cho ta biết.
    """
    normed = _norm(s)
    out = normed.map(table).astype("float64")
    n_present = (normed != "").sum()
    if n_present and out.notna().sum() == 0:
        vals = normed[normed != ""].value_counts().head(5).index.tolist()
        print(f"  ⚠️  '{name}': {n_present:,} giá trị không rỗng nhưng KHÔNG khớp "
              f"bảng tra nào → đặc trưng thành NaN toàn bộ.")
        print(f"      Giá trị hay gặp: {vals}")
        print(f"      Bổ sung chúng vào bảng tra trong ml/features.py.")
    return out


def direction_features(s: pd.Series, prefix: str) -> pd.DataFrame:
    deg = _norm(s).map(DIRECTION_DEGREES).astype("float64")
    rad = np.deg2rad(deg)
    return pd.DataFrame({
        f"{prefix}_sin": np.sin(rad),
        f"{prefix}_cos": np.cos(rad),
    }, index=s.index)


# ── Nhóm 1: Vị trí — target encoding có chống rò rỉ ────────────────────────
def fit_district_encoding(
    df: pd.DataFrame, target: pd.Series, n_splits: int = 5, seed: int = 42
) -> tuple[pd.Series, dict[str, float], float]:
    """Target encoding cho quận/huyện, tính out-of-fold.

    Trả về (giá trị mã hóa cho tập train, bảng tra cho suy luận, giá trị mặc
    định toàn cục).

    VÌ SAO PHẢI OUT-OF-FOLD — điểm này bắt buộc nêu trong báo cáo:
    Nếu mã hóa mỗi quận bằng trung vị tính trên TOÀN BỘ dữ liệu, thì giá của
    chính dòng đang huấn luyện đã góp phần tạo ra đặc trưng của nó. Mô hình sẽ
    học được "đặc trưng vị trí" chứa sẵn đáp án, cho R² đẹp giả tạo lúc
    validate rồi sụp khi gặp dữ liệu thật. Tính theo K-fold, mỗi dòng lấy giá
    trị từ các fold KHÁC, loại bỏ rò rỉ này.
    """
    from sklearn.model_selection import KFold

    codes = df["district_code"].fillna("__na__").astype(str)
    global_median = float(target.median())

    oof = pd.Series(np.nan, index=df.index, dtype="float64")
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=seed)
    for tr_idx, va_idx in kf.split(df):
        med = target.iloc[tr_idx].groupby(codes.iloc[tr_idx]).median()
        oof.iloc[va_idx] = codes.iloc[va_idx].map(med).to_numpy()
    oof = oof.fillna(global_median)

    # Bảng tra dùng lúc suy luận: tính trên toàn bộ train (không còn rò rỉ vì
    # dữ liệu suy luận nằm ngoài tập này)
    lookup = target.groupby(codes).median().to_dict()
    return oof, {str(k): float(v) for k, v in lookup.items()}, global_median


def build_features(
    df: pd.DataFrame,
    district_encoding: pd.Series | None = None,
    district_lookup: dict[str, float] | None = None,
    global_median: float | None = None,
) -> pd.DataFrame:
    """Bảng tin rao đã làm sạch → ma trận đặc trưng.

    Huấn luyện: truyền `district_encoding` (giá trị out-of-fold).
    Suy luận:   truyền `district_lookup` + `global_median`.
    """
    X = pd.DataFrame(index=df.index)

    # ── Nhóm 2: Quy mô ────────────────────────────────────────────────
    area = pd.to_numeric(df["area"], errors="coerce")
    X["log_area"] = np.log1p(area)
    X["floors"] = pd.to_numeric(df.get("floors"), errors="coerce")
    X["bedrooms"] = pd.to_numeric(df.get("bedrooms"), errors="coerce")
    X["bathrooms"] = pd.to_numeric(df.get("bathrooms"), errors="coerce")
    # Mật độ phòng: 30 phòng ngủ trên 80 m² là nhà trọ cho thuê, không phải nhà
    # ở — đặc trưng này tách được hai loại hình đó.
    X["area_per_bedroom"] = area / X["bedrooms"].replace(0, np.nan)
    X["rooms_total"] = X["bedrooms"].fillna(0) + X["bathrooms"].fillna(0)

    # ── Nhóm 3: Hình học lô đất (đặc thù Việt Nam) ────────────────────
    frontage = pd.to_numeric(df.get("frontage"), errors="coerce")
    access = pd.to_numeric(df.get("access_road"), errors="coerce")
    X["frontage"] = frontage
    X["access_road"] = access
    # Cùng diện tích, lô nở hậu (mặt tiền rộng) đáng giá hơn lô hình ống.
    # Chuẩn hóa theo căn bậc hai diện tích để tỷ số không phụ thuộc quy mô.
    X["frontage_ratio"] = frontage / np.sqrt(area)
    # Ngưỡng 4m: dưới mức này ô tô không vào được — ranh giới giá rất rõ trên
    # thị trường Việt Nam.
    X["is_car_access"] = (access >= 4.0).astype("float64").where(access.notna())
    X["is_wide_road"] = (access >= 8.0).astype("float64").where(access.notna())

    # ── Nhóm 4: Pháp lý & tiện nghi ───────────────────────────────────
    X["legal_rank"] = _rank(df.get("legal_status", pd.Series(index=df.index, dtype=object)),
                            LEGAL_RANK, "legal_status")
    X["furniture_rank"] = _rank(
        df.get("furniture_state", pd.Series(index=df.index, dtype=object)),
        FURNITURE_RANK, "furniture_state")

    # ── Nhóm 5: Phong thủy ────────────────────────────────────────────
    for col, prefix in (("house_direction", "house_dir"), ("balcony_direction", "balcony_dir")):
        src = df.get(col, pd.Series(index=df.index, dtype=object))
        X = X.join(direction_features(src, prefix))

    # ── Nhóm 1: Vị trí ────────────────────────────────────────────────
    if district_encoding is not None:
        X["district_price_level"] = district_encoding.to_numpy()
    else:
        if district_lookup is None or global_median is None:
            raise ValueError(
                "Suy luận cần district_lookup và global_median; "
                "huấn luyện cần district_encoding")
        codes = df["district_code"].fillna("__na__").astype(str)
        X["district_price_level"] = codes.map(district_lookup).fillna(global_median).to_numpy()

    # ── Nhóm 6: Tương tác chéo ────────────────────────────────────────
    # Diện tích ảnh hưởng tới đơn giá KHÁC NHAU tùy mặt bằng giá khu vực: ở
    # quận đắt, nhà lớn bị chiết khấu đơn giá mạnh hơn (ít người đủ tiền mua).
    # Cây quyết định phải tốn nhiều nhánh mới diễn tả được tương tác này, cho
    # sẵn thì rẻ hơn nhiều.
    X["district_x_area"] = X["district_price_level"] * X["log_area"]
    X["district_x_legal"] = X["district_price_level"] * X["legal_rank"]

    return X


FEATURE_NAMES: tuple[str, ...] = (
    "log_area", "floors", "bedrooms", "bathrooms", "area_per_bedroom", "rooms_total",
    "frontage", "access_road", "frontage_ratio", "is_car_access", "is_wide_road",
    "legal_rank", "furniture_rank",
    "house_dir_sin", "house_dir_cos", "balcony_dir_sin", "balcony_dir_cos",
    "district_price_level", "district_x_area", "district_x_legal",
)

FEATURE_GROUPS: dict[str, tuple[str, ...]] = {
    "Vị trí": ("district_price_level",),
    "Quy mô": ("log_area", "floors", "bedrooms", "bathrooms",
               "area_per_bedroom", "rooms_total"),
    "Hình học lô đất": ("frontage", "access_road", "frontage_ratio",
                        "is_car_access", "is_wide_road"),
    "Pháp lý & tiện nghi": ("legal_rank", "furniture_rank"),
    "Phong thủy": ("house_dir_sin", "house_dir_cos",
                   "balcony_dir_sin", "balcony_dir_cos"),
    "Tương tác chéo": ("district_x_area", "district_x_legal"),
}


def make_target(df: pd.DataFrame) -> pd.Series:
    """log(đơn giá triệu VND/m²) — xem giải thích ở đầu file."""
    ppm = pd.to_numeric(df["price_per_m2"], errors="coerce")
    return np.log(ppm)


def inverse_target(pred_log: np.ndarray, area: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """log(đơn giá) → (đơn giá triệu/m², tổng giá tỷ VND)."""
    price_m2 = np.exp(pred_log)
    return price_m2, price_m2 * area / 1000.0


# ── Kiểm tra ──────────────────────────────────────────────────────────────
def demo() -> None:
    df = pd.DataFrame({
        "district_code": ["760", "760", "766", "766", "001", "001", None, "760"],
        "area": [80.0, 50.0, 100.0, 60.0, 70.0, 45.0, 90.0, 120.0],
        "price": [12.0, 9.0, 10.0, 7.0, 20.0, 14.0, 8.0, 18.0],
        "price_per_m2": [150.0, 180.0, 100.0, 116.7, 285.7, 311.1, 88.9, 150.0],
        "frontage": [5.0, 4.0, None, 4.5, 6.0, 3.5, 5.0, 8.0],
        "access_road": [6.0, 3.0, 10.0, None, 12.0, 2.5, 4.0, 3.0],
        "floors": [3, 2, 4, 3, 5, 2, 3, 4],
        "bedrooms": [3, 2, 4, 3, 4, 2, 3, 30],
        "bathrooms": [2, 1, 3, 2, 3, 1, 2, 15],
        "legal_status": ["Sổ đỏ", "Sổ hồng", "Hợp đồng mua bán", "Đang chờ sổ",
                         "Sổ đỏ", None, "Giấy tờ hợp lệ", "Sổ hồng"],
        "furniture_state": ["Đầy đủ", "Cơ bản", None, "Bàn giao thô",
                            "Cao cấp", "Đầy đủ", "Cơ bản", None],
        "house_direction": ["Đông", "Tây", "Bắc", "Nam", "Đông Bắc", None, "Tây Nam", "Đông"],
        "balcony_direction": [None, "Đông", None, "Tây", "Nam", None, None, "Bắc"],
    })

    y = make_target(df)
    assert np.isclose(y.iloc[0], np.log(150.0)), y.iloc[0]

    oof, lookup, gmed = fit_district_encoding(df, y, n_splits=4)
    assert len(oof) == len(df) and oof.notna().all(), "mã hóa không được để trống"
    assert "760" in lookup and "__na__" in lookup, lookup.keys()

    X = build_features(df, district_encoding=oof)
    assert list(X.columns) == list(FEATURE_NAMES), \
        f"lệch cột:\n có: {list(X.columns)}\n cần: {list(FEATURE_NAMES)}"
    assert len(X) == len(df)

    # Mọi đặc trưng trong FEATURE_GROUPS phải tồn tại và không trùng nhóm
    flat = [f for g in FEATURE_GROUPS.values() for f in g]
    assert sorted(flat) == sorted(FEATURE_NAMES), "FEATURE_GROUPS lệch FEATURE_NAMES"
    assert len(flat) == len(set(flat)), "một đặc trưng bị xếp vào hai nhóm"

    # Hướng nhà: mã hóa vòng phải giữ được khoảng cách góc
    assert np.isclose(X["house_dir_sin"].iloc[2], 0.0, atol=1e-9)   # Bắc  = 0°
    assert np.isclose(X["house_dir_cos"].iloc[2], 1.0, atol=1e-9)
    assert np.isclose(X["house_dir_sin"].iloc[0], 1.0, atol=1e-9)   # Đông = 90°
    assert X["house_dir_sin"].isna().iloc[5], "thiếu hướng phải là NaN, không phải 0"

    # Giá trị TIẾNG ANH của dataset Kaggle phải khớp được — đây là lỗi đã thực
    # sự xảy ra: bảng tra chỉ có tiếng Việt nên đặc trưng thành NaN toàn bộ.
    en = pd.DataFrame({
        "legal_status": ["Have certificate", "Sale contract", None],
        "furniture_state": ["Full", "Basic", None],
    })
    lr = _rank(en["legal_status"], LEGAL_RANK, "legal_status")
    fr = _rank(en["furniture_state"], FURNITURE_RANK, "furniture_state")
    assert lr.iloc[0] == 4 and lr.iloc[1] == 2, list(lr)
    assert fr.iloc[0] == 2 and fr.iloc[1] == 1, list(fr)
    assert pd.isna(lr.iloc[2]) and pd.isna(fr.iloc[2]), "ô trống phải là NaN"

    # Pháp lý theo thứ bậc
    assert X["legal_rank"].iloc[0] == 4       # Sổ đỏ
    assert X["legal_rank"].iloc[2] == 2       # Hợp đồng mua bán
    assert X["legal_rank"].iloc[3] == 1       # Đang chờ sổ
    assert X["legal_rank"].iloc[0] > X["legal_rank"].iloc[3], "sổ đỏ phải xếp trên chờ sổ"

    # Đường vào
    assert X["is_car_access"].iloc[0] == 1.0   # 6m
    assert X["is_car_access"].iloc[1] == 0.0   # 3m
    assert X["is_wide_road"].iloc[4] == 1.0    # 12m
    assert X["is_car_access"].isna().iloc[3], "thiếu đường vào phải là NaN"

    # Nhà trọ: 30 phòng ngủ / 120 m² → mật độ phòng rất thấp
    assert X["area_per_bedroom"].iloc[7] < 5.0, X["area_per_bedroom"].iloc[7]

    # Suy luận cho ra cùng schema đặc trưng
    Xi = build_features(df.head(2), district_lookup=lookup, global_median=gmed)
    assert list(Xi.columns) == list(FEATURE_NAMES)
    # Quận chưa từng gặp phải rơi về giá trị mặc định, không được nổ
    unseen = df.head(1).copy(); unseen["district_code"] = ["99999"]
    Xu = build_features(unseen, district_lookup=lookup, global_median=gmed)
    assert Xu["district_price_level"].iloc[0] == gmed

    # Quay ngược về giá
    price_m2, price_ty = inverse_target(np.array([np.log(150.0)]), np.array([80.0]))
    assert np.isclose(price_m2[0], 150.0) and np.isclose(price_ty[0], 12.0)

    # Thiếu district_lookup lúc suy luận phải báo lỗi rõ ràng
    try:
        build_features(df.head(1))
        raise AssertionError("phải báo lỗi khi thiếu tham số mã hóa")
    except ValueError:
        pass

    print(f"features.py — tất cả kiểm tra đạt ({len(FEATURE_NAMES)} đặc trưng, "
          f"{len(FEATURE_GROUPS)} nhóm)")


if __name__ == "__main__":
    demo()
