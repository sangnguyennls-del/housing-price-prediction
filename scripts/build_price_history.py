"""Dựng chuỗi thời gian giá cho bài toán 3 → bảng gold_price_history.

    docker compose exec ml python scripts/build_price_history.py
    docker compose exec ml python scripts/build_price_history.py --freq D

═══════════════════════════════════════════════════════════════════════════
QUY TẮC BẤT DI BẤT DỊCH CỦA FILE NÀY: KHÔNG BỊA MỐC THỜI GIAN.

Dataset Kaggle chính (~30k tin từ batdongsan) KHÔNG có cột ngày. Cách dễ nhất
để "có" chuỗi thời gian là rải ngẫu nhiên các tin vào 24 tháng rồi chạy
Prophet — biểu đồ sẽ rất đẹp và hoàn toàn vô nghĩa. Mọi xu hướng thấy được chỉ
là hàm sinh số ngẫu nhiên của chính ta.

Vì vậy script chỉ nhận ba nguồn có mốc thời gian THẬT:

  crawler_accumulated  silver_listings.posted_at — ngày đăng tin do site công
                       bố. Kho tin của alonhadat trải nhiều tuần nên một phiên
                       crawl đã cho một chuỗi thật. Đây là nguồn tự sinh ra
                       bởi kiến trúc streaming, và là bằng chứng cho giá trị
                       của nó.
  kaggle_hcm           BẤT KỲ file CSV nào trong data/raw thực sự có cột
                       ngày parse được. Nhận diện theo NỘI DUNG, không theo
                       tên file — xem _looks_like_series(). Tính tới hiện tại
                       chưa file nào trong data/raw đạt: cả hai dataset Kaggle
                       đã tải đều là ảnh chụp một thời điểm.
  bds_index            CSV nhập tay từ chỉ số giá công bố công khai. Chỉ nạp
                       khi file tồn tại; nội dung do người làm đồ án nhập và
                       trích dẫn nguồn, script không sinh ra số nào.

Không nguồn nào khả dụng thì script DỪNG và nói rõ thiếu gì — chứ không tự
chế dữ liệu để bài toán 3 "chạy được".
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

RAW = Path("data/raw")

# Số điểm tối thiểu để một chuỗi đáng đưa vào huấn luyện. Dưới ngưỡng này,
# backtest cuốn chiếu không còn đủ fold để có ý nghĩa thống kê.
MIN_POINTS = 12

# Số tin tối thiểu trong một kỳ để trung vị không quá nhiễu. Ngưỡng thấp hơn
# heatmap (30) vì chuỗi thời gian chia nhỏ dữ liệu theo cả không gian lẫn
# thời gian — đòi 30 tin/quận/tuần thì không địa bàn nào đạt.
MIN_LISTINGS_PER_PERIOD = 5


def pg_engine():
    from sqlalchemy import create_engine

    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'reuser')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'repass')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'realestate')}")


# ══════════════════════════════════════════════════════════════════════
# Nguồn 1 — chuỗi do chính hệ thống tích lũy
# ══════════════════════════════════════════════════════════════════════
def from_crawler(engine, freq: str) -> pd.DataFrame:
    """Trung vị đơn giá theo kỳ, từ ngày ĐĂNG tin.

    Tổng hợp bằng SQL chứ không kéo 30k dòng về pandas: PostgreSQL làm
    percentile ngay tại chỗ, và câu lệnh này đọc được nguyên văn trong báo cáo.

    Sinh song song hai mức: từng quận, và một chuỗi tổng "VN" toàn quốc. Chuỗi
    toàn quốc luôn dài và dày hơn nên thường là chuỗi duy nhất đủ dữ liệu để
    huấn luyện thật — cấp quận để trong bảng cho phần mở rộng.
    """
    trunc = {"D": "day", "W": "week", "M": "month"}[freq]
    sql = f"""
        WITH base AS (
            SELECT district_code, district,
                   date_trunc('{trunc}', posted_at)::date AS ds,
                   price_per_m2
            FROM silver_listings
            WHERE posted_at IS NOT NULL AND price_per_m2 IS NOT NULL
        )
        SELECT district_code AS area_code, district AS area_name, ds,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS price_m2,
               COUNT(*) AS n
        FROM base WHERE district_code IS NOT NULL
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT 'VN', 'Toàn quốc', ds,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2), COUNT(*)
        FROM base
        GROUP BY 3
        ORDER BY 1, 3
    """
    df = pd.read_sql(sql, engine)
    if df.empty:
        return df
    df = df[df["n"] >= MIN_LISTINGS_PER_PERIOD].copy()
    df["source"] = "crawler_accumulated"
    return df.drop(columns=["n"])


# ══════════════════════════════════════════════════════════════════════
# Nguồn 2 — dataset chuỗi căn hộ TP.HCM trên Kaggle
# ══════════════════════════════════════════════════════════════════════
_DATE_KEYS = ("date", "thang", "month", "time", "period", "ngay", "quarter")
_PRICE_KEYS = ("price", "gia", "value", "index")
_AREA_KEYS = ("district", "quan", "area_name", "region", "location")


def _pick(cols: dict[str, str], keys: tuple[str, ...]) -> str | None:
    for k in keys:
        for low, orig in cols.items():
            if k in low:
                return orig
    return None


def _looks_like_series(path: Path) -> tuple[str, str, str | None] | None:
    """Mở file ra xem: đây CÓ PHẢI chuỗi thời gian không?

    NHẬN DIỆN THEO NỘI DUNG, KHÔNG THEO TÊN — và đây là bài học phải trả giá
    bằng một vòng làm việc thừa. Đề cương của đồ án ghi dataset Kaggle
    "hoandan/apartment-prices-in-the-city-ho-chi-minh-city" là chuỗi thời gian
    có cột Date, suy ra từ CÁI TÊN chứ không mở ra xem. Tải về mới biết bên
    trong là "chung cu chotot.csv": 2.015 tin rao, 5 cột, không có ngày tháng
    nào. Bản dò theo tên file trước đây cũng hỏng nốt vì file không chứa chữ
    "apartment".

    Đọc 50 dòng đầu rồi thử parse thành ngày thì đúng/sai do DỮ LIỆU quyết
    định, không do cách đặt tên.
    """
    try:
        head = pd.read_csv(path, nrows=50)
    except Exception:                                       # noqa: BLE001
        return None
    cols = {c.lower().strip(): c for c in head.columns}
    c_date, c_price = _pick(cols, _DATE_KEYS), _pick(cols, _PRICE_KEYS)
    if not c_date or not c_price:
        return None
    # Tên cột nghe giống ngày vẫn chưa đủ — phải parse được thật. Cột
    # "Unnamed: 0" chứa chữ "nam" và từng bị bắt nhầm đúng theo kiểu này.
    parsed = pd.to_datetime(head[c_date], errors="coerce")
    if parsed.notna().mean() < 0.8 or parsed.nunique() < 3:
        return None
    return c_date, c_price, _pick(cols, _AREA_KEYS)


def from_kaggle_hcm() -> pd.DataFrame:
    """Nạp MỌI CSV trong data/raw thực sự có chuỗi thời gian.

    Không đòi tên file cụ thể: người dùng tải dataset nào về cũng được, hệ
    thống tự nhận ra file nào dùng được và nói rõ file nào không.
    """
    frames, skipped = [], []
    for path in sorted(RAW.glob("*.csv")):
        if path.stem.lower().startswith("sample"):
            continue
        hit = _looks_like_series(path)
        if hit is None:
            skipped.append(path.name)
            continue
        c_date, c_price, c_area = hit

        df = pd.read_csv(path)
        out = pd.DataFrame({
            "ds": pd.to_datetime(df[c_date], errors="coerce"),
            "price_m2": pd.to_numeric(
                df[c_price].astype(str).str.replace(r"[^\d.,-]", "", regex=True)
                          .str.replace(",", ".", regex=False), errors="coerce"),
            "area_name": (df[c_area].astype(str) if c_area else "TP. Hồ Chí Minh"),
        }).dropna()
        if out.empty:
            skipped.append(path.name)
            continue

        out["area_code"] = "HCM_" + out["area_name"].str.replace(r"\W+", "", regex=True)
        out["ds"] = out["ds"].dt.date
        out["source"] = "kaggle_hcm"
        print(f"  ✓ {path.name}: {len(out):,} điểm · "
              f"{out['area_code'].nunique()} địa bàn · {out['ds'].min()} → {out['ds'].max()}")
        frames.append(out[["area_code", "area_name", "ds", "price_m2", "source"]])

    for name in skipped:
        print(f"  ✗ {name}: không có cột thời gian dùng được")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════
# Nguồn 3 — chỉ số công bố công khai, nhập tay
# ══════════════════════════════════════════════════════════════════════
def from_manual_index() -> pd.DataFrame:
    """data/raw/bds_price_index.csv nếu người làm đồ án đã nhập.

    Cột bắt buộc: area_code, area_name, ds, price_m2. Script KHÔNG tạo file
    này — số liệu phải do người nhập từ nguồn công bố và trích dẫn trong báo
    cáo, nếu không thì đó là dữ liệu bịa.
    """
    path = RAW / "bds_price_index.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    need = {"area_code", "area_name", "ds", "price_m2"}
    if not need.issubset(df.columns):
        print(f"⚠️  {path.name} thiếu cột {need - set(df.columns)} → bỏ qua")
        return pd.DataFrame()
    df["ds"] = pd.to_datetime(df["ds"]).dt.date
    df["source"] = "bds_index"
    print(f"  {path.name}: {len(df):,} điểm")
    return df[["area_code", "area_name", "ds", "price_m2", "source"]]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--freq", default="W", choices=["D", "W", "M"],
                    help="độ phân giải chuỗi từ dữ liệu crawl (mặc định: tuần)")
    ap.add_argument("--no-postgres", action="store_true")
    args = ap.parse_args()

    print("═" * 74)
    print("DỰNG CHUỖI THỜI GIAN GIÁ — bài toán 3")
    print("═" * 74)

    frames = []

    print("\n[1] Chuỗi tích lũy từ crawler (silver_listings.posted_at)")
    try:
        eng = pg_engine()
        crawled = from_crawler(eng, args.freq)
        if crawled.empty:
            print("  chưa có tin nào có posted_at.")
            print("  → chạy: docker compose exec ml python ingestion/crawler_alonhadat.py "
                  "--pages 40")
            print("    rồi chạy lại cold path (kafka→bronze→silver).")
        else:
            n_area = crawled["area_code"].nunique()
            print(f"  {len(crawled):,} điểm · {n_area} địa bàn · "
                  f"{crawled['ds'].min()} → {crawled['ds'].max()}")
            frames.append(crawled)
    except Exception as e:                                     # noqa: BLE001
        print(f"  không đọc được PostgreSQL: {type(e).__name__}: {e}")
        eng = None

    print("\n[2] Dataset chuỗi căn hộ TP.HCM (Kaggle)")
    hcm = from_kaggle_hcm()
    if hcm.empty:
        print("  chưa có trong data/raw — xem scripts/download_data.py")
    else:
        frames.append(hcm)

    print("\n[3] Chỉ số giá công bố công khai (nhập tay)")
    manual = from_manual_index()
    if manual.empty:
        print("  chưa có data/raw/bds_price_index.csv")
    else:
        frames.append(manual)

    if not frames:
        print("\n" + "═" * 74)
        print("KHÔNG CÓ NGUỒN CHUỖI THỜI GIAN NÀO.")
        print("Bài toán 3 dừng ở đây — đúng như vậy, và phải ghi vào báo cáo.")
        print("Rải ngẫu nhiên 30k tin không ngày vào một trục thời gian sẽ cho")
        print("biểu đồ đẹp nhưng mọi xu hướng đều do bộ sinh số ngẫu nhiên tạo ra.")
        print("═" * 74)
        return 1

    hist = pd.concat(frames, ignore_index=True)
    hist = hist.dropna(subset=["price_m2"]).drop_duplicates(
        subset=["area_code", "ds", "source"])

    # Chuỗi nào đủ dài để huấn luyện thật — thông tin này quyết định
    # train_forecast.py chạy trên chuỗi nào.
    lens = (hist.groupby(["source", "area_code", "area_name"])
                .size().reset_index(name="n")
                .sort_values("n", ascending=False))
    ok = lens[lens["n"] >= MIN_POINTS]
    print("\n" + "─" * 74)
    print(f"Tổng: {len(hist):,} điểm · {hist['area_code'].nunique()} địa bàn")
    print(f"Chuỗi đủ ≥{MIN_POINTS} điểm để huấn luyện: {len(ok)}")
    for _, r in ok.head(10).iterrows():
        print(f"  {r['source']:22} {r['area_name'][:28]:30} {r['n']:>4} điểm")
    if ok.empty:
        print(f"  ⚠️  Không chuỗi nào đạt {MIN_POINTS} điểm. Chuỗi dài nhất: "
              f"{lens['n'].max()} điểm ({lens.iloc[0]['area_name']}).")
        print("      Cần crawl thêm để kho tin trải rộng hơn về thời gian.")

    if not args.no_postgres and eng is not None:
        with eng.begin() as con:
            from sqlalchemy import text
            con.execute(text("TRUNCATE TABLE gold_price_history"))
        hist.to_sql("gold_price_history", eng, if_exists="append", index=False)
        print(f"\n  → PostgreSQL.gold_price_history: {len(hist):,} dòng")

    return 0 if not ok.empty else 1


if __name__ == "__main__":
    raise SystemExit(main())
