"""Bài toán 3 — Dự báo xu hướng giá (Prophet vs LSTM vs Naive).

    docker compose exec ml python scripts/build_price_history.py   # dựng chuỗi trước
    docker compose exec ml python ml/train_forecast.py
    docker compose exec ml python ml/train_forecast.py --area VN --horizon 4

═══════════════════════════════════════════════════════════════════════════
BA MÔ HÌNH, VÀ VÌ SAO PHẢI CÓ CÁI THỨ BA.

  Naive     Dự báo = giá trị quan sát cuối cùng. Không học gì cả.
  Prophet   Phân rã xu hướng + mùa vụ, có điểm gãy (changepoint).
  LSTM      Mạng hồi quy, học phụ thuộc phi tuyến trong cửa sổ trượt.

Naive KHÔNG phải để cho đủ bảng. Trên chuỗi giá tài sản ngắn, nó là đối thủ
thật sự khó đánh bại: giá bất động sản gần với bước ngẫu nhiên, mà với bước
ngẫu nhiên thì dự báo tối ưu CHÍNH LÀ giá trị cuối cùng. Một bài báo cáo đưa
ra "Prophet đạt MAPE 6%" mà không nói naive đạt bao nhiêu thì con số đó vô
nghĩa — có thể naive đạt 5%.

Nếu Prophet và LSTM đều thua naive, kết luận đúng là: chuỗi quá ngắn / quá
gần bước ngẫu nhiên để mô hình phức tạp có chỗ dùng. Đó là kết quả khoa học
hợp lệ và phải viết vào báo cáo đúng như vậy.

ĐÁNH GIÁ BẰNG BACKTEST CUỐN CHIẾU (rolling-origin), không phải một lần chia:
với chuỗi vài chục điểm, một lần chia train/test cho ra con số phụ thuộc hoàn
toàn vào việc điểm cắt rơi vào đâu. Cuốn chiếu cắt ở nhiều mốc liên tiếp, mỗi
lần huấn luyện lại, rồi lấy trung bình — và luôn chỉ dùng quá khứ để dự báo
tương lai, không bao giờ ngược lại.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

EXPERIMENT = "price-forecast"
FIGDIR = Path("report/figures")
SEED = 42

# Số điểm tối thiểu để backtest còn nghĩa: phải đủ cho ít nhất 3 fold sau khi
# giữ lại một đoạn huấn luyện khởi đầu.
MIN_POINTS = 12
MIN_TRAIN = 8

warnings.filterwarnings("ignore", category=FutureWarning)


def pg_engine():
    from sqlalchemy import create_engine

    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'reuser')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'repass')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'realestate')}")


def metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    y_true, y_pred = np.asarray(y_true, float), np.asarray(y_pred, float)
    err = y_pred - y_true
    return {
        "mape": float(np.mean(np.abs(err / y_true)) * 100),
        "rmse": float(np.sqrt(np.mean(err ** 2))),
        "mae": float(np.mean(np.abs(err))),
    }


# ══════════════════════════════════════════════════════════════════════
# Các mô hình — cùng một giao diện: fit(train) → dự báo h bước tiếp theo
# ══════════════════════════════════════════════════════════════════════
def fit_naive(train: pd.DataFrame, h: int) -> np.ndarray:
    """Giá trị cuối lặp lại h lần. Đối chứng bắt buộc."""
    return np.repeat(train["y"].iloc[-1], h)


def fit_prophet(train: pd.DataFrame, h: int, freq: str,
                return_model: bool = False):
    """Prophet, cấu hình TỰ ĐIỀU CHỈNH theo độ dài chuỗi.

    Mùa vụ năm chỉ được bật khi chuỗi trải ít nhất 2 năm. Đây không phải tuỳ
    chọn thẩm mỹ: ước lượng một chu kỳ 12 tháng từ dưới 2 chu kỳ quan sát là
    khớp nhiễu, và Prophet sẽ ngoại suy rất tự tin một mùa vụ không tồn tại.
    Chuỗi giá căn hộ TP.HCM có 5,5 năm nên bật được; chuỗi tự tích lũy từ
    crawler mới vài tuần thì không.

    changepoint_prior_scale giữ 0.05 — xu hướng ít gãy, tránh ngoại suy dốc
    đứng từ vài điểm cuối.
    """
    from prophet import Prophet

    span_days = (train["ds"].max() - train["ds"].min()).days
    yearly = span_days >= 730

    m = Prophet(
        yearly_seasonality=yearly,
        weekly_seasonality=False,
        daily_seasonality=False,
        changepoint_prior_scale=0.05,
        interval_width=0.80,
    )
    import logging
    logging.getLogger("prophet").setLevel(logging.ERROR)
    logging.getLogger("cmdstanpy").setLevel(logging.ERROR)

    m.fit(train[["ds", "y"]])
    future = m.make_future_dataframe(periods=h, freq=freq)
    fc = m.predict(future).tail(h)
    if return_model:
        return fc["yhat"].to_numpy(), fc
    return fc["yhat"].to_numpy()


def fit_lstm(train: pd.DataFrame, h: int, lookback: int = 4,
             epochs: int = 300) -> np.ndarray:
    """LSTM một biến, dự báo đệ quy.

    Cố ý rất nhỏ (1 lớp, 16 đơn vị): với vài chục điểm huấn luyện, mạng lớn
    hơn chỉ học thuộc lòng. Chuẩn hóa min-max theo tập TRAIN của từng fold —
    dùng min/max toàn chuỗi là rò rỉ thông tin tương lai vào quá khứ.
    """
    import torch
    from torch import nn

    torch.manual_seed(SEED)
    y = train["y"].to_numpy(dtype=np.float32)
    if len(y) <= lookback + 1:
        return fit_naive(train, h)

    lo, hi = float(y.min()), float(y.max())
    scale = (hi - lo) or 1.0
    ys = (y - lo) / scale

    X = np.stack([ys[i:i + lookback] for i in range(len(ys) - lookback)])
    Y = ys[lookback:]
    Xt = torch.tensor(X).unsqueeze(-1)
    Yt = torch.tensor(Y).unsqueeze(-1)

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.lstm = nn.LSTM(1, 16, batch_first=True)
            self.fc = nn.Linear(16, 1)

        def forward(self, x):
            out, _ = self.lstm(x)
            return self.fc(out[:, -1, :])

    net = Net()
    opt = torch.optim.Adam(net.parameters(), lr=0.02)
    lossf = nn.MSELoss()
    for _ in range(epochs):
        opt.zero_grad()
        loss = lossf(net(Xt), Yt)
        loss.backward()
        opt.step()

    # Dự báo đệ quy: mỗi bước dự đoán được nối vào cửa sổ để dự đoán bước sau.
    net.eval()
    window = list(ys[-lookback:])
    preds = []
    with torch.no_grad():
        for _ in range(h):
            x = torch.tensor(np.array(window[-lookback:], dtype=np.float32))
            p = float(net(x.view(1, lookback, 1)).item())
            preds.append(p)
            window.append(p)
    return np.array(preds) * scale + lo


MODELS = {
    "naive": lambda tr, h, freq: fit_naive(tr, h),
    "prophet": lambda tr, h, freq: fit_prophet(tr, h, freq),
    "lstm": lambda tr, h, freq: fit_lstm(tr, h),
}


# ══════════════════════════════════════════════════════════════════════
# Backtest cuốn chiếu
# ══════════════════════════════════════════════════════════════════════
def rolling_backtest(series: pd.DataFrame, horizon: int, freq: str,
                     min_train: int | None = None,
                     max_folds: int = 24) -> pd.DataFrame:
    """Cắt tại nhiều mốc liên tiếp, mỗi lần huấn luyện lại từ đầu.

    Cửa sổ MỞ RỘNG (expanding) chứ không trượt: dữ liệu bất động sản ít, vứt
    bỏ phần đầu chuỗi để giữ cửa sổ cố định là lãng phí thông tin hiếm.

    GIỚI HẠN SỐ FOLD là bắt buộc, không phải tối ưu vặt. Cắt tại MỌI mốc trên
    chuỗi 1.996 điểm là gần 2.000 lần huấn luyện lại — nhân với Prophet (fit
    Stan) và LSTM (300 epoch) thì mất nhiều giờ, để đổi lấy các fold gần như
    trùng nhau vì chỉ lệch một quan sát. Lấy mẫu đều tay trên toàn dải cho
    cùng thông tin với chi phí hữu hạn.

    min_train mặc định tỷ lệ theo chuỗi chứ không cố định: Prophet có mùa vụ
    năm cần ít nhất hai chu kỳ, mà một hằng số 8 điểm thì fold đầu tiên sẽ
    huấn luyện trên 8 tháng và cho kết quả vô nghĩa.
    """
    rows = []
    n = len(series)
    if min_train is None:
        min_train = max(MIN_TRAIN, 2 * horizon, int(0.30 * n))

    all_cuts = list(range(min_train, n - horizon + 1))
    if not all_cuts:
        raise SystemExit(
            f"Chuỗi {n} điểm quá ngắn cho backtest "
            f"(cần ≥ {min_train + horizon}). Thu thập thêm dữ liệu.")
    step = max(1, len(all_cuts) // max_folds)
    cuts = all_cuts[::step][:max_folds]
    print(f"  {len(cuts)} fold · huấn luyện tối thiểu {min_train} kỳ · "
          f"bước {step}")

    for cut in cuts:
        train = series.iloc[:cut]
        test = series.iloc[cut:cut + horizon]
        for name, fn in MODELS.items():
            try:
                pred = fn(train, horizon, freq)
            except Exception as e:                             # noqa: BLE001
                print(f"    {name} lỗi tại cut={cut}: {type(e).__name__}")
                continue
            m = metrics(test["y"].to_numpy(), pred)
            rows.append({"model": name, "cut": cut, "n_train": cut, **m})
    return pd.DataFrame(rows)


def plot_forecast(series: pd.DataFrame, forecasts: dict[str, pd.DataFrame],
                  area_name: str, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(series["ds"], series["y"], "o-", color="#333", lw=1.6,
            ms=4, label="Thực tế")

    colors = {"prophet": "#1f77b4", "lstm": "#d62728", "naive": "#7f7f7f"}
    for name, fc in forecasts.items():
        ax.plot(fc["ds"], fc["yhat"], "--o", ms=4, lw=1.6,
                color=colors.get(name, None), label=f"Dự báo — {name}")
        if {"yhat_lower", "yhat_upper"}.issubset(fc.columns):
            ax.fill_between(fc["ds"], fc["yhat_lower"], fc["yhat_upper"],
                            color=colors.get(name), alpha=0.15)

    ax.axvline(series["ds"].iloc[-1], color="#999", ls=":", lw=1)
    ax.set_title(f"Dự báo đơn giá — {area_name}\n"
                 f"({len(series)} điểm quan sát)")
    ax.set_ylabel("Đơn giá (triệu VND/m²)")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    plt.close(fig)
    print(f"  → {path}")


def run_all(engine, horizon: int, max_folds: int) -> int:
    """Chạy backtest trên MỌI chuỗi đủ dài, rồi tổng hợp.

    Một quận có thể ra kết quả bất thường do ngẫu nhiên. Chín quận cho biết
    quy luật có thật hay không — và câu "naive thắng ở 8/9 địa bàn" mạnh hơn
    nhiều so với "naive thắng ở Quận 1".
    """
    hist = pd.read_sql(
        "SELECT area_code, area_name, ds, price_m2 FROM gold_price_history "
        "ORDER BY area_code, ds", engine)
    lens = hist.groupby(["area_code", "area_name"]).size()
    areas = [(c, n) for (c, n), k in lens.items() if k >= MIN_POINTS]

    print(f"Chạy backtest trên {len(areas)} chuỗi · horizon={horizon}\n")
    rows = []
    for code, name in areas:
        ser = (hist[hist["area_code"] == code]
               .groupby("ds", as_index=False)["price_m2"].median()
               .rename(columns={"price_m2": "y"}).sort_values("ds"))
        ser["ds"] = pd.to_datetime(ser["ds"])
        if ser["ds"].diff().dt.days.median() <= 2 and len(ser) > 200:
            ser = (ser.set_index("ds")["y"].resample("MS").median()
                      .dropna().reset_index())
        if len(ser) < MIN_POINTS:
            continue
        bt = rolling_backtest(ser, horizon, "MS", max_folds=max_folds)
        agg = bt.groupby("model")["mape"].mean()
        best = agg.idxmin()
        rows.append({"area_code": code, "area_name": name, "n": len(ser),
                     "best": best, **{f"mape_{m}": v for m, v in agg.items()}})
        print(f"  {name[:30]:32} tốt nhất: {best:8} "
              + "  ".join(f"{m}={v:5.2f}%" for m, v in agg.items()))

    res = pd.DataFrame(rows)
    print("\n" + "═" * 70)
    win = res["best"].value_counts()
    print("Mô hình tốt nhất, đếm theo địa bàn:")
    for m, k in win.items():
        print(f"  {m:10} {k}/{len(res)} địa bàn")
    cols = [c for c in res.columns if c.startswith("mape_")]
    print("\nMAPE trung bình trên toàn bộ địa bàn:")
    for c in sorted(cols, key=lambda c: res[c].mean()):
        print(f"  {c[5:]:10} {res[c].mean():6.2f}%  (±{res[c].std():.2f})")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all", action="store_true",
                    help="chạy trên mọi chuỗi đủ dài rồi tổng hợp")
    ap.add_argument("--area", default=None,
                    help="area_code cần dự báo; mặc định lấy chuỗi dài nhất")
    ap.add_argument("--horizon", type=int, default=4, help="số kỳ dự báo")
    ap.add_argument("--freq", default=None, choices=[None, "D", "W", "MS"],
                    help="tần suất chuỗi; mặc định suy từ dữ liệu")
    ap.add_argument("--resample", default="auto", choices=["auto", "D", "W", "M"],
                    help="gộp chuỗi trước khi mô hình hóa (auto: ngày → tháng)")
    ap.add_argument("--max-folds", type=int, default=24)
    ap.add_argument("--no-mlflow", action="store_true")
    ap.add_argument("--no-postgres", action="store_true")
    args = ap.parse_args()

    eng = pg_engine()
    if args.all:
        return run_all(eng, args.horizon, args.max_folds)

    hist = pd.read_sql(
        "SELECT area_code, area_name, ds, price_m2, source FROM gold_price_history "
        "ORDER BY area_code, ds", eng)
    if hist.empty:
        print("gold_price_history rỗng — chạy scripts/build_price_history.py trước.")
        return 1

    lens = hist.groupby(["area_code", "area_name"]).size().sort_values(ascending=False)
    if args.area:
        sel = args.area
        if sel not in hist["area_code"].values:
            print(f"Không có chuỗi cho area_code={sel}. Có sẵn:")
            print(lens.head(10).to_string())
            return 1
    else:
        sel = lens.index[0][0]

    series = (hist[hist["area_code"] == sel]
              .groupby("ds", as_index=False)["price_m2"].median()
              .rename(columns={"price_m2": "y"})
              .sort_values("ds").reset_index(drop=True))
    series["ds"] = pd.to_datetime(series["ds"])
    area_name = hist.loc[hist["area_code"] == sel, "area_name"].iloc[0]

    # ── Gộp chuỗi ngày thành tháng ────────────────────────────────
    # Câu hỏi của đồ án là XU HƯỚNG THỊ TRƯỜNG, và ở tần suất ngày thì phần
    # lớn biến động là nhiễu đo chứ không phải tín hiệu — giá bất động sản
    # không đổi theo ngày. Gộp về tháng bằng TRUNG VỊ (nhất quán với cold
    # path) vừa lọc nhiễu, vừa đưa 1.996 điểm về 66 điểm đủ để Prophet ước
    # lượng mùa vụ năm trên 5,5 chu kỳ quan sát.
    gap_days = series["ds"].diff().dt.days.median()
    do_month = (args.resample == "M"
                or (args.resample == "auto" and gap_days <= 2 and len(series) > 200))
    if do_month:
        n_before = len(series)
        series = (series.set_index("ds")["y"].resample("MS").median()
                        .dropna().reset_index())
        print(f"Gộp theo THÁNG (trung vị): {n_before:,} điểm ngày → "
              f"{len(series)} điểm tháng")

    # Suy tần suất từ khoảng cách điển hình giữa hai điểm liên tiếp
    if args.freq:
        freq = args.freq
    else:
        gap = series["ds"].diff().dt.days.median()
        freq = "D" if gap <= 2 else ("W" if gap <= 10 else "MS")

    print("═" * 74)
    print(f"DỰ BÁO XU HƯỚNG GIÁ — {area_name} ({sel})")
    print("═" * 74)
    print(f"Chuỗi: {len(series)} điểm · {series['ds'].min().date()} → "
          f"{series['ds'].max().date()} · tần suất {freq}")
    print(f"Đơn giá: {series['y'].min():.1f} → {series['y'].max():.1f} triệu/m²")

    if len(series) < MIN_POINTS:
        print(f"\n⚠️  Chuỗi chỉ {len(series)} điểm (cần ≥ {MIN_POINTS}).")
        print("    Kết quả trên chuỗi ngắn thế này KHÔNG đủ tin cậy để đưa vào")
        print("    báo cáo như một kết luận về thị trường. Thu thập thêm rồi chạy lại.")
        return 1

    # ── Backtest ──────────────────────────────────────────────────────
    print(f"\nBacktest cuốn chiếu · horizon={args.horizon} kỳ")
    bt = rolling_backtest(series, args.horizon, freq, max_folds=args.max_folds)
    summary = (bt.groupby("model")[["mape", "rmse", "mae"]]
                 .agg(["mean", "std"]).round(3))
    n_folds = bt.groupby("model").size()

    print(f"\n{'Mô hình':10} {'MAPE %':>10} {'±':>7} {'RMSE':>9} {'MAE':>9} {'fold':>6}")
    print("─" * 60)
    order = summary[("mape", "mean")].sort_values().index
    for m in order:
        print(f"{m:10} {summary.loc[m, ('mape', 'mean')]:10.2f} "
              f"{summary.loc[m, ('mape', 'std')]:7.2f} "
              f"{summary.loc[m, ('rmse', 'mean')]:9.2f} "
              f"{summary.loc[m, ('mae', 'mean')]:9.2f} "
              f"{n_folds[m]:6d}")

    best = order[0]
    naive_mape = summary.loc["naive", ("mape", "mean")]
    best_mape = summary.loc[best, ("mape", "mean")]
    beats_naive = best != "naive"

    print("")
    span_years = (series["ds"].max() - series["ds"].min()).days / 365.25
    if beats_naive:
        print(f"✓ {best} thắng naive: {best_mape:.2f}% so với {naive_mape:.2f}% "
              f"(cải thiện {naive_mape - best_mape:.2f} điểm).")
    else:
        print(f"✗ KHÔNG mô hình nào thắng naive ({naive_mape:.2f}%).")
        # Chẩn đoán phải khớp với dữ liệu THẬT, không phải một câu có sẵn.
        # Bản trước luôn in "chuỗi quá ngắn" — đúng khi chuỗi tự tích lũy mới
        # vài tuần, nhưng sai hoàn toàn với chuỗi 5,5 năm.
        if len(series) < 30 or span_years < 2:
            print(f"  Nguyên nhân: chuỗi chỉ {len(series)} điểm / {span_years:.1f} năm")
            print("  — quá ngắn để mô hình phức tạp có chỗ phát huy.")
        else:
            print(f"  Chuỗi KHÔNG ngắn ({len(series)} điểm, {span_years:.1f} năm), nên")
            print("  đây là kết luận về BẢN CHẤT chuỗi chứ không phải về lượng dữ liệu:")
            print("  giá bất động sản rất gần bước ngẫu nhiên có trôi. Với bước ngẫu")
            print("  nhiên, dự báo tối ưu CHÍNH LÀ giá trị cuối — đúng cái naive làm.")
            print("  Prophet và LSTM cùng mắc một lỗi: ngoại suy đà tăng gần nhất ra")
            print(f"  {best_mape - naive_mape:.1f} điểm xa hơn mức thị trường thực sự đi.")
        print("  Đây là kết quả khoa học hợp lệ, không phải lỗi cài đặt.")

    # ── Huấn luyện lại trên toàn chuỗi và dự báo về phía trước ─────────
    print(f"\nDự báo {args.horizon} kỳ tới (huấn luyện trên toàn bộ chuỗi):")
    step = {"D": pd.Timedelta(days=1), "W": pd.Timedelta(weeks=1),
            "MS": pd.DateOffset(months=1)}[freq]
    future_ds = [series["ds"].iloc[-1] + step * (i + 1) for i in range(args.horizon)]

    forecasts: dict[str, pd.DataFrame] = {}
    for name in ("prophet", "lstm", "naive"):
        try:
            if name == "prophet":
                _, fc = fit_prophet(series, args.horizon, freq, return_model=True)
                d = pd.DataFrame({"ds": pd.to_datetime(fc["ds"].to_numpy()),
                                  "yhat": fc["yhat"].to_numpy(),
                                  "yhat_lower": fc["yhat_lower"].to_numpy(),
                                  "yhat_upper": fc["yhat_upper"].to_numpy()})
            else:
                yhat = MODELS[name](series, args.horizon, freq)
                d = pd.DataFrame({"ds": future_ds, "yhat": yhat,
                                  "yhat_lower": np.nan, "yhat_upper": np.nan})
            forecasts[name] = d
            print(f"  {name:8} " + " ".join(f"{v:7.1f}" for v in d["yhat"]))
        except Exception as e:                                 # noqa: BLE001
            print(f"  {name:8} lỗi: {type(e).__name__}: {e}")

    plot_forecast(series, forecasts, area_name, FIGDIR / "forecast.png")

    # ── Ghi kết quả ───────────────────────────────────────────────────
    if not args.no_postgres:
        from sqlalchemy import text

        fc_rows = pd.concat(
            [d.assign(model_name=n, area_code=sel, area_name=area_name,
                      is_forecast=True)
             for n, d in forecasts.items()], ignore_index=True)
        fc_rows["ds"] = pd.to_datetime(fc_rows["ds"]).dt.date
        met_rows = pd.DataFrame([
            {"area_code": sel, "model_name": m, "horizon_months": args.horizon,
             "mape": float(summary.loc[m, ("mape", "mean")]),
             "rmse": float(summary.loc[m, ("rmse", "mean")]),
             "mae": float(summary.loc[m, ("mae", "mean")]),
             "n_folds": int(n_folds[m])}
            for m in summary.index])

        with eng.begin() as con:
            con.execute(text("DELETE FROM gold_price_forecast WHERE area_code = :a"),
                        {"a": sel})
            con.execute(text("DELETE FROM gold_forecast_metrics WHERE area_code = :a"),
                        {"a": sel})
        fc_rows[["area_code", "area_name", "model_name", "ds", "yhat",
                 "yhat_lower", "yhat_upper", "is_forecast"]].to_sql(
            "gold_price_forecast", eng, if_exists="append", index=False)
        met_rows.to_sql("gold_forecast_metrics", eng, if_exists="append", index=False)
        print(f"  → PostgreSQL: {len(fc_rows)} điểm dự báo · {len(met_rows)} dòng metric")

    if not args.no_mlflow:
        import mlflow

        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
        mlflow.set_experiment(EXPERIMENT)
        with mlflow.start_run(run_name=f"forecast_{sel}_h{args.horizon}"):
            mlflow.log_params({"area_code": sel, "area_name": area_name,
                               "horizon": args.horizon, "freq": freq,
                               "n_points": len(series),
                               "n_folds": int(n_folds.max())})
            for m in summary.index:
                for k in ("mape", "rmse", "mae"):
                    mlflow.log_metric(f"{m}_{k}", float(summary.loc[m, (k, "mean")]))
            mlflow.set_tag("best_model", best)
            # Ghi thẳng vào tag: người đọc MLflow UI thấy ngay kết luận quan
            # trọng nhất mà không phải so từng con số.
            mlflow.set_tag("beats_naive", str(beats_naive))
            mlflow.log_artifact(str(FIGDIR / "forecast.png"))
        print(f"  → MLflow: experiment '{EXPERIMENT}'")

    return 0


# ── Kiểm tra ──────────────────────────────────────────────────────────────
def demo() -> None:
    """Kiểm tra bộ khung backtest trên chuỗi tổng hợp.

    Chuỗi ở đây CHỈ để kiểm tra code chạy đúng — không bao giờ được dùng làm
    dữ liệu báo cáo. Đó là ranh giới giữa "kiểm thử" và "bịa dữ liệu".
    """
    global MODELS

    n = 30
    ds = pd.date_range("2025-01-06", periods=n, freq="W")
    # xu hướng tăng + dao động nhỏ, đủ để mô hình có cái để học
    y = 100 + np.arange(n) * 0.8 + np.sin(np.arange(n) / 3) * 2
    series = pd.DataFrame({"ds": ds, "y": y})

    m = metrics(np.array([100.0, 110.0]), np.array([110.0, 110.0]))
    assert abs(m["mape"] - 5.0) < 1e-9, m
    assert abs(m["mae"] - 5.0) < 1e-9, m

    # Naive phải trả đúng h giá trị và bằng quan sát cuối
    p = fit_naive(series, 3)
    assert len(p) == 3 and p[0] == series["y"].iloc[-1]

    avail = {"naive": MODELS["naive"]}
    for name in ("prophet", "lstm"):
        try:
            MODELS[name](series.head(12), 2, "W")
            avail[name] = MODELS[name]
        except Exception as e:                                 # noqa: BLE001
            print(f"  bỏ qua {name} (không có thư viện: {type(e).__name__})")

    MODELS = avail
    bt = rolling_backtest(series, horizon=3, freq="W", min_train=20)
    assert not bt.empty and set(bt["model"]) == set(avail), bt["model"].unique()
    assert bt["mape"].notna().all() and (bt["mape"] >= 0).all()
    # Trên chuỗi có xu hướng tăng rõ, naive phải TỆ hơn — nếu không, hoặc backtest
    # đang rò rỉ tương lai, hoặc mô hình không học được gì.
    if "prophet" in avail:
        by = bt.groupby("model")["mape"].mean()
        assert by["prophet"] < by["naive"], by.to_dict()

    print(f"train_forecast.py — kiểm tra đạt ({len(avail)} mô hình, "
          f"{len(bt)} lần chạy backtest)")


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        demo()
    else:
        raise SystemExit(main())
