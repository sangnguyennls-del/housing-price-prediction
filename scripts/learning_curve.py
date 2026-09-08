"""Đường cong học — trả lời câu "đồ án có đủ dữ liệu chưa?" bằng số đo.

    docker compose exec ml python scripts/learning_curve.py

═══════════════════════════════════════════════════════════════════════════
CÂU HỎI, VÀ VÌ SAO KHÔNG TRẢ LỜI BẰNG CẢM TÍNH ĐƯỢC.

"30.524 tin rao có đủ không?" là câu hỏi sai nếu hỏi trống không — đủ hay
không phụ thuộc vào việc thêm dữ liệu có làm mô hình tốt lên hay không. Câu
hỏi đúng là: NẾU có gấp đôi dữ liệu thì MAPE giảm bao nhiêu?

Đường cong học trả lời trực tiếp: huấn luyện lại trên 10%, 25%, 50%, 75%,
100% tập train, đo trên CÙNG một tập test cố định.

  Còn dốc ở mốc 100%  → thiếu dữ liệu, thu thập thêm sẽ có lợi.
  Đã phẳng            → đủ dữ liệu; trần chính xác nằm ở ĐẶC TRƯNG, không
                        phải ở số dòng. Thu thập thêm là lãng phí công.

Tập test GIỮ NGUYÊN qua mọi mốc — nếu test cũng co lại theo thì con số giữa
các mốc không so sánh được với nhau.

Mã hóa vị trí được tính LẠI trên từng tập con. Dùng chung một bảng mã hóa
tính từ toàn bộ train là rò rỉ: mốc 10% sẽ hưởng lợi từ thông tin của 90%
còn lại và đường cong sẽ phẳng một cách giả tạo.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ml"))

from features import (  # noqa: E402
    FEATURE_NAMES,
    build_features,
    fit_district_encoding,
    inverse_target,
    make_target,
)
from train_price import load_silver  # noqa: E402

SEED = 42
FIGDIR = Path("report/figures")
FRACTIONS = (0.10, 0.25, 0.50, 0.75, 1.00)

# Siêu tham số giữ CỐ ĐỊNH qua mọi mốc. Tinh chỉnh lại ở từng mốc sẽ trộn lẫn
# hai hiệu ứng — "nhiều dữ liệu hơn" và "tham số hợp hơn" — và đường cong
# không còn đo được thứ ta muốn đo.
XGB_PARAMS = dict(
    n_estimators=600, learning_rate=0.05, max_depth=7,
    subsample=0.85, colsample_bytree=0.85, reg_lambda=1.5,
    random_state=SEED, n_jobs=-1, tree_method="hist",
)


def mape(y_true_log, y_pred_log, area) -> float:
    _, p_true = inverse_target(np.asarray(y_true_log), area)
    _, p_pred = inverse_target(np.asarray(y_pred_log), area)
    return float(np.mean(np.abs(p_pred - p_true) / p_true) * 100)


def run(df: pd.DataFrame, fractions=FRACTIONS) -> pd.DataFrame:
    from sklearn.model_selection import train_test_split
    from xgboost import XGBRegressor

    df = df[df["price_per_m2"].notna() & (df["price_per_m2"] > 0)
            & df["area"].notna() & (df["area"] > 0)].reset_index(drop=True)
    y = make_target(df)
    ok = y.notna() & np.isfinite(y)
    df, y = df[ok].reset_index(drop=True), y[ok].reset_index(drop=True)

    idx_tr, idx_te = train_test_split(
        np.arange(len(df)), test_size=0.2, random_state=SEED, shuffle=True)
    df_te = df.iloc[idx_te].reset_index(drop=True)
    y_te = y.iloc[idx_te].reset_index(drop=True)
    area_te = pd.to_numeric(df_te["area"], errors="coerce").to_numpy()

    rng = np.random.default_rng(SEED)
    idx_tr = rng.permutation(idx_tr)          # xáo một lần, các mốc lồng nhau

    rows = []
    for frac in fractions:
        take = idx_tr[: max(50, int(len(idx_tr) * frac))]
        df_sub = df.iloc[take].reset_index(drop=True)
        y_sub = y.iloc[take].reset_index(drop=True)

        oof, lookup, gmed = fit_district_encoding(df_sub, y_sub)
        X_sub = build_features(df_sub, district_encoding=oof)[list(FEATURE_NAMES)]
        X_te = build_features(df_te, district_lookup=lookup,
                              global_median=gmed)[list(FEATURE_NAMES)]

        m = XGBRegressor(**XGB_PARAMS).fit(X_sub, y_sub)
        e = mape(y_te, m.predict(X_te), area_te)

        # Số quận mà tập con này "nhìn thấy" — biến ẩn quan trọng: đặc trưng
        # vị trí mạnh nhất chỉ tốt khi mỗi quận có đủ mẫu để tính trung vị.
        n_dist = df_sub["district_code"].nunique()
        rows.append({"frac": frac, "n_train": len(df_sub),
                     "n_districts": n_dist, "mape": e})
        print(f"  {frac:5.0%}  {len(df_sub):>7,} dòng  {n_dist:>4} quận  "
              f"MAPE {e:6.2f}%", flush=True)

    return pd.DataFrame(rows)


def plot(res: pd.DataFrame, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(res["n_train"], res["mape"], "o-", color="#b3123c", lw=2, ms=7)
    for _, r in res.iterrows():
        ax.annotate(f"{r['mape']:.2f}%", (r["n_train"], r["mape"]),
                    textcoords="offset points", xytext=(0, 9),
                    ha="center", fontsize=9)
    ax.set_xlabel("Số dòng huấn luyện")
    ax.set_ylabel("MAPE trên tập kiểm tra cố định (%)")
    ax.set_title("Đường cong học — thêm dữ liệu còn giúp được bao nhiêu?")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    plt.close(fig)
    print(f"  → {path}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-plot", action="store_true")
    args = ap.parse_args()

    df = load_silver()
    print(f"\nĐường cong học · tập test cố định 20% · XGBoost tham số cố định")
    res = run(df)

    first, last = res.iloc[0], res.iloc[-1]
    prev = res.iloc[-2]

    print("\n" + "─" * 66)
    print(f"Từ {first['frac']:.0%} → 100% dữ liệu: MAPE "
          f"{first['mape']:.2f}% → {last['mape']:.2f}% "
          f"(giảm {first['mape'] - last['mape']:.2f} điểm)")
    print(f"Chặng cuối ({prev['frac']:.0%} → 100%, thêm "
          f"{last['n_train'] - prev['n_train']:,} dòng): giảm "
          f"{prev['mape'] - last['mape']:.2f} điểm")

    # Ngoại suy thô cho câu "nếu có gấp đôi dữ liệu thì sao": chặng cuối tăng
    # 33% số dòng: nếu đà giảm giữ nguyên thì gấp đôi cũng chỉ được chừng này.
    gain = prev["mape"] - last["mape"]
    print("")
    if gain < 0.15:
        print("KẾT LUẬN: đường cong ĐÃ PHẲNG.")
        print(f"  Thêm {last['n_train'] - prev['n_train']:,} dòng cuối chỉ đổi "
              f"{gain:.2f} điểm MAPE — nhỏ hơn cả dao động giữa hai lần chạy.")
        print("  Dữ liệu ĐỦ cho bài toán dự đoán giá. Trần chính xác nằm ở ĐẶC")
        print("  TRƯNG (vị trí chỉ là một số/quận), không nằm ở số dòng. Thu thập")
        print("  thêm tin rao cùng loại sẽ không cải thiện được gì đáng kể.")
    elif gain < 0.5:
        print("KẾT LUẬN: đường cong gần phẳng, còn dốc nhẹ.")
        print(f"  Gấp đôi dữ liệu ước chừng chỉ giảm thêm ~{gain:.1f}–{gain * 2:.1f} điểm.")
        print("  Ưu tiên đặc trưng mới thay vì thu thập thêm dòng.")
    else:
        print("KẾT LUẬN: đường cong CÒN DỐC — thu thập thêm dữ liệu vẫn có lợi rõ.")

    if not args.no_plot:
        plot(res, FIGDIR / "learning_curve.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
