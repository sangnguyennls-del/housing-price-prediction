"""Bài toán 5 — Phát hiện tin rao bất thường.

    docker compose exec ml python ml/train_anomaly.py
    docker compose exec ml python ml/train_anomaly.py --labels data/raw/sample_labels.csv

═══════════════════════════════════════════════════════════════════════════
BA TÍN HIỆU ĐỘC LẬP, mỗi tín hiệu bắt một kiểu bất thường khác nhau:

  1. Phần dư mô hình giá (residual)
     |giá thực tế − giá mô hình dự đoán|. Bắt tin có giá lệch hẳn mặt bằng
     KHI ĐÃ TÍNH ĐẾN mọi thuộc tính của nó. Đây là tín hiệu sát nghĩa "giá
     bất thường" nhất, và tái dùng ngay mô hình của bài toán 1 nên gần như
     miễn phí.

  2. Isolation Forest
     Bắt tin có TỔ HỢP thuộc tính hiếm gặp — ví dụ 30 phòng ngủ trên 50 m²,
     hoặc mặt tiền 20 m mà đường vào 2 m. Giá có thể hợp lý nhưng bản thân
     bất động sản mô tả không hợp lý.

  3. Local Outlier Factor
     Bắt tin bất thường CỤC BỘ: bình thường nếu nhìn toàn quốc, nhưng lạc lõng
     so với các tin lân cận trong không gian đặc trưng.

ĐIỂM CHÍNH lấy theo thứ hạng bách phân của phần dư, KHÔNG phải trung bình cả
ba. Thiết kế ban đầu có trộn cả ba nhưng phép đo trên nhãn thật bác bỏ (xem
khối chú thích ở WEIGHTS bên dưới): trộn nhiễu vào tín hiệu tốt làm PR-AUC rơi
từ 0.99 xuống 0.17. IF và LOF được giữ nguyên làm cờ bổ sung trong phần lý do.

Dùng thứ hạng bách phân thay vì giá trị thô vì ba tín hiệu có thang đo hoàn
toàn khác nhau và không so sánh trực tiếp được.

CÁCH CHẤM ĐIỂM: dữ liệu thật không có nhãn "tin này là ảo". Với dữ liệu giả
lập, scripts/make_sample_data.py ghi nhãn ra file RIÊNG (không đi vào pipeline)
nên chấm được precision/recall/PR-AUC thật. Truyền --labels để bật chế độ này.
Trên dữ liệu thật, báo cáo dùng precision@50 do người kiểm tra thủ công.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import mlflow  # noqa: E402
from sklearn.ensemble import IsolationForest  # noqa: E402
from sklearn.impute import SimpleImputer  # noqa: E402
from sklearn.neighbors import LocalOutlierFactor  # noqa: E402
from sklearn.preprocessing import StandardScaler  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from features import FEATURE_NAMES, build_features, make_target  # noqa: E402

EXPERIMENT = "anomaly-detection"
FIGDIR = Path("report/figures")
MODEL_NAME = "housing-price-model"
SEED = 42

# ═══════════════════════════════════════════════════════════════════════════
# CÁCH TỔ HỢP — quyết định này dựa trên số đo, không dựa trên trực giác.
#
# Thiết kế ban đầu lấy trung bình có trọng số cả ba tín hiệu (0.5/0.3/0.2).
# Chấm điểm trên nhãn thật cho kết quả bác bỏ thẳng thừng giả định đó:
#
#     Phần dư            PR-AUC 0.9924   P@50 100%
#     Isolation Forest   PR-AUC 0.0270   P@50   2%     (mức nền 0.0192)
#     LOF                PR-AUC 0.0205   P@50   0%
#     Trung bình có trọng số  PR-AUC 0.1740   P@50  26%
#
# Trộn hai tín hiệu gần như nhiễu vào một tín hiệu gần như hoàn hảo làm hỏng
# tín hiệu tốt: 0.99 → 0.17. Bài học: chỉ tổ hợp khi các thành phần thực sự
# bù trừ lỗi cho nhau, không tổ hợp vì "ensemble thì thường tốt hơn".
#
# Nguyên nhân sâu xa là ba tín hiệu trả lời BA CÂU HỎI KHÁC NHAU:
#     phần dư   → "giá có lệch mặt bằng không?"      (bất thường về GIÁ)
#     IF / LOF  → "bản thân mô tả có kỳ lạ không?"    (bất thường về THUỘC TÍNH)
# Ép chúng vào một con số là trộn hai câu hỏi. Vì vậy điểm chính lấy theo phần
# dư — đúng câu hỏi nghiệp vụ "tin này có giá bất thường không" — còn IF và LOF
# giữ nguyên làm CỜ BỔ SUNG, hiển thị trong phần lý do.
#
# LƯU Ý cho dữ liệu thật: kết luận trên rút ra từ dữ liệu giả lập, nơi mọi tin
# bất thường đều là nhiễu loạn GIÁ thuần tuý, còn thuộc tính vẫn bình thường —
# IF/LOF không thể thấy gì. Trên dữ liệu thật có cả tin mô tả phi lý (30 phòng
# ngủ trên 50 m², mặt tiền 20 m mà đường vào 2 m), khi đó IF/LOF sẽ có giá trị.
# Vì vậy hai tín hiệu này được GIỮ LẠI và vẫn tính, chỉ không trộn vào điểm
# chính. Chạy lại với --labels trên dữ liệu thật để đo lại.
# ═══════════════════════════════════════════════════════════════════════════
WEIGHTS = {"residual": 1.0, "iforest": 0.0, "lof": 0.0}

# Ngưỡng thứ hạng để coi một cờ bổ sung là "có kêu"
FLAG_PCT = 97.0

# Tỷ lệ tin giả định là bất thường, dùng làm tham số contamination.
CONTAMINATION = 0.03


def pg_engine():
    from sqlalchemy import create_engine

    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'reuser')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'repass')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'realestate')}")


def load_price_model():
    """Nạp mô hình giá đã đăng ký cùng bảng mã hóa vị trí đi kèm.

    Bảng mã hóa PHẢI lấy từ đúng run đã sinh ra mô hình. Nếu tính lại bảng mới
    trên dữ liệu hiện tại, đặc trưng district_price_level sẽ khác với lúc huấn
    luyện và phần dư sẽ phản ánh sự lệch đó chứ không phản ánh giá bất thường.
    """
    client = mlflow.MlflowClient()
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    if not versions:
        raise SystemExit(
            f"Chưa có mô hình '{MODEL_NAME}' trong MLflow Registry.\n"
            f"Chạy trước: docker compose exec ml python ml/train_price.py --stage all")

    latest = max(versions, key=lambda v: int(v.version))
    print(f"Mô hình giá: {MODEL_NAME} v{latest.version} (run {latest.run_id[:8]})")

    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{latest.version}")
    enc_path = client.download_artifacts(latest.run_id, "district_encoding.json")
    enc = json.loads(Path(enc_path).read_text(encoding="utf-8"))
    return model, enc["lookup"], float(enc["global_median"])


def pct_rank(x: np.ndarray) -> np.ndarray:
    """Thứ hạng bách phân 0-100. Cao = bất thường hơn."""
    return pd.Series(x).rank(pct=True).to_numpy() * 100.0


def describe(row: pd.Series) -> str:
    """Diễn giải cho người dùng vì sao tin bị gắn cờ."""
    parts = []
    ratio = row["residual_ratio"]
    if ratio is not None and not np.isnan(ratio) and ratio >= 0.4:
        direction = "cao hơn" if row["price_per_m2"] > row["predicted_price_m2"] else "thấp hơn"
        parts.append(f"Giá {direction} {ratio:.0%} so với mặt bằng khu vực")
    if row["r_iforest"] >= FLAG_PCT:
        parts.append("cờ bổ sung: tổ hợp thuộc tính hiếm gặp")
    if row["r_lof"] >= FLAG_PCT:
        parts.append("cờ bổ sung: lạc lõng so với tin lân cận")
    return "; ".join(parts) if parts else "Điểm bất thường tổng hợp cao"


def evaluate(df: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    """Chấm điểm từng tín hiệu và điểm tổng hợp trên nhãn thật."""
    from sklearn.metrics import average_precision_score, roc_auc_score

    m = df.merge(labels, on="listing_id", how="inner")
    if m.empty:
        print("  ⚠️  Không ghép được nhãn nào với dữ liệu — bỏ qua chấm điểm.")
        return pd.DataFrame()

    y = m["is_anomaly"].to_numpy()
    n_pos = int(y.sum())
    print(f"\nChấm điểm trên {len(m):,} tin ({n_pos} tin bất thường, "
          f"{n_pos / len(m):.1%})")
    if n_pos == 0:
        return pd.DataFrame()

    rows = []
    for name, col in [("Phần dư (residual)", "r_residual"),
                      ("Isolation Forest", "r_iforest"),
                      ("Local Outlier Factor", "r_lof"),
                      ("Điểm chính (đang dùng)", "anomaly_score")]:
        s = m[col].to_numpy()
        order = np.argsort(-s)
        row = {
            "tin_hieu": name,
            "PR_AUC": average_precision_score(y, s),
            "ROC_AUC": roc_auc_score(y, s),
        }
        for k in (50, 100, 200):
            k = min(k, len(m))
            row[f"P@{k}"] = float(y[order[:k]].sum() / k)
        row["recall@200"] = float(y[order[:min(200, len(m))]].sum() / n_pos)
        rows.append(row)

    table = pd.DataFrame(rows)
    # Mức nền: đoán ngẫu nhiên cho PR-AUC bằng đúng tỷ lệ tin bất thường
    base = n_pos / len(m)
    print(f"\n  Mức nền (đoán ngẫu nhiên): PR-AUC = {base:.4f}")
    print()
    print(table.to_string(index=False, float_format=lambda v: f"{v:.4f}"))

    best = table.loc[table["PR_AUC"].idxmax(), "tin_hieu"]
    combo = float(table.loc[table["tin_hieu"] == "Điểm chính (đang dùng)", "PR_AUC"].iloc[0])
    top = float(table["PR_AUC"].max())
    print(f"\n  → Tín hiệu tốt nhất: {best} (PR-AUC {top:.4f})")
    # So sánh có dung sai: khi điểm chính lấy nguyên một tín hiệu thành phần,
    # hai dòng bằng nhau và idxmax sẽ trả về dòng đứng trước — không có nghĩa
    # là điểm chính kém hơn.
    if top - combo > 1e-6:
        print(f"     Điểm chính hiện tại KÉM hơn ({combo:.4f}) — cần xem lại")
        print("     trọng số WEIGHTS trong file này.")
    else:
        print("     Điểm chính đang dùng đúng tín hiệu mạnh nhất.")
    return table


def plot_scores(df: pd.DataFrame, labels: pd.DataFrame | None, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # KHÔNG vẽ histogram của anomaly_score: điểm là thứ hạng bách phân nên phân
    # bố của nó luôn phẳng theo định nghĩa, biểu đồ sẽ là một hình chữ nhật
    # không mang thông tin nào.
    if labels is not None and not labels.empty:
        from sklearn.metrics import average_precision_score, precision_recall_curve

        m = df.merge(labels, on="listing_id", how="inner")
        y = m["is_anomaly"].to_numpy()
        for name, col, color in [("Phần dư", "r_residual", "#2E7D32"),
                                 ("Isolation Forest", "r_iforest", "#4C78A8"),
                                 ("LOF", "r_lof", "#F58518")]:
            prec, rec, _ = precision_recall_curve(y, m[col].to_numpy())
            ap = average_precision_score(y, m[col].to_numpy())
            ax1.plot(rec, prec, lw=2, color=color, label=f"{name} (PR-AUC {ap:.3f})")
        base = y.mean()
        ax1.axhline(base, ls="--", c="#999", lw=1.2,
                    label=f"Đoán ngẫu nhiên ({base:.3f})")
        ax1.set_xlabel("Recall")
        ax1.set_ylabel("Precision")
        ax1.set_title("Đường cong Precision–Recall theo tín hiệu", fontweight="bold")
        ax1.legend(fontsize=8.5, loc="center left")
        ax1.set_ylim(-0.02, 1.02)
    else:
        # Không có nhãn: vẽ phân bố mức lệch giá — thứ thực sự có hình dạng
        ratio = df["residual_ratio"].dropna()
        ax1.hist(ratio[ratio < ratio.quantile(0.995)], bins=60,
                 color="#4C78A8", alpha=0.85)
        cut = float(np.percentile(df["anomaly_score"], 97))
        thr = df.loc[df["anomaly_score"] >= cut, "residual_ratio"].min()
        ax1.axvline(thr, ls="--", c="#E45756", lw=1.5,
                    label=f"Ngưỡng gắn cờ ({thr:.0%} lệch)")
        ax1.set_xlabel("Mức lệch so với giá mô hình dự đoán")
        ax1.set_ylabel("Số tin rao")
        ax1.set_title("Phân bố mức lệch giá", fontweight="bold")
        ax1.legend(fontsize=9)
    ax1.grid(alpha=0.3)

    sub = df.dropna(subset=["predicted_price_m2"])
    if labels is not None and not labels.empty:
        m = sub.merge(labels, on="listing_id", how="left")
        m["is_anomaly"] = m["is_anomaly"].fillna(0)
        normal, anom = m[m["is_anomaly"] == 0], m[m["is_anomaly"] == 1]
        ax2.scatter(normal["predicted_price_m2"], normal["price_per_m2"],
                    s=6, alpha=0.25, color="#9BA7B4", label="Tin bình thường")
        ax2.scatter(anom["predicted_price_m2"], anom["price_per_m2"],
                    s=28, alpha=0.9, color="#E45756", label="Tin bất thường (nhãn thật)")
        ax2.legend()
    else:
        sc = ax2.scatter(sub["predicted_price_m2"], sub["price_per_m2"],
                         s=8, alpha=0.5, c=sub["anomaly_score"], cmap="YlOrRd")
        fig.colorbar(sc, ax=ax2, label="Điểm bất thường")

    lim = [0, float(np.nanpercentile(sub["price_per_m2"], 99))]
    ax2.plot(lim, lim, "--", c="#333", lw=1, alpha=0.7, label="_dự đoán = thực tế")
    ax2.set_xlim(lim); ax2.set_ylim(lim)
    ax2.set_xlabel("Đơn giá mô hình dự đoán (triệu/m²)")
    ax2.set_ylabel("Đơn giá rao thực tế (triệu/m²)")
    ax2.set_title("Thực tế so với dự đoán", fontweight="bold")
    ax2.grid(alpha=0.3)

    fig.suptitle("Phát hiện tin rao bất thường", fontsize=13, fontweight="bold")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\nBiểu đồ → {path}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--labels", default=None,
                    help="CSV listing_id,is_anomaly để chấm điểm (chỉ có với dữ liệu giả lập)")
    ap.add_argument("--contamination", type=float, default=CONTAMINATION)
    ap.add_argument("--top", type=int, default=20, help="số tin in ra màn hình")
    ap.add_argument("--no-mlflow", action="store_true")
    args = ap.parse_args()

    mlflow.set_tracking_uri(f"file://{Path('mlruns').resolve()}" if args.no_mlflow
                            else os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment(EXPERIMENT)

    df = pd.read_sql("SELECT * FROM silver_listings", pg_engine())
    df = df[df["price_per_m2"].notna() & (df["price_per_m2"] > 0)
            & df["area"].notna() & (df["area"] > 0)].reset_index(drop=True)
    print(f"Silver: {len(df):,} tin rao")

    model, lookup, gmed = load_price_model()
    X = build_features(df, district_lookup=lookup, global_median=gmed)

    with mlflow.start_run(run_name="anomaly_detection"):
        # ── Tín hiệu 1: phần dư mô hình giá ───────────────────────────
        y_true = make_target(df).to_numpy()
        y_pred = model.predict(X)
        # Phần dư tính trên thang LOG: lệch gấp đôi và lệch còn một nửa cho ra
        # cùng độ lớn, đúng bản chất "lệch bao nhiêu lần so với mặt bằng".
        resid_log = np.abs(y_true - y_pred)
        df["predicted_price_m2"] = np.exp(y_pred)
        df["residual_ratio"] = np.expm1(resid_log)     # đọc được: 0.4 = lệch 40%

        # ── Tín hiệu 2 & 3: bất thường trong không gian đặc trưng ─────
        Xn = StandardScaler().fit_transform(
            SimpleImputer(strategy="median").fit_transform(X.to_numpy(dtype="float64")))

        iforest = IsolationForest(contamination=args.contamination,
                                  random_state=SEED, n_estimators=300, n_jobs=-1)
        iforest.fit(Xn)
        # score_samples: càng THẤP càng bất thường → đảo dấu cho nhất quán
        df["iforest_score"] = -iforest.score_samples(Xn)

        lof = LocalOutlierFactor(n_neighbors=20, contamination=args.contamination)
        lof.fit_predict(Xn)
        df["lof_score"] = -lof.negative_outlier_factor_

        # ── Tổ hợp theo thứ hạng bách phân ────────────────────────────
        df["r_residual"] = pct_rank(resid_log)
        df["r_iforest"] = pct_rank(df["iforest_score"].to_numpy())
        df["r_lof"] = pct_rank(df["lof_score"].to_numpy())
        df["anomaly_score"] = (WEIGHTS["residual"] * df["r_residual"]
                               + WEIGHTS["iforest"] * df["r_iforest"]
                               + WEIGHTS["lof"] * df["r_lof"])
        df["reason"] = df.apply(describe, axis=1)

        mlflow.log_params({
            "n_listings": len(df), "contamination": args.contamination,
            "n_features": len(FEATURE_NAMES), "weights": json.dumps(WEIGHTS),
            "price_model": MODEL_NAME,
        })

        # ── Chấm điểm nếu có nhãn ─────────────────────────────────────
        labels = None
        if args.labels and Path(args.labels).exists():
            labels = pd.read_csv(args.labels)
            table = evaluate(df, labels)
            if not table.empty:
                for _, r in table.iterrows():
                    tag = r["tin_hieu"].split(" ")[0].lower()
                    mlflow.log_metrics({f"{tag}_pr_auc": r["PR_AUC"],
                                        f"{tag}_roc_auc": r["ROC_AUC"],
                                        f"{tag}_p_at_50": r["P@50"]})
        elif args.labels:
            print(f"  ⚠️  Không thấy file nhãn {args.labels} — bỏ qua chấm điểm.")

        # ── Top tin đáng ngờ ──────────────────────────────────────────
        top = df.nlargest(args.top, "anomaly_score")
        print(f"\nTop {args.top} tin đáng ngờ nhất:")
        print(f"  {'điểm':>6} {'giá rao':>9} {'dự đoán':>9}  {'quận':22} lý do")
        print("  " + "─" * 96)
        for _, r in top.iterrows():
            print(f"  {r['anomaly_score']:6.1f} {r['price_per_m2']:9.1f} "
                  f"{r['predicted_price_m2']:9.1f}  {str(r['district'])[:21]:22} "
                  f"{r['reason'][:46]}")

        # ── Ghi serving layer ─────────────────────────────────────────
        out = df[["listing_id", "district_code", "district", "price", "price_per_m2",
                  "predicted_price_m2", "residual_ratio", "iforest_score",
                  "lof_score", "anomaly_score", "reason"]].copy()
        # Chỉ lưu nhóm đáng ngờ: bảng này để người dùng rà soát, không phải bản
        # sao của toàn bộ dữ liệu.
        cut = float(np.percentile(out["anomaly_score"], 100 - args.contamination * 100))
        out = out[out["anomaly_score"] >= cut]

        from sqlalchemy import text

        with pg_engine().begin() as conn:
            conn.execute(text("TRUNCATE TABLE gold_anomaly"))
            out.to_sql("gold_anomaly", conn, if_exists="append", index=False)
        print(f"\n  → PostgreSQL.gold_anomaly: {len(out):,} tin "
              f"(ngưỡng điểm ≥ {cut:.1f})")
        mlflow.log_metric("n_flagged", len(out))

        plot_scores(df, labels, FIGDIR / "anomaly_detection.png")
        mlflow.log_artifact(str(FIGDIR / "anomaly_detection.png"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
