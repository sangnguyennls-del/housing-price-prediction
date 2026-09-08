"""Bài toán 4 — Phân cụm khu vực bất động sản (KMeans).

    docker compose exec ml python ml/train_cluster.py
    docker compose exec ml python ml/train_cluster.py --k 4     # ép số cụm

Gom các quận/huyện có đặc điểm thị trường tương đồng thành nhóm, gán nhãn theo
mặt bằng giá: Luxury / Mid / Affordable / Emerging.

═══════════════════════════════════════════════════════════════════════════
CHỌN SỐ CỤM — không tự đặt k, phải chứng minh.

Dùng đồng thời hai tiêu chí và ưu tiên Silhouette:

  Elbow (inertia)   Trực quan nhưng chủ quan — "khuỷu tay" nằm ở đâu là do
                    người đọc biểu đồ quyết định, hai người có thể chọn khác
                    nhau trên cùng một hình.
  Silhouette        Định lượng, có tối ưu rõ ràng. Đo mỗi điểm gần cụm của
                    mình hơn cụm gần nhất bao nhiêu; càng gần 1 càng tách bạch.

Báo cáo phải trình bày cả hai và nói rõ vì sao chọn k cuối cùng.

CẢNH BÁO VỀ SỐ LƯỢNG: phân cụm trên vài chục quận là bài toán nhỏ. Với dưới
~20 quận, silhouette rất nhiễu và kết quả đổi theo seed. Script sẽ cảnh báo
khi rơi vào tình huống đó thay vì im lặng đưa ra một con số trông có vẻ chắc.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import mlflow  # noqa: E402
from sklearn.cluster import KMeans  # noqa: E402
from sklearn.decomposition import PCA  # noqa: E402
from sklearn.impute import SimpleImputer  # noqa: E402
from sklearn.metrics import calinski_harabasz_score, silhouette_score  # noqa: E402
from sklearn.preprocessing import StandardScaler  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

EXPERIMENT = "area-clustering"
FIGDIR = Path("report/figures")
SEED = 42

# Đặc trưng mô tả "tính cách" của một địa bàn, không phải quy mô dữ liệu.
# n_listings CỐ Ý bị loại: nó phản ánh mức độ phủ của nguồn crawl chứ không
# phải đặc điểm thị trường — đưa vào sẽ gom nhóm theo "quận nào nhiều tin rao".
CLUSTER_FEATURES = (
    "median_price_m2",   # mặt bằng giá — trục phân hoá mạnh nhất
    "mean_area",         # quy mô bất động sản điển hình
    "mean_floors",       # nhà cao tầng (nội thành) hay thấp tầng (ven đô)
    "pct_red_book",      # mức độ hoàn chỉnh pháp lý
    "pct_mat_tien",      # tỷ lệ tiếp giáp đường ô tô vào được
)

# Nhãn theo thứ tự giá GIẢM DẦN
LABELS_BY_K = {
    2: ["Cao cấp", "Phổ thông"],
    3: ["Cao cấp", "Trung cấp", "Bình dân"],
    4: ["Cao cấp", "Trung cấp", "Bình dân", "Vùng ven"],
    5: ["Siêu cao cấp", "Cao cấp", "Trung cấp", "Bình dân", "Vùng ven"],
}


def pg_engine():
    from sqlalchemy import create_engine

    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'reuser')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'repass')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'realestate')}")


def load_districts() -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM gold_district_features", pg_engine())
    print(f"Đặc trưng quận: {len(df)} quận/huyện")
    return df


MIN_CV = 0.15


def screen_features(df: pd.DataFrame, min_cv: float = MIN_CV) -> list[str]:
    """Loại các đặc trưng gần như không biến thiên giữa các quận.

    VÌ SAO CẦN BƯỚC NÀY — bài học rút ra từ chính đồ án này, nên viết vào báo cáo:

    StandardScaler đưa mọi đặc trưng về cùng thang, tức là gán cho chúng TRỌNG
    SỐ BẰNG NHAU trong khoảng cách Euclid. Nếu 4 trên 5 đặc trưng gần như hằng
    số giữa các quận, phần biến thiên còn lại của chúng chỉ là nhiễu — nhưng
    sau khi chuẩn hóa, nhiễu đó được phóng đại lên bằng đúng tín hiệu giá.

    Hệ quả quan sát được: quận Ba Đình (408 triệu/m², đắt thứ ba) bị xếp cùng
    nhóm với Biên Hòa (52 triệu/m²), và nhãn "Cao cấp / Bình dân" trở nên vô
    nghĩa vì các cụm chồng lấn hoàn toàn về giá.

    Hệ số biến thiên CV = độ lệch chuẩn / trung bình đo mức phân hoá tương đối
    giữa các quận. CV < 0.15 nghĩa là đặc trưng đó gần như giống nhau ở mọi
    địa bàn, không giúp phân nhóm được.
    """
    kept, dropped = [], []
    print("\nSàng lọc đặc trưng (CV = độ lệch chuẩn / trung bình giữa các quận):")
    for f in CLUSTER_FEATURES:
        s = pd.to_numeric(df[f], errors="coerce")
        mean = s.mean()
        cv = float(s.std() / mean) if mean and abs(mean) > 1e-12 else 0.0
        if cv >= min_cv:
            kept.append(f)
            print(f"  ✓ giữ   {f:18} CV = {cv:5.3f}")
        else:
            dropped.append(f)
            print(f"  ✗ loại  {f:18} CV = {cv:5.3f}  (gần như hằng số)")

    if dropped:
        print(f"\n  Đã loại {len(dropped)} đặc trưng không phân hoá giữa các quận.")
        print("  Giữ lại chúng sẽ để nhiễu lấn át tín hiệu sau khi chuẩn hóa.")
    if not kept:
        raise SystemExit("Không đặc trưng nào đủ phân hoá — không thể phân cụm.")
    return kept


def check_label_validity(df: pd.DataFrame) -> float:
    """Đo mức chồng lấn giá giữa các cụm; cảnh báo khi nhãn gây hiểu sai.

    Nhãn "Cao cấp / Bình dân" ngụ ý phân tách theo giá. Nếu khoảng giá của các
    cụm chồng lên nhau nhiều thì nhãn đó nói dối người đọc, và phải báo ra chứ
    không được im lặng ghi vào dashboard.

    Trả về tỷ lệ chồng lấn: 0 = tách bạch hoàn toàn, 1 = trùng khít.
    """
    ranges = (df.groupby("cluster_label")["median_price_m2"]
                .agg(["min", "max", "median", "count"])
                .sort_values("median", ascending=False))

    print("\nKhoảng giá từng cụm (triệu VND/m²):")
    for label, r in ranges.iterrows():
        print(f"  {label:14} {r['min']:6.0f} – {r['max']:6.0f}   "
              f"trung vị {r['median']:6.0f}   ({int(r['count'])} quận)")

    # Chồng lấn = phần giao của khoảng rộng nhất và hẹp nhất so với khoảng tổng
    lo, hi = df["median_price_m2"].min(), df["median_price_m2"].max()
    span = hi - lo
    if span <= 0:
        return 1.0
    overlaps = []
    labels = list(ranges.index)
    for i in range(len(labels) - 1):
        a, b = ranges.loc[labels[i]], ranges.loc[labels[i + 1]]
        inter = min(a["max"], b["max"]) - max(a["min"], b["min"])
        overlaps.append(max(0.0, inter) / span)
    ratio = float(np.mean(overlaps)) if overlaps else 0.0

    if ratio > 0.5:
        print(f"\n  ⚠️  Chồng lấn giá giữa các cụm liền kề: {ratio:.0%}")
        print("      Nhãn theo phân khúc giá KHÔNG phản ánh đúng kết quả phân cụm.")
        print("      Cụm được hình thành chủ yếu bởi các đặc trưng khác, không phải giá.")
    else:
        print(f"\n  Chồng lấn giá giữa các cụm liền kề: {ratio:.0%} — nhãn hợp lệ.")
    return ratio


def choose_k(X: np.ndarray, k_min: int, k_max: int) -> tuple[int, pd.DataFrame]:
    """Quét k, trả về k tốt nhất theo Silhouette và bảng chỉ số đầy đủ."""
    rows = []
    for k in range(k_min, k_max + 1):
        km = KMeans(n_clusters=k, random_state=SEED, n_init=10)
        labels = km.fit_predict(X)
        rows.append({
            "k": k,
            "inertia": float(km.inertia_),
            "silhouette": float(silhouette_score(X, labels)),
            # Calinski-Harabasz bổ trợ: tỷ số phân tán giữa cụm / trong cụm.
            # Đưa vào để báo cáo không chỉ dựa trên một chỉ số duy nhất.
            "calinski": float(calinski_harabasz_score(X, labels)),
        })
    table = pd.DataFrame(rows)
    best = int(table.loc[table["silhouette"].idxmax(), "k"])

    print("\n  k   inertia   silhouette   calinski")
    print("  " + "─" * 40)
    for _, r in table.iterrows():
        mark = " ←" if int(r["k"]) == best else ""
        print(f"  {int(r['k'])}  {r['inertia']:8.2f}   {r['silhouette']:9.4f}   "
              f"{r['calinski']:8.2f}{mark}")
    return best, table


def label_clusters(df: pd.DataFrame, k: int) -> pd.Series:
    """Gán nhãn theo thứ hạng giá trung vị của cụm.

    KMeans đánh số cụm tuỳ ý và số đó đổi theo seed. Xếp hạng theo giá rồi mới
    gán nhãn giúp "Cao cấp" luôn là cụm đắt nhất, bất kể lần chạy nào — điều
    kiện cần để kết quả tái lập được và để dashboard tô màu ổn định.
    """
    order = (df.groupby("cluster_id")["median_price_m2"].median()
               .sort_values(ascending=False).index.tolist())
    names = LABELS_BY_K.get(k, [f"Nhóm {i + 1}" for i in range(k)])
    return df["cluster_id"].map({cid: names[i] for i, cid in enumerate(order)})


def plot_selection(table: pd.DataFrame, best_k: int, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))

    ax1.plot(table["k"], table["inertia"], "o-", color="#4C78A8")
    ax1.axvline(best_k, ls="--", c="#E45756", alpha=0.7)
    ax1.set_xlabel("Số cụm (k)")
    ax1.set_ylabel("Inertia (tổng bình phương trong cụm)")
    ax1.set_title("Phương pháp Elbow", fontweight="bold")
    ax1.grid(alpha=0.3)

    ax2.plot(table["k"], table["silhouette"], "o-", color="#54A24B")
    ax2.axvline(best_k, ls="--", c="#E45756", alpha=0.7)
    ax2.annotate(f"k = {best_k}", (best_k, table["silhouette"].max()),
                 xytext=(8, -12), textcoords="offset points",
                 fontsize=10, fontweight="bold", color="#E45756")
    ax2.set_xlabel("Số cụm (k)")
    ax2.set_ylabel("Silhouette (cao hơn là tốt hơn)")
    ax2.set_title("Hệ số Silhouette", fontweight="bold")
    ax2.grid(alpha=0.3)

    fig.suptitle("Lựa chọn số cụm cho phân nhóm khu vực", fontsize=13, fontweight="bold")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\nBiểu đồ chọn k → {path}")


def plot_scatter(df: pd.DataFrame, n_comp: int, path: Path) -> None:
    """Vẽ kết quả phân cụm.

    Chọn kiểu biểu đồ theo số chiều THẬT của dữ liệu. Khi chỉ còn một đặc trưng,
    vẽ mặt phẳng PCA là bịa ra một trục không tồn tại: nhãn chồng lên nhau và
    người đọc tưởng trục tung có ý nghĩa. Biểu đồ chấm ngang sắp theo giá vừa
    trung thực vừa dễ đọc hơn nhiều.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    palette = dict(zip(sorted(df["cluster_label"].unique()), plt.cm.Set2.colors))

    if n_comp == 1:
        d = df.sort_values("median_price_m2").reset_index(drop=True)
        fig, ax = plt.subplots(figsize=(10, max(6, 0.34 * len(d))))

        for i, r in d.iterrows():
            color = palette[r["cluster_label"]]
            ax.hlines(i, 0, r["median_price_m2"], color=color, alpha=0.45, lw=2.2)
            ax.plot(r["median_price_m2"], i, "o", ms=11, color=color,
                    markeredgecolor="white", markeredgewidth=1.4, zorder=3)
            ax.text(r["median_price_m2"] + d["median_price_m2"].max() * 0.015, i,
                    f"{r['median_price_m2']:.0f}", va="center", fontsize=8, alpha=0.75)

        ax.set_yticks(range(len(d)))
        ax.set_yticklabels(d["district"], fontsize=9)
        ax.set_ylim(-0.8, len(d) - 0.2)
        ax.set_xlabel("Giá trung vị (triệu VND/m²)")
        ax.set_title("Phân cụm khu vực theo mặt bằng giá\n"
                     "(chỉ một đặc trưng vượt sàng lọc — phân cụm 1 chiều)",
                     fontsize=13, fontweight="bold")
        handles = [plt.Line2D([], [], marker="o", ls="", ms=10, color=palette[lb],
                              label=f"{lb} ({(df['cluster_label'] == lb).sum()} quận)")
                   for lb in palette]
        ax.legend(handles=handles, title="Phân khúc", loc="lower right")
        ax.grid(axis="x", alpha=0.3)
    else:
        fig, ax = plt.subplots(figsize=(11, 8))
        for label, grp in df.groupby("cluster_label"):
            ax.scatter(grp["pca_x"], grp["pca_y"], s=140, alpha=0.85,
                       label=f"{label} ({len(grp)} quận)", color=palette[label],
                       edgecolors="white", linewidths=1.5, zorder=3)
        for _, r in df.iterrows():
            ax.annotate(r["district"], (r["pca_x"], r["pca_y"]), fontsize=7.5,
                        xytext=(0, 9), textcoords="offset points", ha="center", alpha=0.8)
        ax.set_xlabel("Thành phần chính 1")
        ax.set_ylabel("Thành phần chính 2")
        ax.set_title("Phân cụm khu vực bất động sản (chiếu PCA 2 chiều)",
                     fontsize=13, fontweight="bold")
        ax.legend(title="Phân khúc", loc="best")
        ax.grid(alpha=0.25)

    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Biểu đồ phân cụm → {path}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--k", type=int, default=0, help="ép số cụm; 0 = tự chọn")
    ap.add_argument("--k-min", type=int, default=2)
    ap.add_argument("--k-max", type=int, default=6)
    ap.add_argument("--min-cv", type=float, default=MIN_CV,
                    help="ngưỡng hệ số biến thiên để giữ một đặc trưng")
    ap.add_argument("--no-mlflow", action="store_true")
    args = ap.parse_args()

    if args.no_mlflow:
        mlflow.set_tracking_uri(f"file://{Path('mlruns').resolve()}")
    else:
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment(EXPERIMENT)

    df = load_districts()
    if len(df) < 6:
        print(f"Chỉ có {len(df)} quận — quá ít để phân cụm có ý nghĩa. Dừng.",
              file=sys.stderr)
        return 1
    if len(df) < 20:
        print(f"  ⚠️  Chỉ {len(df)} quận. Silhouette sẽ nhiễu và kết quả nhạy với "
              f"seed.\n      Cần nêu hạn chế này trong báo cáo.")

    # Chỉ giữ đặc trưng thật sự phân hoá giữa các quận — xem screen_features()
    features = screen_features(df, args.min_cv)

    # Khuyết dữ liệu điền bằng trung vị: KMeans không xử lý được NaN, và với
    # đặc trưng cấp quận (đã là trung bình của hàng trăm tin) thì trung vị là
    # ước lượng hợp lý, không bóp méo cấu trúc cụm.
    raw = df[features].to_numpy(dtype="float64")
    imputed = SimpleImputer(strategy="median").fit_transform(raw)
    # Chuẩn hóa là BẮT BUỘC: median_price_m2 cỡ hàng trăm còn pct_red_book nằm
    # trong [0,1]. Không chuẩn hóa thì khoảng cách Euclid chỉ còn là chênh lệch
    # giá, và bốn đặc trưng kia trở nên vô hình.
    X = StandardScaler().fit_transform(imputed)

    k_max = min(args.k_max, len(df) - 1)
    with mlflow.start_run(run_name="kmeans_district_segmentation"):
        if args.k:
            best_k, table = args.k, pd.DataFrame()
            print(f"Dùng k = {best_k} do người dùng chỉ định")
        else:
            best_k, table = choose_k(X, args.k_min, k_max)
            print(f"\n  → Chọn k = {best_k} (Silhouette cao nhất)")

        km = KMeans(n_clusters=best_k, random_state=SEED, n_init=10)
        df["cluster_id"] = km.fit_predict(X)
        df["cluster_label"] = label_clusters(df, best_k)

        # Sau khi sàng lọc có thể chỉ còn một đặc trưng — khi đó phân cụm thực
        # chất là chia khoảng trên một trục, và PCA 2 chiều không tồn tại.
        # Trải theo trục tung để biểu đồ vẫn đọc được, nhưng phải nói rõ bản
        # chất 1 chiều thay vì vẽ ra một mặt phẳng giả.
        n_comp = min(2, X.shape[1])
        pca = PCA(n_components=n_comp, random_state=SEED)
        coords = pca.fit_transform(X)
        df["pca_x"] = coords[:, 0]
        if n_comp == 2:
            df["pca_y"] = coords[:, 1]
        else:
            df["pca_y"] = df.groupby("cluster_id").cumcount() * 0.35
            print(f"\n  ⓘ  Chỉ còn 1 đặc trưng sau sàng lọc ({features[0]}).")
            print("      Phân cụm ở đây là chia khoảng trên MỘT trục, không phải")
            print("      phân nhóm đa chiều. Trục tung của biểu đồ chỉ để tách")
            print("      điểm cho dễ đọc, không mang ý nghĩa dữ liệu.")

        sil = float(silhouette_score(X, df["cluster_id"]))
        var = float(pca.explained_variance_ratio_.sum())

        # Thang diễn giải Silhouette (Kaufman & Rousseeuw): > 0.7 cấu trúc mạnh,
        # 0.5-0.7 hợp lý, 0.25-0.5 yếu, < 0.25 gần như không có cấu trúc thật.
        if sil < 0.25:
            print("")
            print(f"  ⚠️  Silhouette = {sil:.3f} < 0.25 — dữ liệu gần như KHÔNG có")
            print("      cấu trúc cụm rõ ràng. Phải nêu hạn chế này trong báo cáo")
            print("      thay vì trình bày kết quả như một phân khúc chắc chắn.")

        overlap = check_label_validity(df)

        mlflow.log_params({
            "k": best_k, "n_districts": len(df), "seed": SEED,
            "features_used": ",".join(features),
            "features_dropped": ",".join(f for f in CLUSTER_FEATURES if f not in features),
            "min_cv": args.min_cv,
        })
        mlflow.log_metrics({
            "silhouette": sil, "inertia": float(km.inertia_),
            "pca_explained_variance": var, "price_overlap_ratio": overlap,
            "n_features_used": len(features),
        })

        # ── Kết quả ───────────────────────────────────────────────────
        print(f"\nSilhouette = {sil:.4f}  ·  PCA {n_comp} chiều giữ được "
              f"{var:.1%} phương sai")
        print("\nPhân khúc thị trường:")
        summary = (df.groupby("cluster_label")
                     .agg(so_quan=("district", "count"),
                          gia_tv=("median_price_m2", "median"),
                          dt_tb=("mean_area", "mean"),
                          so_do=("pct_red_book", "mean"))
                     .sort_values("gia_tv", ascending=False))
        for label, r in summary.iterrows():
            names = ", ".join(df.loc[df["cluster_label"] == label, "district"].head(5))
            print(f"\n  {label}  —  {int(r['so_quan'])} quận · "
                  f"{r['gia_tv']:.0f} triệu/m² · {r['dt_tb']:.0f} m² · "
                  f"{r['so_do']:.0%} có sổ đỏ")
            print(f"      {names}"
                  + (" ..." if r["so_quan"] > 5 else ""))

        # ── Ghi serving layer ─────────────────────────────────────────
        out = df.rename(columns={"district_code": "area_code", "district": "area_name",
                                 "province": "province_name"})[[
            "area_code", "area_name", "province_name", "cluster_id", "cluster_label",
            "median_price_m2", "mean_area", "pct_red_book", "pct_mat_tien",
            "pca_x", "pca_y"]].copy()
        out["listing_density"] = df["n_listings"].to_numpy(dtype="float64")

        with pg_engine().begin() as conn:
            from sqlalchemy import text

            conn.execute(text("TRUNCATE TABLE gold_area_cluster"))
            out.to_sql("gold_area_cluster", conn, if_exists="append", index=False)
        print(f"\n  → PostgreSQL.gold_area_cluster: {len(out)} dòng")

        if not table.empty:
            plot_selection(table, best_k, FIGDIR / "cluster_selection.png")
            mlflow.log_artifact(str(FIGDIR / "cluster_selection.png"))
        plot_scatter(df, n_comp, FIGDIR / "cluster_scatter.png")
        mlflow.log_artifact(str(FIGDIR / "cluster_scatter.png"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
