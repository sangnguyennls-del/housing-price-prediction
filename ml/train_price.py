"""Bài toán 1 — Dự đoán giá bất động sản. Khung thực nghiệm 4 giai đoạn.

    docker compose exec ml python ml/train_price.py --stage all
    docker compose exec ml python ml/train_price.py --stage 1 --trials 30

═══════════════════════════════════════════════════════════════════════════
KHUNG THỰC NGHIỆM — cấu trúc này chính là bố cục mục 4.2 của báo cáo.

  Giai đoạn 1  Sàng lọc cơ sở      5 thuật toán, tham số mặc định → mốc so sánh
  Giai đoạn 2  Naive Ensemble      trung bình cộng 3 mô hình boosting chưa tinh chỉnh
  Giai đoạn 3  Tinh chỉnh sâu      Optuna (tối ưu Bayesian) cho từng mô hình
  Giai đoạn 4  Advanced Ensemble   Stacking trên các mô hình đã tối ưu

Mục đích không phải để "chọn được mô hình tốt nhất" — mà để CHỨNG MINH bằng số
liệu rằng từng bước phức tạp hóa có đáng hay không. Rất có thể giai đoạn 4 chỉ
hơn giai đoạn 3 vài phần nghìn nhưng chậm gấp bốn; đó cũng là một kết quả, và
phải báo cáo đúng như vậy.

ĐÁNH GIÁ: mọi chỉ số đều quy về THANG GIÁ GỐC (tỷ VND), không phải thang log.
R² trên log đẹp hơn nhưng vô nghĩa với người đọc. MAPE là chỉ số chính vì nó
trả lời đúng câu người mua nhà hỏi: "máy đoán lệch bao nhiêu phần trăm?"
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import json
import os
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

import mlflow  # noqa: E402
import mlflow.sklearn  # noqa: E402
from sklearn.ensemble import RandomForestRegressor  # noqa: E402
from sklearn.impute import SimpleImputer  # noqa: E402
from sklearn.linear_model import LinearRegression, Ridge  # noqa: E402
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score  # noqa: E402
from sklearn.model_selection import GroupKFold, train_test_split  # noqa: E402
from sklearn.pipeline import Pipeline  # noqa: E402

import sys  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from features import (  # noqa: E402
    FEATURE_GROUPS,
    FEATURE_NAMES,
    build_features,
    fit_district_encoding,
    inverse_target,
    make_target,
)

EXPERIMENT = "price-prediction"
FIGDIR = Path("report/figures")
SEED = 42


# ── Nạp dữ liệu ───────────────────────────────────────────────────────────
def load_silver() -> pd.DataFrame:
    """Đọc tầng Silver từ PostgreSQL.

    VÌ SAO KHÔNG ĐỌC THẲNG HDFS — cần trả lời được câu này khi bảo vệ:
    Bảng silver_listings trong PostgreSQL do chính Spark ghi ra từ HDFS, nên
    lineage vẫn nguyên vẹn. Tập huấn luyện cỡ vài chục nghìn dòng nằm gọn trong
    RAM; kéo Parquet phân vùng qua WebHDFS chỉ tốn thêm thời gian mà không đổi
    kết quả. Spark được dùng đúng chỗ nó có giá trị — ETL và tổng hợp trên toàn
    bộ dữ liệu; nếu tập huấn luyện lên tới hàng trăm triệu dòng thì mới cần
    chuyển sang Spark MLlib.
    """
    from sqlalchemy import create_engine

    url = (f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'reuser')}:"
           f"{os.getenv('POSTGRES_PASSWORD', 'repass')}@"
           f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
           f"{os.getenv('POSTGRES_PORT', '5432')}/"
           f"{os.getenv('POSTGRES_DB', 'realestate')}")
    df = pd.read_sql("SELECT * FROM silver_listings", create_engine(url))
    print(f"Silver: {len(df):,} tin rao")
    return df


def prepare(df: pd.DataFrame, test_size: float = 0.2):
    """Làm sạch, tạo đặc trưng, chia tập."""
    df = df[df["price_per_m2"].notna() & (df["price_per_m2"] > 0)].copy()
    df = df[df["area"].notna() & (df["area"] > 0)]
    df = df.reset_index(drop=True)

    y = make_target(df)
    ok = y.notna() & np.isfinite(y)
    df, y = df[ok].reset_index(drop=True), y[ok].reset_index(drop=True)

    # Chia TRƯỚC khi mã hóa vị trí. Nếu mã hóa trên toàn bộ rồi mới chia, giá
    # của tập test đã lọt vào đặc trưng của tập train — rò rỉ kinh điển.
    idx_tr, idx_te = train_test_split(
        np.arange(len(df)), test_size=test_size, random_state=SEED, shuffle=True)

    df_tr, df_te = df.iloc[idx_tr].reset_index(drop=True), df.iloc[idx_te].reset_index(drop=True)
    y_tr, y_te = y.iloc[idx_tr].reset_index(drop=True), y.iloc[idx_te].reset_index(drop=True)

    oof, lookup, gmed = fit_district_encoding(df_tr, y_tr)
    X_tr = build_features(df_tr, district_encoding=oof)
    X_te = build_features(df_te, district_lookup=lookup, global_median=gmed)

    print(f"Train {len(X_tr):,} · Test {len(X_te):,} · {len(FEATURE_NAMES)} đặc trưng")
    return df_tr, df_te, X_tr, X_te, y_tr, y_te, lookup, gmed


# ── Đánh giá ──────────────────────────────────────────────────────────────
def evaluate(y_true_log: np.ndarray, y_pred_log: np.ndarray,
             area: np.ndarray) -> dict[str, float]:
    """Chỉ số trên THANG GIÁ GỐC, không phải thang log."""
    _, price_true = inverse_target(np.asarray(y_true_log), area)
    _, price_pred = inverse_target(np.asarray(y_pred_log), area)

    m2_true, _ = inverse_target(np.asarray(y_true_log), area)
    m2_pred, _ = inverse_target(np.asarray(y_pred_log), area)

    return {
        # Trên tổng giá (tỷ VND) — thứ người mua quan tâm
        "rmse_ty": float(np.sqrt(mean_squared_error(price_true, price_pred))),
        "mae_ty": float(mean_absolute_error(price_true, price_pred)),
        "mape_pct": float(np.mean(np.abs((price_true - price_pred) / price_true)) * 100),
        "r2_price": float(r2_score(price_true, price_pred)),
        # Trên đơn giá (triệu/m²) — thứ mô hình dự đoán trực tiếp
        "rmse_m2": float(np.sqrt(mean_squared_error(m2_true, m2_pred))),
        "mae_m2": float(mean_absolute_error(m2_true, m2_pred)),
        "r2_log": float(r2_score(y_true_log, y_pred_log)),
    }


def measure_latency(model, X: pd.DataFrame, n: int = 200) -> float:
    """Độ trễ suy luận (mili-giây/dự đoán) — dùng để chọn mô hình production."""
    sample = X.head(1)
    model.predict(sample)                              # làm nóng
    t0 = time.perf_counter()
    for _ in range(n):
        model.predict(sample)
    return (time.perf_counter() - t0) / n * 1000.0


def fmt(name: str, m: dict[str, float], extra: str = "") -> str:
    return (f"  {name:26} MAPE {m['mape_pct']:6.2f}%  "
            f"MAE {m['mae_ty']:6.2f} tỷ  RMSE {m['rmse_ty']:6.2f}  "
            f"R² {m['r2_price']:6.3f}  {extra}")


# ── Định nghĩa mô hình ────────────────────────────────────────────────────
def imputed(estimator):
    """Bọc imputer cho mô hình không tự xử lý giá trị khuyết.

    Các mô hình boosting đều xử lý NaN nội bộ và làm tốt hơn — chúng học được
    "khuyết dữ liệu" cũng là một tín hiệu (tin không ghi pháp lý thường là tin
    có vấn đề). Vì vậy CHỈ bọc cho hồi quy tuyến tính và rừng ngẫu nhiên.
    """
    return Pipeline([("impute", SimpleImputer(strategy="median")), ("model", estimator)])


def base_models() -> dict:
    from catboost import CatBoostRegressor
    from lightgbm import LGBMRegressor
    from xgboost import XGBRegressor

    return {
        "LinearRegression": imputed(LinearRegression()),
        "RandomForest": imputed(RandomForestRegressor(
            n_estimators=300, random_state=SEED, n_jobs=-1)),
        "XGBoost": XGBRegressor(random_state=SEED, n_jobs=-1, verbosity=0),
        "LightGBM": LGBMRegressor(random_state=SEED, n_jobs=-1, verbose=-1),
        "CatBoost": CatBoostRegressor(random_seed=SEED, verbose=0, allow_writing_files=False),
    }


# ── Giai đoạn 1 ───────────────────────────────────────────────────────────
def stage1_baseline(X_tr, y_tr, X_te, y_te, area_te) -> dict:
    print("\n" + "═" * 78)
    print("GIAI ĐOẠN 1 — Sàng lọc mô hình cơ sở (tham số mặc định)")
    print("═" * 78)

    results = {}
    for name, model in base_models().items():
        with mlflow.start_run(run_name=f"stage1_{name}", nested=True):
            t0 = time.perf_counter()
            model.fit(X_tr, y_tr)
            fit_s = time.perf_counter() - t0

            m = evaluate(y_te, model.predict(X_te), area_te)
            lat = measure_latency(model, X_te)

            mlflow.log_params({"stage": 1, "algorithm": name})
            mlflow.log_metrics({**m, "fit_seconds": fit_s, "latency_ms": lat})
            results[name] = {"metrics": m, "model": model, "latency_ms": lat,
                             "fit_seconds": fit_s}
            print(fmt(name, m, f"({fit_s:5.1f}s fit, {lat:.2f}ms/dự đoán)"))

    best = min(results, key=lambda k: results[k]["metrics"]["mape_pct"])
    print(f"\n  → Tốt nhất giai đoạn 1: {best} "
          f"(MAPE {results[best]['metrics']['mape_pct']:.2f}%)")
    return results


# ── Giai đoạn 2 ───────────────────────────────────────────────────────────
BOOSTERS = ("XGBoost", "LightGBM", "CatBoost")


def stage2_naive_ensemble(stage1: dict, X_te, y_te, area_te) -> dict:
    print("\n" + "═" * 78)
    print("GIAI ĐOẠN 2 — Naive Ensemble (trung bình cộng, chưa tinh chỉnh)")
    print("═" * 78)

    with mlflow.start_run(run_name="stage2_naive_ensemble", nested=True):
        preds = np.mean([stage1[n]["model"].predict(X_te) for n in BOOSTERS], axis=0)
        m = evaluate(y_te, preds, area_te)
        mlflow.log_params({"stage": 2, "members": ",".join(BOOSTERS)})
        mlflow.log_metrics(m)
        print(fmt("Naive Ensemble (3 mô hình)", m))

        best_single = min((stage1[n]["metrics"]["mape_pct"] for n in BOOSTERS))
        delta = best_single - m["mape_pct"]
        verdict = ("cải thiện" if delta > 0 else "KHÔNG cải thiện")
        print(f"\n  → So với mô hình đơn tốt nhất: {verdict} {abs(delta):.3f} điểm MAPE")
        mlflow.log_metric("mape_delta_vs_best_single", delta)
    return {"metrics": m, "predictions": preds}


# ── Giai đoạn 3 ───────────────────────────────────────────────────────────
def _optuna_space(trial, algo: str) -> dict:
    common = {
        "n_estimators": trial.suggest_int("n_estimators", 200, 1200, step=100),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
    }
    if algo == "XGBoost":
        return {**common,
                "max_depth": trial.suggest_int("max_depth", 3, 12),
                "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
                "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
                "reg_lambda": trial.suggest_float("reg_lambda", 1e-3, 10.0, log=True)}
    if algo == "LightGBM":
        return {**common,
                "num_leaves": trial.suggest_int("num_leaves", 15, 255),
                "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
                "feature_fraction": trial.suggest_float("feature_fraction", 0.5, 1.0),
                "bagging_fraction": trial.suggest_float("bagging_fraction", 0.5, 1.0),
                "reg_lambda": trial.suggest_float("reg_lambda", 1e-3, 10.0, log=True)}
    return {**common,
            "depth": trial.suggest_int("depth", 4, 10),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 30.0, log=True)}


def _make(algo: str, params: dict):
    from catboost import CatBoostRegressor
    from lightgbm import LGBMRegressor
    from xgboost import XGBRegressor

    if algo == "XGBoost":
        return XGBRegressor(**params, random_state=SEED, n_jobs=-1, verbosity=0)
    if algo == "LightGBM":
        return LGBMRegressor(**params, random_state=SEED, n_jobs=-1, verbose=-1)
    return CatBoostRegressor(**params, random_seed=SEED, verbose=0, allow_writing_files=False)


def stage3_optuna(X_tr, y_tr, X_te, y_te, area_te, n_trials: int) -> dict:
    import optuna
    from sklearn.model_selection import cross_val_score

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    print("\n" + "═" * 78)
    print(f"GIAI ĐOẠN 3 — Tinh chỉnh sâu bằng Optuna ({n_trials} lượt/mô hình)")
    print("═" * 78)

    tuned = {}
    for algo in BOOSTERS:
        def objective(trial):
            model = _make(algo, _optuna_space(trial, algo))
            # Chấm điểm bằng cross-validation TRÊN TẬP TRAIN. Dùng tập test để
            # chọn siêu tham số sẽ biến test thành tập validate và chỉ số cuối
            # sẽ lạc quan giả tạo.
            score = cross_val_score(model, X_tr, y_tr, cv=3,
                                    scoring="neg_root_mean_squared_error", n_jobs=1)
            return -score.mean()

        with mlflow.start_run(run_name=f"stage3_{algo}_optuna", nested=True):
            t0 = time.perf_counter()
            study = optuna.create_study(direction="minimize",
                                        sampler=optuna.samplers.TPESampler(seed=SEED))
            study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
            tune_s = time.perf_counter() - t0

            model = _make(algo, study.best_params)
            model.fit(X_tr, y_tr)
            m = evaluate(y_te, model.predict(X_te), area_te)
            lat = measure_latency(model, X_te)

            mlflow.log_params({"stage": 3, "algorithm": algo, "n_trials": n_trials,
                               **{f"best_{k}": v for k, v in study.best_params.items()}})
            mlflow.log_metrics({**m, "cv_rmse_log": study.best_value,
                                "tuning_seconds": tune_s, "latency_ms": lat})
            tuned[algo] = {"metrics": m, "model": model, "params": study.best_params,
                           "latency_ms": lat}
            print(fmt(f"{algo} (tuned)", m, f"({tune_s:5.1f}s tune, {lat:.2f}ms)"))
    return tuned


# ── Giai đoạn 4 ───────────────────────────────────────────────────────────
def stage4_stacking(tuned: dict, X_tr, y_tr, X_te, y_te, area_te) -> dict:
    from sklearn.ensemble import StackingRegressor

    print("\n" + "═" * 78)
    print("GIAI ĐOẠN 4 — Advanced Ensemble (Stacking trên mô hình đã tối ưu)")
    print("═" * 78)

    with mlflow.start_run(run_name="stage4_stacking", nested=True):
        # Meta-model là Ridge chứ không phải mô hình phi tuyến: đầu vào của nó
        # đã là dự đoán của các mô hình mạnh, việc còn lại chỉ là học trọng số
        # tổ hợp. Meta phức tạp ở đây gần như luôn dẫn tới overfit.
        stack = StackingRegressor(
            estimators=[(n.lower(), tuned[n]["model"]) for n in BOOSTERS],
            final_estimator=Ridge(alpha=1.0),
            cv=5, n_jobs=1,
        )
        t0 = time.perf_counter()
        stack.fit(X_tr, y_tr)
        fit_s = time.perf_counter() - t0

        m = evaluate(y_te, stack.predict(X_te), area_te)
        lat = measure_latency(stack, X_te)

        mlflow.log_params({"stage": 4, "meta_model": "Ridge", "cv_folds": 5})
        mlflow.log_metrics({**m, "fit_seconds": fit_s, "latency_ms": lat})
        print(fmt("Stacking Ensemble", m, f"({fit_s:5.1f}s fit, {lat:.2f}ms)"))

        best_tuned = min(tuned, key=lambda k: tuned[k]["metrics"]["mape_pct"])
        delta = tuned[best_tuned]["metrics"]["mape_pct"] - m["mape_pct"]
        slower = lat / tuned[best_tuned]["latency_ms"]
        print(f"\n  → So với {best_tuned} đã tinh chỉnh: "
              f"{'tốt hơn' if delta > 0 else 'KÉM hơn'} {abs(delta):.3f} điểm MAPE, "
              f"nhưng chậm hơn {slower:.1f} lần")
        mlflow.log_metrics({"mape_delta_vs_best_tuned": delta, "latency_ratio": slower})

    return {"metrics": m, "model": stack, "latency_ms": lat}


# ── Kiểm tra độ bền: giữ nguyên quận chưa từng thấy ───────────────────────
def district_holdout(df, X, y, model_factory, n_splits: int = 5) -> dict:
    """Đánh giá trên các quận KHÔNG xuất hiện trong tập huấn luyện.

    Chia ngẫu nhiên đo được năng lực trong điều kiện vận hành bình thường (dự
    đoán cho quận đã có dữ liệu). Phép chia theo nhóm quận này đo một thứ khác
    và khó hơn nhiều: mô hình còn lại gì khi mất đặc trưng vị trí — tức là khi
    gặp địa bàn hoàn toàn mới. Chênh lệch giữa hai con số cho biết mô hình phụ
    thuộc vào vị trí đến mức nào.
    """
    groups = df["district_code"].fillna("__na__").astype(str)
    if groups.nunique() < n_splits:
        return {}

    scores = []
    for tr, te in GroupKFold(n_splits=n_splits).split(X, y, groups):
        model = model_factory()
        model.fit(X.iloc[tr], y.iloc[tr])
        scores.append(evaluate(y.iloc[te], model.predict(X.iloc[te]),
                               df["area"].iloc[te].to_numpy())["mape_pct"])
    return {"holdout_mape_mean": float(np.mean(scores)),
            "holdout_mape_std": float(np.std(scores))}


# ── Biểu đồ cho báo cáo ───────────────────────────────────────────────────
def plot_comparison(rows: list[tuple[str, float, float]], path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = sorted(rows, key=lambda r: r[1], reverse=True)
    names = [r[0] for r in rows]
    mape = [r[1] for r in rows]
    lat = [r[2] for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.5), width_ratios=[1.6, 1])

    bars = ax1.barh(names, mape, color="#4C78A8")
    bars[int(np.argmin(mape))].set_color("#2E7D32")
    ax1.set_xlabel("MAPE (%) — thấp hơn là tốt hơn")
    ax1.set_title("Sai số phần trăm tuyệt đối trung bình", fontweight="bold")
    for b, v in zip(bars, mape):
        ax1.text(v + max(mape) * 0.01, b.get_y() + b.get_height() / 2,
                 f"{v:.2f}", va="center", fontsize=9)
    ax1.grid(axis="x", alpha=0.3)

    ax2.scatter(lat, mape, s=90, c="#E45756", zorder=3)
    for n, x, yv in zip(names, lat, mape):
        ax2.annotate(n, (x, yv), fontsize=7.5, xytext=(4, 4), textcoords="offset points")
    ax2.set_xscale("log")
    ax2.set_xlabel("Độ trễ suy luận (ms, thang log)")
    ax2.set_ylabel("MAPE (%)")
    ax2.set_title("Đánh đổi chính xác / tốc độ", fontweight="bold")
    ax2.grid(alpha=0.3)

    fig.suptitle("So sánh mô hình dự đoán giá bất động sản qua 4 giai đoạn",
                 fontsize=13, fontweight="bold")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\nBiểu đồ so sánh → {path}")


def plot_importance(model, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    imp = getattr(model, "feature_importances_", None)
    if imp is None:
        return
    s = pd.Series(imp, index=FEATURE_NAMES).sort_values()
    group_of = {f: g for g, fs in FEATURE_GROUPS.items() for f in fs}
    palette = dict(zip(FEATURE_GROUPS, plt.cm.tab10.colors))

    fig, ax = plt.subplots(figsize=(9, 7))
    ax.barh(s.index, s.values, color=[palette[group_of[f]] for f in s.index])
    ax.set_xlabel("Mức độ quan trọng")
    ax.set_title("Đóng góp của từng đặc trưng (tô màu theo nhóm)", fontweight="bold")
    ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=c) for c in palette.values()],
              labels=list(palette), fontsize=8, loc="lower right")
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Biểu đồ đặc trưng → {path}")


# ── Điều phối ─────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stage", default="all", choices=["1", "2", "3", "4", "all"])
    ap.add_argument("--trials", type=int, default=40, help="số lượt Optuna mỗi mô hình")
    ap.add_argument("--test-size", type=float, default=0.2)
    ap.add_argument("--no-mlflow", action="store_true")
    args = ap.parse_args()

    if args.no_mlflow:
        mlflow.set_tracking_uri(f"file://{Path('mlruns').resolve()}")
    else:
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment(EXPERIMENT)

    df = load_silver()
    df_tr, df_te, X_tr, X_te, y_tr, y_te, lookup, gmed = prepare(df, args.test_size)
    area_te = df_te["area"].to_numpy()

    with mlflow.start_run(run_name=f"price_experiment_stage{args.stage}"):
        mlflow.log_params({
            "n_total": len(df), "n_train": len(X_tr), "n_test": len(X_te),
            "n_features": len(FEATURE_NAMES), "target": "log(price_per_m2)",
            "test_size": args.test_size, "seed": SEED,
        })

        rows: list[tuple[str, float, float]] = []
        stage1 = tuned = stack = None

        if args.stage in ("1", "all"):
            stage1 = stage1_baseline(X_tr, y_tr, X_te, y_te, area_te)
            rows += [(n, r["metrics"]["mape_pct"], r["latency_ms"])
                     for n, r in stage1.items()]

        if args.stage in ("2", "all") and stage1:
            s2 = stage2_naive_ensemble(stage1, X_te, y_te, area_te)
            rows.append(("Naive Ensemble", s2["metrics"]["mape_pct"],
                         sum(stage1[n]["latency_ms"] for n in BOOSTERS)))

        if args.stage in ("3", "all"):
            tuned = stage3_optuna(X_tr, y_tr, X_te, y_te, area_te, args.trials)
            rows += [(f"{n} (tuned)", r["metrics"]["mape_pct"], r["latency_ms"])
                     for n, r in tuned.items()]

        if args.stage in ("4", "all") and tuned:
            stack = stage4_stacking(tuned, X_tr, y_tr, X_te, y_te, area_te)
            rows.append(("Stacking", stack["metrics"]["mape_pct"], stack["latency_ms"]))

        # ── Chọn mô hình production ───────────────────────────────────
        if tuned:
            print("\n" + "═" * 78)
            print("QUYẾT ĐỊNH CHO PRODUCTION")
            print("═" * 78)

            best_algo = min(tuned, key=lambda k: tuned[k]["metrics"]["mape_pct"])
            chosen, why = tuned[best_algo], f"{best_algo} (tinh chỉnh Optuna)"

            if stack:
                gain = tuned[best_algo]["metrics"]["mape_pct"] - stack["metrics"]["mape_pct"]
                ratio = stack["latency_ms"] / tuned[best_algo]["latency_ms"]
                # Quy tắc: chỉ nhận Stacking khi nó thật sự đáng. Dưới 0.5 điểm
                # MAPE mà chậm hơn 3 lần thì không đáng đánh đổi cho một API
                # phục vụ trực tiếp người dùng.
                if gain > 0.5 and ratio < 3.0:
                    chosen, why = stack, f"Stacking (hơn {gain:.2f} điểm MAPE)"
                else:
                    print(f"  Stacking hơn {gain:+.3f} điểm MAPE nhưng chậm hơn "
                          f"{ratio:.1f} lần → KHÔNG chọn")

            print(f"  → Chọn: {why}")
            m = chosen["metrics"]
            print(f"     MAPE {m['mape_pct']:.2f}%  ·  MAE {m['mae_ty']:.2f} tỷ  "
                  f"·  R² {m['r2_price']:.3f}  ·  {chosen['latency_ms']:.2f} ms/dự đoán")

            hold = district_holdout(df_tr, X_tr, y_tr,
                                    lambda: _make(best_algo, tuned[best_algo]["params"]))
            if hold:
                print(f"\n  Kiểm tra độ bền — quận chưa từng thấy khi huấn luyện:")
                print(f"     MAPE {hold['holdout_mape_mean']:.2f}% "
                      f"(±{hold['holdout_mape_std']:.2f})  so với "
                      f"{m['mape_pct']:.2f}% khi chia ngẫu nhiên")
                mlflow.log_metrics(hold)

            mlflow.log_metrics({f"production_{k}": v for k, v in m.items()})
            mlflow.log_param("production_model", why)

            # Bảng tra vị trí phải đi cùng mô hình — thiếu nó thì không tạo
            # được đặc trưng district_price_level lúc suy luận.
            art = Path("report/district_encoding.json")
            art.parent.mkdir(parents=True, exist_ok=True)
            art.write_text(json.dumps(
                {"lookup": lookup, "global_median": gmed,
                 "features": list(FEATURE_NAMES)}, ensure_ascii=False), encoding="utf-8")
            mlflow.log_artifact(str(art))

            # MLflow 2.x dùng `artifact_path`; tham số `name` chỉ có từ 3.x.
            mlflow.sklearn.log_model(
                chosen["model"], artifact_path="model",
                registered_model_name=None if args.no_mlflow else "housing-price-model")

        if rows:
            plot_comparison(rows, FIGDIR / "model_comparison.png")
            if tuned:
                plot_importance(tuned[best_algo]["model"], FIGDIR / "feature_importance.png")

    print("\nXem chi tiết tại MLflow UI: http://localhost:5000")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
