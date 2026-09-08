"""API suy luận mô hình — FastAPI.

    docker compose --profile apps up -d api-ml
    curl -X POST localhost:8000/predict -H 'Content-Type: application/json' \
      -d '{"area":80,"bedrooms":3,"bathrooms":2,"floors":3,
           "address":"Quận 7, TP.HCM","legal_status":"Sổ đỏ"}'

VÌ SAO TẦNG NÀY PHẢI LÀ PYTHON, không gộp vào Spring Boot: mô hình là XGBoost
sinh ra từ scikit-learn/MLflow. Nạp nó ở JVM đòi hoặc xuất PMML/ONNX (mất đúng
pipeline tiền xử lý), hoặc nhúng một trình thông dịch Python. Cả hai đều đắt
hơn nhiều so với một tiến trình FastAPI 200 dòng.

NGUYÊN TẮC MỘT NGUỒN ĐẶC TRƯNG: file này KHÔNG tự tính đặc trưng. Nó gọi
ml/features.py — đúng module đã dùng lúc huấn luyện. Viết lại logic đặc trưng ở
tầng serving là cách kinh điển để mô hình lệch âm thầm giữa train và production:
mọi thứ vẫn chạy, chỉ là dự đoán sai và không ai biết vì sao.
"""

from __future__ import annotations

import json
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

sys.path.insert(0, "/app/ml")
sys.path.insert(0, "/app/spark")

from features import (  # noqa: E402
    FEATURE_NAMES,
    build_features,
    inverse_target,
)
from udf_address import normalize_address  # noqa: E402

MODEL_URI = os.getenv("MODEL_URI", "models:/housing-price-model/latest")
ENCODING_PATH = Path(os.getenv("ENCODING_PATH", "/app/report/district_encoding.json"))

# Ngưỡng gắn cờ bất thường theo dư mô hình. Cùng cách chấm với ml/train_anomaly.py
# nhưng chạy trên MỘT tin, tức thời — bản batch chấm cả kho.
ANOMALY_RATIO = 0.50

STATE: dict[str, Any] = {}


# ══════════════════════════════════════════════════════════════════════
# Vòng đời — nạp mô hình một lần khi khởi động
# ══════════════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    STATE["model"] = None
    STATE["model_error"] = None
    try:
        import mlflow

        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
        STATE["model"] = mlflow.pyfunc.load_model(MODEL_URI)
        print(f"Đã nạp mô hình: {MODEL_URI}")
    except Exception as e:                                   # noqa: BLE001
        # KHÔNG để service chết: các endpoint đọc dữ liệu (heatmap, cụm, cảnh
        # báo) vẫn phục vụ được khi chưa huấn luyện xong. /predict sẽ trả 503
        # kèm lý do thật, thay vì container restart vô hạn.
        STATE["model_error"] = f"{type(e).__name__}: {e}"
        print(f"⚠️  Chưa nạp được mô hình ({STATE['model_error']}) — /predict tạm khoá.")

    if ENCODING_PATH.exists():
        enc = json.loads(ENCODING_PATH.read_text(encoding="utf-8"))
        STATE["lookup"] = enc["lookup"]
        STATE["global_median"] = enc["global_median"]
        print(f"Bảng mã hóa quận: {len(STATE['lookup'])} mã")
    else:
        STATE["lookup"], STATE["global_median"] = {}, None
        print(f"⚠️  Thiếu {ENCODING_PATH} — chạy ml/train_price.py trước.")

    yield


app = FastAPI(
    title="API mô hình giá bất động sản — IE221",
    description="Tầng suy luận cho hệ thống Big Data dự đoán giá nhà Việt Nam",
    version="1.0",
    lifespan=lifespan,
)
# Dashboard chạy ở cổng khác nên trình duyệt coi là cross-origin. Đồ án chạy
# cục bộ nên mở hết; production phải liệt kê tường minh.
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                   allow_headers=["*"])


# ══════════════════════════════════════════════════════════════════════
# Kết nối hạ tầng
# ══════════════════════════════════════════════════════════════════════
def pg():
    from sqlalchemy import create_engine

    if "engine" not in STATE:
        STATE["engine"] = create_engine(
            f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'reuser')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'repass')}@"
            f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
            f"{os.getenv('POSTGRES_PORT', '5432')}/"
            f"{os.getenv('POSTGRES_DB', 'realestate')}",
            pool_pre_ping=True)
    return STATE["engine"]


def rds():
    import redis

    if "redis" not in STATE:
        STATE["redis"] = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")), decode_responses=True)
    return STATE["redis"]


def query(sql: str, **params) -> list[dict]:
    try:
        return pd.read_sql(sql, pg(), params=params).to_dict("records")
    except Exception as e:                                   # noqa: BLE001
        raise HTTPException(503, f"PostgreSQL: {type(e).__name__}: {e}")


# ══════════════════════════════════════════════════════════════════════
# Hợp đồng dữ liệu
# ══════════════════════════════════════════════════════════════════════
class Listing(BaseModel):
    """Một tin rao cần định giá.

    Chỉ `area` là bắt buộc. Mọi trường khác để trống được — mô hình cây xử lý
    giá trị khuyết trực tiếp, và người dùng thật thường không biết hết thông số.
    """
    area: float = Field(..., gt=0, le=10_000, description="diện tích m²")
    address: str | None = Field(None, description='ví dụ "123 Nguyễn Thị Thập, Quận 7, TP.HCM"')
    province: str | None = None
    district: str | None = None
    ward: str | None = None
    bedrooms: float | None = None
    bathrooms: float | None = None
    floors: float | None = None
    frontage: float | None = Field(None, description="chiều ngang mặt tiền, m")
    access_road: float | None = Field(None, description="bề rộng đường vào, m")
    house_direction: str | None = None
    balcony_direction: str | None = None
    legal_status: str | None = None
    furniture_state: str | None = None
    price: float | None = Field(None, description="giá rao (tỷ) — chỉ cần cho /anomaly")


class GeoOut(BaseModel):
    province: str | None
    district: str | None
    ward: str | None
    district_code: str | None
    match_score: float
    source: str | None


class PredictOut(BaseModel):
    price_ty: float
    price_per_m2: float
    interval_ty: list[float]
    geo: GeoOut
    district_median_m2: float | None
    vs_district_pct: float | None
    latency_ms: float
    model_uri: str
    note: str


class AnomalyOut(BaseModel):
    is_anomaly: bool
    residual_ratio: float
    predicted_price_ty: float
    listed_price_ty: float
    severity: str
    reason: str
    geo: GeoOut


# ══════════════════════════════════════════════════════════════════════
# Suy luận
# ══════════════════════════════════════════════════════════════════════
def resolve_geo(x: Listing):
    return normalize_address(x.address, x.province, x.district, x.ward)


def to_frame(x: Listing, district_code: str | None) -> pd.DataFrame:
    return pd.DataFrame([{
        "district_code": district_code,
        "area": x.area, "floors": x.floors,
        "bedrooms": x.bedrooms, "bathrooms": x.bathrooms,
        "frontage": x.frontage, "access_road": x.access_road,
        "house_direction": x.house_direction,
        "balcony_direction": x.balcony_direction,
        "legal_status": x.legal_status, "furniture_state": x.furniture_state,
    }])


def predict_raw(x: Listing, district_code: str | None) -> tuple[float, float]:
    """→ (đơn giá triệu/m², tổng giá tỷ). Ném 503 khi chưa có mô hình."""
    model = STATE.get("model")
    if model is None:
        raise HTTPException(503, "Chưa nạp được mô hình: "
                                 f"{STATE.get('model_error')}. "
                                 "Chạy ml/train_price.py rồi khởi động lại api-ml.")
    if STATE["global_median"] is None:
        raise HTTPException(503, f"Thiếu {ENCODING_PATH} — chạy ml/train_price.py.")

    df = to_frame(x, district_code)
    X = build_features(df, district_lookup=STATE["lookup"],
                       global_median=STATE["global_median"])
    X = X[list(FEATURE_NAMES)]
    pred_log = np.asarray(model.predict(X), dtype=float).ravel()
    price_m2, price_ty = inverse_target(pred_log, np.array([x.area]))
    return float(price_m2[0]), float(price_ty[0])


def district_median(code: str | None) -> float | None:
    if not code:
        return None
    rows = query("SELECT median_price_m2 FROM gold_area_price "
                 "WHERE level = 'district' AND area_code = %(c)s", c=code)
    return float(rows[0]["median_price_m2"]) if rows else None


def geo_out(m) -> GeoOut:
    return GeoOut(province=m.province_name, district=m.district_name,
                  ward=m.ward_name, district_code=m.district_code,
                  match_score=m.score, source=m.geo_source)


@app.post("/predict", response_model=PredictOut, tags=["Bài toán 1 — Dự đoán giá"])
def predict(x: Listing):
    t0 = time.perf_counter()
    m = resolve_geo(x)
    price_m2, price_ty = predict_raw(x, m.district_code)
    dt = (time.perf_counter() - t0) * 1000

    med = district_median(m.district_code)

    # Khoảng tin cậy suy từ MAPE đo được trên tập kiểm tra (19,6%), KHÔNG phải
    # từ phân phối hậu nghiệm của mô hình — cây tăng cường không cho đại lượng
    # đó. Nói rõ như vậy để người đọc không hiểu nhầm đây là khoảng dự báo
    # theo nghĩa thống kê chặt chẽ.
    mape = float(os.getenv("MODEL_MAPE", "19.6")) / 100.0
    interval = [round(price_ty * (1 - mape), 3), round(price_ty * (1 + mape), 3)]

    note = "Giá RAO ước tính, không phải giá giao dịch."
    if not m.district_code:
        note += " Không nhận ra quận/huyện — dùng mặt bằng giá toàn quốc, sai số lớn hơn."
    elif m.district_code not in STATE["lookup"]:
        note += " Quận này không có trong tập huấn luyện — dùng giá trị mặc định."

    return PredictOut(
        price_ty=round(price_ty, 3),
        price_per_m2=round(price_m2, 2),
        interval_ty=interval,
        geo=geo_out(m),
        district_median_m2=round(med, 2) if med else None,
        vs_district_pct=round((price_m2 / med - 1) * 100, 1) if med else None,
        latency_ms=round(dt, 2),
        model_uri=MODEL_URI,
        note=note,
    )


@app.post("/anomaly", response_model=AnomalyOut, tags=["Bài toán 5 — Bất thường"])
def anomaly(x: Listing):
    """Chấm một tin theo DƯ của mô hình giá.

    Dùng lại mô hình bài toán 1 thay vì nuôi một mô hình riêng: nếu mô hình đã
    dự đoán tốt giá bình thường, thì mọi tin lệch xa dự đoán chính là định
    nghĩa của bất thường. Rẻ, và giải thích được cho người dùng cuối.
    """
    if x.price is None or x.price <= 0:
        raise HTTPException(400, "Cần trường 'price' (tỷ VND) để chấm bất thường.")
    m = resolve_geo(x)
    _, pred_ty = predict_raw(x, m.district_code)
    ratio = abs(x.price - pred_ty) / pred_ty

    sev = "cao" if ratio >= 1.0 else ("trung bình" if ratio >= ANOMALY_RATIO else "thấp")
    if x.price < pred_ty:
        reason = (f"Rẻ hơn ước tính {(1 - x.price / pred_ty) * 100:.0f}% — "
                  f"nghi giá mồi, pháp lý xấu, hoặc thông tin thiếu.")
    else:
        reason = (f"Đắt hơn ước tính {(x.price / pred_ty - 1) * 100:.0f}% — "
                  f"kiểm tra lại diện tích và đơn vị giá.")

    return AnomalyOut(
        is_anomaly=ratio >= ANOMALY_RATIO,
        residual_ratio=round(ratio, 4),
        predicted_price_ty=round(pred_ty, 3),
        listed_price_ty=x.price,
        severity=sev,
        reason=reason if ratio >= ANOMALY_RATIO else "Nằm trong khoảng bình thường.",
        geo=geo_out(m),
    )


# ══════════════════════════════════════════════════════════════════════
# Đọc tầng Gold — dữ liệu cho dashboard
# ══════════════════════════════════════════════════════════════════════
@app.get("/areas/price", tags=["Bài toán 2 — Heatmap"])
def areas_price(level: str = Query("district", pattern="^(province|district|ward)$"),
                parent: str | None = None):
    sql = ("SELECT level, area_code, area_name, parent_code, n_listings, "
           "median_price_m2, mean_price_m2, p25_price_m2, p75_price_m2, "
           "median_area, median_price FROM gold_area_price WHERE level = %(lv)s")
    if parent:
        sql += " AND parent_code = %(p)s"
    return query(sql + " ORDER BY median_price_m2 DESC NULLS LAST",
                 lv=level, p=parent)


@app.get("/clusters", tags=["Bài toán 4 — Phân cụm"])
def clusters():
    return query("SELECT * FROM gold_area_cluster ORDER BY cluster_id, "
                 "median_price_m2 DESC")


@app.get("/anomalies", tags=["Bài toán 5 — Bất thường"])
def anomalies(limit: int = Query(50, ge=1, le=500)):
    return query("SELECT * FROM gold_anomaly ORDER BY anomaly_score DESC "
                 "LIMIT %(n)s", n=limit)


@app.get("/forecast", tags=["Bài toán 3 — Dự báo"])
def forecast(area_code: str | None = None):
    if area_code is None:
        rows = query("SELECT area_code, area_name, COUNT(*) AS n "
                     "FROM gold_price_history GROUP BY 1, 2 ORDER BY n DESC LIMIT 1")
        if not rows:
            return {"history": [], "forecast": [], "metrics": [],
                    "note": "Chưa có chuỗi thời gian — chạy scripts/build_price_history.py."}
        area_code = rows[0]["area_code"]
    return {
        "area_code": area_code,
        "history": query("SELECT ds, price_m2, source FROM gold_price_history "
                         "WHERE area_code = %(a)s ORDER BY ds", a=area_code),
        "forecast": query("SELECT model_name, ds, yhat, yhat_lower, yhat_upper "
                          "FROM gold_price_forecast WHERE area_code = %(a)s "
                          "ORDER BY model_name, ds", a=area_code),
        "metrics": query("SELECT model_name, horizon_months, mape, rmse, mae, n_folds "
                         "FROM gold_forecast_metrics WHERE area_code = %(a)s "
                         "ORDER BY mape", a=area_code),
    }


@app.get("/summary", tags=["Tổng quan"])
def summary():
    return {r["metric_key"]: {"value": r["metric_value"], "text": r["metric_text"]}
            for r in query("SELECT metric_key, metric_value, metric_text "
                           "FROM gold_market_summary")}


@app.get("/quality", tags=["Tổng quan"])
def quality():
    """Chất lượng ETL qua từng lần chạy — bằng chứng cho Chương 3 báo cáo."""
    return query("SELECT run_at, stage, rows_in, rows_out, rows_dropped, "
                 "province_match_rate, district_match_rate, ward_match_rate, notes "
                 "FROM etl_data_quality ORDER BY run_id DESC LIMIT 20")


# ══════════════════════════════════════════════════════════════════════
# Hot path — đọc Redis
# ══════════════════════════════════════════════════════════════════════
@app.get("/hot/velocity", tags=["Hot path"])
def hot_velocity(limit: int = Query(20, ge=1, le=100)):
    try:
        r = rds()
        top = r.zrevrange("hot:top_districts", 0, limit - 1, withscores=True)
        out = []
        for member, score in top:
            code = member.split("|")[0]
            h = r.hgetall(f"hot:velocity:{code}")
            out.append({"district_code": code, "n_listings": int(score), **h})
        return {"districts": out, "meta": r.hgetall("hot:meta")}
    except Exception as e:                                   # noqa: BLE001
        raise HTTPException(503, f"Redis: {type(e).__name__}: {e}")


@app.get("/hot/alerts", tags=["Hot path"])
def hot_alerts(limit: int = Query(50, ge=1, le=200)):
    try:
        r = rds()
        raw = r.lrange("hot:alerts", 0, limit - 1)
        return {"total": int(r.get("hot:alerts:total") or 0),
                "alerts": [json.loads(s) for s in raw]}
    except Exception as e:                                   # noqa: BLE001
        raise HTTPException(503, f"Redis: {type(e).__name__}: {e}")


@app.get("/health", tags=["Tổng quan"])
def health():
    """Trạng thái từng phụ thuộc, không phải một chữ 'ok' vô nghĩa."""
    out = {"model": bool(STATE.get("model")), "model_uri": MODEL_URI,
           "model_error": STATE.get("model_error"),
           "encoding": bool(STATE.get("lookup"))}
    for name, probe in (("postgres", lambda: query("SELECT 1 AS x")),
                        ("redis", lambda: rds().ping())):
        try:
            probe()
            out[name] = True
        except Exception as e:                               # noqa: BLE001
            out[name] = f"{type(e).__name__}"
    out["status"] = "ok" if out["model"] and out["postgres"] is True else "degraded"
    return out
