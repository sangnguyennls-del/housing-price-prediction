"""Tải ranh giới hành chính Việt Nam → GeoJSON cho heatmap (bài toán 2).

    python scripts/fetch_geojson.py
    python scripts/fetch_geojson.py --tol-province 0.02 --tol-district 0.008

Nguồn: https://github.com/daohoangson/dvhcvn — dữ liệu ranh giới 2 cấp
(tỉnh/thành và quận/huyện), trích từ hệ thống bản đồ hành chính nhà nước.

VÌ SAO CHỌN NGUỒN NÀY, chứ không phải GADM hay bộ dữ liệu GIS phổ biến khác:
mã đơn vị của nó là MÃ TỔNG CỤC THỐNG KÊ, đúng bộ mã mà data/geo/admin_units.json
và toàn bộ tầng Silver đang dùng. Nguồn khác thường đánh mã riêng (GID_1/GID_2
của GADM chẳng hạn) và phải ghép theo TÊN — mà ghép theo tên tiếng Việt có dấu
giữa hai bộ dữ liệu độc lập là đúng cái bẫy đã tốn của đồ án này một ngày ở
bước chuẩn hóa địa chỉ. Trùng mã sẵn thì bản đồ ghép được bằng một phép join.

HỆ QUY CHIẾU: 63 tỉnh / 705 quận huyện — hệ TRƯỚC sáp nhập 01/07/2025, khớp
với dữ liệu tin rao và với hệ quy chiếu đã chọn cho cả đồ án.

GIẢN LƯỢC ĐA GIÁC: dữ liệu gốc ~30MB, trình duyệt sẽ nghẹn khi vẽ. Dùng thuật
toán Douglas–Peucker tự cài (~20 dòng) thay vì kéo thêm shapely/geopandas —
đây là bài toán một chiều đơn giản, không đáng thêm một phụ thuộc nặng.
"""

from __future__ import annotations

import argparse
import json
import math
import time
import urllib.request
from pathlib import Path

GEO = Path("data/geo")
CACHE = GEO / "dvhcvn"
BASE = "https://raw.githubusercontent.com/daohoangson/dvhcvn/master/data/gis"
BBOX_URL = f"{BASE}/level1s_bbox.json"

UA = {"User-Agent": "UIT-IE221-Academic/1.0 (do an hoc thuat)"}

# Dung sai giản lược, đơn vị ĐỘ. 0.01° ≈ 1.1 km ở vĩ độ Việt Nam.
# Tỉnh vẽ ở mức thu nhỏ toàn quốc nên chịu được dung sai lớn; quận vẽ khi
# phóng to nên cần mịn hơn.
TOL_PROVINCE = 0.01
TOL_DISTRICT = 0.004


def fetch(url: str, timeout: int = 120, retries: int = 5) -> bytes:
    """Tải một file, lùi theo cấp số nhân khi bị giới hạn tốc độ.

    raw.githubusercontent trả HTTP 429 khi nhận 63 request liên tiếp. Không có
    bước lùi này thì script chết giữa chừng — và vì đã cache, lần chạy lại chỉ
    tải phần còn thiếu, nhưng vẫn phải chạy tay nhiều lần.
    """
    req = urllib.request.Request(url, headers=UA)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code not in (429, 403, 503) or attempt == retries - 1:
                raise
            wait = 5 * 2 ** attempt
            print(f"      HTTP {e.code} — chờ {wait}s rồi thử lại "
                  f"({attempt + 1}/{retries})", flush=True)
            time.sleep(wait)
    raise RuntimeError("không tới đây được")


# ── Giản lược đa giác ─────────────────────────────────────────────────────
def _perp_dist(p, a, b) -> float:
    """Khoảng cách từ p tới đoạn thẳng ab."""
    (px, py), (ax, ay), (bx, by) = p, a, b
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return math.hypot(px - ax, py - ay)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (ax + t * dx), py - (ay + t * dy))


def rdp(points: list, tol: float) -> list:
    """Douglas–Peucker, bản lặp (không đệ quy).

    Đệ quy sẽ vượt giới hạn ngăn xếp Python trên vòng ranh giới có hàng chục
    nghìn đỉnh — mà bờ biển Việt Nam thì đúng như vậy.
    """
    n = len(points)
    if n < 3:
        return points
    keep = [False] * n
    keep[0] = keep[-1] = True
    stack = [(0, n - 1)]
    while stack:
        i, j = stack.pop()
        if j - i < 2:
            continue
        dmax, idx = 0.0, i
        a, b = points[i], points[j]
        for k in range(i + 1, j):
            d = _perp_dist(points[k], a, b)
            if d > dmax:
                dmax, idx = d, k
        if dmax > tol:
            keep[idx] = True
            stack.append((i, idx))
            stack.append((idx, j))
    return [p for p, k in zip(points, keep) if k]


def simplify_ring(ring: list, tol: float) -> list | None:
    """Giản lược một vòng, giữ tính khép kín. Trả None nếu vòng bị teo hết."""
    pts = [tuple(p) for p in ring]
    out = rdp(pts, tol)
    if len(out) < 4:
        return None                       # đảo/vụn quá nhỏ ở mức thu nhỏ này
    if out[0] != out[-1]:
        out.append(out[0])
    return [list(p) for p in out]


def simplify_coords(coords: list, gtype: str, tol: float) -> tuple[list, str] | None:
    """Giản lược coordinates của Polygon hoặc MultiPolygon."""
    polys = coords if gtype == "MultiPolygon" else [coords]
    out = []
    for poly in polys:
        rings = [simplify_ring(r, tol) for r in poly]
        rings = [r for r in rings if r]
        if rings:
            out.append(rings)
    if not out:
        return None
    return (out, "MultiPolygon") if gtype == "MultiPolygon" else (out[0], "Polygon")


def count_points(coords, gtype: str) -> int:
    polys = coords if gtype == "MultiPolygon" else [coords]
    return sum(len(r) for poly in polys for r in poly)


# ── Tải và chuyển đổi ─────────────────────────────────────────────────────
def download_all(delay: float) -> list[dict]:
    CACHE.mkdir(parents=True, exist_ok=True)
    bbox_path = CACHE / "level1s_bbox.json"
    if not bbox_path.exists():
        bbox_path.write_bytes(fetch(BBOX_URL))
    codes = sorted(json.loads(bbox_path.read_text(encoding="utf-8")).keys())
    print(f"Nguồn có {len(codes)} tỉnh/thành")

    out = []
    for i, code in enumerate(codes, 1):
        path = CACHE / f"{code}.json"
        if not path.exists():
            print(f"  [{i:2}/{len(codes)}] tải {code}.json", flush=True)
            path.write_bytes(fetch(f"{BASE}/{code}.json"))
            time.sleep(delay)             # lịch sự với raw.githubusercontent
        out.append(json.loads(path.read_text(encoding="utf-8")))
    return out


def build(provinces_raw: list[dict], tol_p: float, tol_d: float
          ) -> tuple[dict, dict, dict]:
    prov_feats, dist_feats = [], []
    stats = {"pts_in": 0, "pts_out": 0, "n_dist": 0, "dropped": []}

    for p in provinces_raw:
        code, name, gtype = p["level1_id"], p["name"], p["type"]
        stats["pts_in"] += count_points(p["coordinates"], gtype)
        s = simplify_coords(p["coordinates"], gtype, tol_p)
        if s is None:
            stats["dropped"].append(name)
            continue
        coords, gt = s
        stats["pts_out"] += count_points(coords, gt)
        prov_feats.append({
            "type": "Feature",
            "properties": {"code": code, "name": name, "level": "province"},
            "geometry": {"type": gt, "coordinates": coords},
        })

        for d in p.get("level2s", []):
            dt = d["type"]
            s = simplify_coords(d["coordinates"], dt, tol_d)
            if s is None:
                stats["dropped"].append(f"{d['name']} ({name})")
                continue
            coords, gt = s
            stats["n_dist"] += 1
            dist_feats.append({
                "type": "Feature",
                "properties": {"code": d["level2_id"], "name": d["name"],
                               "province_code": code, "province": name,
                               "level": "district"},
                "geometry": {"type": gt, "coordinates": coords},
            })

    fc = lambda feats: {"type": "FeatureCollection", "features": feats}  # noqa: E731
    return fc(prov_feats), fc(dist_feats), stats


def check_codes(dist_fc: dict) -> None:
    """Đối chiếu mã quận của GeoJSON với bảng tham chiếu của hệ thống.

    Đây là bước kiểm tra QUAN TRỌNG NHẤT của script: nếu hai bộ mã lệch nhau,
    bản đồ sẽ hiện ra trắng trơn mà không có lỗi nào — kiểu hỏng khó chẩn đoán
    nhất. Thà biết ngay ở đây.
    """
    admin = GEO / "admin_units.json"
    if not admin.exists():
        print("  (bỏ qua đối chiếu: chưa có admin_units.json)")
        return
    units = json.loads(admin.read_text(encoding="utf-8"))
    ref = {d["code"] for p in units for d in p.get("districts", [])}
    geo = {f["properties"]["code"] for f in dist_fc["features"]}
    both = ref & geo
    print(f"\nĐối chiếu mã quận/huyện:")
    print(f"  bảng tham chiếu : {len(ref):>4}")
    print(f"  GeoJSON         : {len(geo):>4}")
    print(f"  khớp            : {len(both):>4}  ({len(both) / max(len(ref), 1):.1%})")
    if len(both) / max(len(ref), 1) < 0.95:
        print("  ⚠️  Dưới 95% — hai nguồn nhiều khả năng khác hệ quy chiếu.")
        print(f"     Có trong tham chiếu mà GeoJSON thiếu: {sorted(ref - geo)[:8]}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tol-province", type=float, default=TOL_PROVINCE)
    ap.add_argument("--tol-district", type=float, default=TOL_DISTRICT)
    ap.add_argument("--delay", type=float, default=1.5,
                    help="giãn cách giữa hai lần tải (giây)")
    args = ap.parse_args()

    GEO.mkdir(parents=True, exist_ok=True)
    raw = download_all(args.delay)

    print(f"\nGiản lược (tỉnh {args.tol_province}° · quận {args.tol_district}°)")
    prov, dist, st = build(raw, args.tol_province, args.tol_district)

    p_path, d_path = GEO / "provinces.geojson", GEO / "districts.geojson"
    p_path.write_text(json.dumps(prov, ensure_ascii=False, separators=(",", ":")),
                      encoding="utf-8")
    d_path.write_text(json.dumps(dist, ensure_ascii=False, separators=(",", ":")),
                      encoding="utf-8")

    print(f"  đỉnh cấp tỉnh: {st['pts_in']:,} → {st['pts_out']:,} "
          f"(giữ {st['pts_out'] / max(st['pts_in'], 1):.1%})")
    print(f"\n  → {p_path}  {len(prov['features'])} tỉnh · "
          f"{p_path.stat().st_size / 1024:,.0f} KB")
    print(f"  → {d_path}  {st['n_dist']} quận/huyện · "
          f"{d_path.stat().st_size / 1024:,.0f} KB")
    if st["dropped"]:
        print(f"  {len(st['dropped'])} đơn vị quá nhỏ, bị bỏ ở dung sai này: "
              f"{st['dropped'][:5]}")

    check_codes(dist)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
