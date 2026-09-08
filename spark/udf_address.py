"""Chuẩn hóa địa chỉ bất động sản Việt Nam về mã đơn vị hành chính.

Đây là module quan trọng nhất của tầng ETL. Vị trí là đặc trưng có sức dự đoán
mạnh nhất trong bài toán giá nhà; nếu không quy được "Q.7" về cùng một mã với
"Quận 7" và "quan 7", thì cả heatmap lẫn mô hình đều hỏng.

Người đăng tin gõ địa chỉ tự do. Các biến thể thật đã gặp:
    "Quận 7"  "Q.7"  "Q7"  "quan 7"
    "Hồ Chí Minh"  "TP.HCM"  "TPHCM"  "Ho Chi Minh"  "Sài Gòn"  "HCM"
    "Bình Thạnh"  "binh thanh"  "Q. Bình Thạnh"

Chiến lược, theo thứ tự ưu tiên:
    1. Tách địa chỉ theo dấu phẩy. Ở Việt Nam thành phần hành chính nằm ở cuối,
       theo thứ tự phường → quận → tỉnh. Quét từ phải sang cho kết quả chắc
       chắn hơn nhiều so với dò cả chuỗi.
    2. Khớp chính xác trên dạng đã chuẩn hóa (bỏ dấu, bỏ tiền tố "quận/huyện").
    3. Khớp mờ (fuzzy) khi bước 2 thất bại, có ngưỡng tin cậy.

Dùng được ở hai nơi: import trực tiếp trong Spark job (đăng ký làm UDF), hoặc
gọi như thư viện Python thường trong container ML.

    python spark/udf_address.py        # chạy bộ kiểm tra
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

# rapidfuzz nhanh hơn nhiều nhưng chưa chắc có wheel cho mọi phiên bản Python;
# difflib trong stdlib đủ dùng và luôn có sẵn.
try:
    from rapidfuzz import fuzz

    def _ratio(a: str, b: str) -> float:
        return fuzz.ratio(a, b)

    FUZZ_BACKEND = "rapidfuzz"
except ImportError:                                   # pragma: no cover
    from difflib import SequenceMatcher

    def _ratio(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio() * 100.0

    FUZZ_BACKEND = "difflib"


# Spark ship file .py sang thư mục tạm trên executor, nên đường dẫn suy từ
# __file__ sẽ trỏ sai chỗ. Cho phép chỉ định tường minh qua biến môi trường
# (docker-compose đặt ADMIN_UNITS_PATH=/app/data/geo/admin_units.json).
ADMIN_UNITS_PATH = Path(
    os.getenv(
        "ADMIN_UNITS_PATH",
        str(Path(__file__).resolve().parent.parent / "data" / "geo" / "admin_units.json"),
    )
)

# Điểm khớp mờ tối thiểu để chấp nhận. 88 là mức cân bằng: bắt được lỗi gõ và
# thiếu dấu, nhưng không gán nhầm "Quận 1" thành "Quận 11".
FUZZY_THRESHOLD = 88.0

# ── Đơn vị hành chính đã giải thể trước mốc của bảng tham chiếu ───────────
# Bảng admin_units.json chỉ chứa các đơn vị ĐANG tồn tại. Tin rao thì lưu tên
# tại thời điểm đăng, nên dữ liệu cũ vẫn còn tên đơn vị đã bị xoá sổ.
#
# Đo trên dataset thật (30.229 tin): 712 tin — 2,36% — không khớp được chỉ vì
# lý do này, và chiếm 96% tổng số lỗi khớp cấp quận.
#
# Khóa: (mã tỉnh, tên lõi đơn vị cũ) → mã đơn vị kế thừa.
DISSOLVED_DISTRICTS: dict[tuple[str, str], str] = {
    # Nghị quyết 1111/NQ-UBTVQH14 (2021): Quận 2 + Quận 9 + quận Thủ Đức
    # hợp nhất thành thành phố Thủ Đức.
    ("79", "2"): "769",
    ("79", "9"): "769",
}
# HẠN CHẾ phải nêu trong báo cáo: gộp Quận 2 và Quận 9 vào Thủ Đức làm MẤT ĐỘ
# PHÂN GIẢI. Quận 2 cũ (Thảo Điền, An Phú) có mặt bằng giá cao hơn hẳn Quận 9
# cũ; sau khi gộp, trung vị của Thủ Đức là trung bình pha trộn của hai vùng giá
# rất khác nhau. Đây là hệ quả của thực tế hành chính, không phải lỗi xử lý —
# nhưng người đọc heatmap cần biết.


class GeoMatch(NamedTuple):
    province_code: str | None
    province_name: str | None
    district_code: str | None
    district_name: str | None
    ward_code: str | None
    ward_name: str | None
    score: float           # 0-100, độ tin cậy của cấp sâu nhất khớp được
    # Nguồn của mã quận: "old" khớp trực tiếp hệ cũ · "bridge2025" quy đổi từ
    # phường mới qua bảng sáp nhập · "unique" suy từ tên quận duy nhất toàn
    # quốc · None chưa khớp. Cần để báo cáo tách được kết quả theo nguồn.
    geo_source: str | None


EMPTY = GeoMatch(None, None, None, None, None, None, 0.0, None)


# ── Chuẩn hóa văn bản ─────────────────────────────────────────────────────
def strip_accents(s: str) -> str:
    s = s.replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


# Viết tắt phải mở rộng TRƯỚC khi bỏ tiền tố, nếu không "q.7" sẽ thành "q 7"
# và không nhận ra đó là quận.
_ABBREV = [
    (r"\btp\s*\.?\s*hcm\b", " ho chi minh "),
    (r"\btphcm\b", " ho chi minh "),
    (r"\bhcm\b", " ho chi minh "),
    (r"\bsai\s*gon\b", " ho chi minh "),
    (r"\bsg\b", " ho chi minh "),
    (r"\bha\s*noi\b", " ha noi "),
    (r"\bhn\b", " ha noi "),
    (r"\bt\s*\.?\s*p\b\s*\.?", " thanh pho "),
    (r"\btp\s*\.?", " thanh pho "),
    (r"\btt\s*\.?\s", " thi tran "),
    (r"\btx\s*\.?\s", " thi xa "),
    (r"\bq\s*\.?\s*(\d+)\b", r" quan \1 "),      # q.7 / q7 / q 7 → quan 7
    (r"\bq\s*\.\s*", " quan "),                   # q. Bình Thạnh → quan binh thanh
    (r"\bh\s*\.\s*", " huyen "),
    (r"\bp\s*\.\s*", " phuong "),
    (r"\bx\s*\.\s*", " xa "),
]

# Tiền tố loại đơn vị — bỏ đi để lấy "lõi" tên. "Quận Bình Thạnh" và
# "Bình Thạnh" phải quy về cùng một khóa.
_UNIT_PREFIX = re.compile(
    r"^(thanh pho|tinh|quan|huyen|thi xa|thi tran|phuong|xa)\s+"
)


def norm_text(s: str | None) -> str:
    """Bỏ dấu, thường hóa, mở rộng viết tắt, gộp khoảng trắng."""
    if not s:
        return ""
    t = strip_accents(str(s)).lower()
    t = re.sub(r"[^a-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    for pat, rep in _ABBREV:
        t = re.sub(pat, rep, t)
    return re.sub(r"\s+", " ", t).strip()


def core_name(s: str | None) -> str:
    """Tên lõi: đã chuẩn hóa và bỏ tiền tố loại đơn vị.

    "Thành phố Hồ Chí Minh" → "ho chi minh"
    "Quận 7"                → "7"
    "Huyện Nhà Bè"          → "nha be"
    """
    t = norm_text(s)
    prev = None
    while prev != t:                 # "thanh pho thu duc" cần bỏ tiền tố 1 lần
        prev = t
        t = _UNIT_PREFIX.sub("", t)
    return t.strip()


# ── Bảng tra cứu ──────────────────────────────────────────────────────────
class AdminIndex:
    """Chỉ mục tra cứu dựng từ data/geo/admin_units.json."""

    def __init__(self, provinces: list[dict]):
        self.provinces = provinces
        # core name → (code, official name)
        self.province_by_core: dict[str, tuple[str, str]] = {}
        # province_code → {core name → (code, name)}
        self.district_by_core: dict[str, dict[str, tuple[str, str]]] = {}
        # district_code → {core name → (code, name)}
        self.ward_by_core: dict[str, dict[str, tuple[str, str]]] = {}
        # core name → list[(province_code, district_code, name)] để suy ra tỉnh
        # khi tin rao chỉ ghi tên quận
        self.district_global: dict[str, list[tuple[str, str, str]]] = {}

        for p in provinces:
            pc, pname = p["code"], p["name"]
            self.province_by_core[core_name(pname)] = (pc, pname)
            self.district_by_core[pc] = {}
            for d in p["districts"]:
                dc, dname = d["code"], d["name"]
                dcore = core_name(dname)
                self.district_by_core[pc][dcore] = (dc, dname)
                self.district_global.setdefault(dcore, []).append((pc, dc, dname))
                self.ward_by_core[dc] = {}
                for w in d["wards"]:
                    self.ward_by_core[dc][core_name(w["name"])] = (w["code"], w["name"])

    # ── khớp một cấp ──────────────────────────────────────────────────
    @staticmethod
    def _match(table: dict[str, tuple[str, str]], text: str) -> tuple[str, str, float] | None:
        """Khớp `text` với một bảng {core → (code, name)}.

        Ba mức: khớp hệt → chứa nguyên từ → khớp mờ.
        """
        if not text or not table:
            return None

        core = core_name(text)
        if core in table:
            return (*table[core], 100.0)

        # Chứa nguyên cụm, tôn trọng ranh giới từ. Lấy khóa DÀI NHẤT để
        # "quan 1" không nuốt mất "quan 12" và ngược lại.
        norm = norm_text(text)
        best_key = None
        for key in table:
            if not key:
                continue
            if re.search(rf"(?<![a-z0-9]){re.escape(key)}(?![a-z0-9])", norm):
                if best_key is None or len(key) > len(best_key):
                    best_key = key
        if best_key:
            return (*table[best_key], 97.0)

        # Khớp mờ — chỉ dùng khi hai bước trên thất bại
        best, best_score = None, 0.0
        for key, val in table.items():
            score = _ratio(core, key)
            if score > best_score:
                best, best_score = val, score
        if best and best_score >= FUZZY_THRESHOLD:
            return (*best, best_score)
        return None


@lru_cache(maxsize=1)
def load_index(path: str | None = None) -> AdminIndex:
    p = Path(path) if path else ADMIN_UNITS_PATH
    if not p.exists():
        raise FileNotFoundError(
            f"Thiếu {p}. Chạy: python scripts/fetch_admin_units.py"
        )
    return AdminIndex(json.loads(p.read_text(encoding="utf-8")))


# ── Cầu nối hành chính 2025 ───────────────────────────────────────────────
# Từ 01/07/2025 Việt Nam bỏ cấp huyện. Tin rao mới ghi "Phường X, Hà Nội"
# (2 cấp) trong khi dataset Kaggle 2024 ghi "Quận Y, Hà Nội" (3 cấp). Bảng
# này quy phường MỚI về quận CŨ để hai nguồn cùng một không gian tham chiếu.
# Dựng bởi scripts/build_admin_bridge_2025.py.
BRIDGE_PATH = Path(
    os.getenv(
        "WARD2025_BRIDGE_PATH",
        str(Path(__file__).resolve().parent.parent / "data" / "geo" / "ward2025_to_old.json"),
    )
)


def key_name(s: str | None) -> str:
    """Tên → khóa 'thanhphohanoi' (quy ước khóa của bảng cầu nối)."""
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]", "", strip_accents(str(s)).lower())


@lru_cache(maxsize=1)
def load_bridge(path: str | None = None) -> dict[str, dict[str, dict]]:
    """Nạp bảng cầu nối. Thiếu file thì trả rỗng — hệ thống vẫn chạy, chỉ mất
    khả năng xử lý địa chỉ theo hệ hành chính mới."""
    p = Path(path) if path else BRIDGE_PATH
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _bridge_lookup(parts: list[str], province_name: str | None) -> dict | None:
    """Tra phường MỚI → quận CŨ.

    Biết tỉnh thì chỉ tra trong tỉnh đó. Chưa biết tỉnh thì quét toàn quốc và
    chỉ chấp nhận khi kết quả DUY NHẤT — tên phường trùng nhau giữa các tỉnh
    rất phổ biến, đoán bừa sẽ gán sai địa bàn.
    """
    bridge = load_bridge()
    if not bridge:
        return None

    provinces = ([bridge[k] for k in (key_name(province_name),) if k in bridge]
                 if province_name else list(bridge.values()))
    if province_name and not provinces:
        return None

    for part in parts:
        wk = key_name(part)
        if not wk:
            continue
        hits = [tbl[wk] for tbl in provinces if wk in tbl]
        if len(hits) == 1:
            return hits[0]
    return None


# ── Hàm chính ─────────────────────────────────────────────────────────────
def normalize_address(
    address_raw: str | None,
    province_hint: str | None = None,
    district_hint: str | None = None,
    ward_hint: str | None = None,
    index: AdminIndex | None = None,
) -> GeoMatch:
    """Địa chỉ tự do → mã hành chính.

    `*_hint` dành cho nguồn đã tách sẵn trường (Chợ Tốt trả region/area/ward);
    dùng hint trước vì nó đáng tin hơn việc dò trong chuỗi tự do.
    """
    idx = index or load_index()

    if not any((address_raw, province_hint, district_hint, ward_hint)):
        return EMPTY

    # Tách theo dấu phẩy, đảo ngược: thành phần hành chính nằm ở cuối địa chỉ
    parts = [p.strip() for p in re.split(r"[,;/]", address_raw or "") if p.strip()]
    tail = list(reversed(parts))

    # ── Cấp tỉnh ──────────────────────────────────────────────────────
    prov = None
    if province_hint:
        prov = idx._match(idx.province_by_core, province_hint)
    if not prov:
        for part in tail[:3]:                  # tỉnh gần như luôn ở 3 phần cuối
            prov = idx._match(idx.province_by_core, part)
            if prov:
                break
    if not prov and address_raw:
        prov = idx._match(idx.province_by_core, address_raw)

    # ── Cấp quận/huyện ────────────────────────────────────────────────
    # Ba tầng, xếp theo độ tin cậy giảm dần. Dừng ở tầng đầu tiên khớp được.
    dist = None
    geo_source = None
    dist_candidates = ([district_hint] if district_hint else []) + tail

    # Tầng 1 — khớp trực tiếp hệ CŨ. Dữ liệu Kaggle luôn dừng ở đây.
    if prov:
        table = idx.district_by_core[prov[0]]
        for part in dist_candidates:
            dist = idx._match(table, part)
            if dist:
                geo_source = "old"
                break

    # Tầng 1b — đơn vị đã giải thể: tên còn trong tin rao cũ nhưng không còn
    # trong bảng tham chiếu (Quận 2, Quận 9 → TP Thủ Đức từ 2021).
    if not dist and prov:
        for part in dist_candidates:
            code = DISSOLVED_DISTRICTS.get((prov[0], core_name(part)))
            if code:
                name = next((d["name"] for p in idx.provinces if p["code"] == prov[0]
                             for d in p["districts"] if d["code"] == code), None)
                if name:
                    dist = (code, name, 95.0)
                    geo_source = "dissolved"
                    break

    # Tầng 2 — cầu nối 2025: địa chỉ theo hệ MỚI không có cấp quận, quy phường
    # mới về quận cũ bằng bảng ánh xạ chính thức.
    if not dist:
        hit = _bridge_lookup(([ward_hint] if ward_hint else []) + tail,
                             prov[1] if prov else None)
        if hit:
            geo_source = "bridge2025"
            dist = (hit["district_code"], hit["district"], hit["confidence"] * 100.0)
            if not prov:
                prov = (hit["province_code"], hit["province"], hit["confidence"] * 100.0)

    # Tầng 3 — chưa biết tỉnh: chấp nhận tên quận DUY NHẤT toàn quốc rồi suy
    # ngược ra tỉnh. "Quận 1" xuất hiện ở nhiều nơi nên bị bỏ qua — thà thiếu
    # còn hơn gán sai.
    if not dist and not prov:
        for part in dist_candidates:
            hits = idx.district_global.get(core_name(part))
            if hits and len(hits) == 1:
                pc, dc, dname = hits[0]
                pname = next(p["name"] for p in idx.provinces if p["code"] == pc)
                prov, dist = (pc, pname, 90.0), (dc, dname, 90.0)
                geo_source = "unique"
                break

    # ── Cấp phường/xã ─────────────────────────────────────────────────
    ward = None
    if dist:
        table = idx.ward_by_core.get(dist[0], {})
        for part in ([ward_hint] if ward_hint else []) + tail:
            ward = idx._match(table, part)
            if ward:
                break

    deepest = ward or dist or prov
    return GeoMatch(
        province_code=prov[0] if prov else None,
        province_name=prov[1] if prov else None,
        district_code=dist[0] if dist else None,
        district_name=dist[1] if dist else None,
        ward_code=ward[0] if ward else None,
        ward_name=ward[1] if ward else None,
        score=round(deepest[2], 1) if deepest else 0.0,
        geo_source=geo_source,
    )


# ── Đăng ký UDF cho Spark ─────────────────────────────────────────────────
def register_udf(spark):
    """Trả về một UDF Spark nhận (address, province, district, ward) → struct.

    Bảng tham chiếu được broadcast một lần; mỗi executor tự nạp chỉ mục qua
    lru_cache nên không phải đọc lại file cho từng dòng.
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import FloatType, StringType, StructField, StructType

    schema = StructType([
        StructField("province_code", StringType()),
        StructField("province", StringType()),
        StructField("district_code", StringType()),
        StructField("district", StringType()),
        StructField("ward_code", StringType()),
        StructField("ward", StringType()),
        StructField("geo_match_score", FloatType()),
        StructField("geo_source", StringType()),
    ])

    def _fn(address, province, district, ward):
        try:
            m = normalize_address(address, province, district, ward)
        except Exception:                              # noqa: BLE001
            return (None, None, None, None, None, None, 0.0, None)
        return (m.province_code, m.province_name, m.district_code,
                m.district_name, m.ward_code, m.ward_name, float(m.score),
                m.geo_source)

    return udf(_fn, schema)


# ── Kiểm tra ──────────────────────────────────────────────────────────────
def demo() -> None:
    idx = load_index()
    print(f"Chỉ mục: {len(idx.provinces)} tỉnh · "
          f"{sum(len(p['districts']) for p in idx.provinces)} quận/huyện · "
          f"backend fuzzy = {FUZZ_BACKEND}")

    # Chuẩn hóa văn bản
    assert core_name("Thành phố Hồ Chí Minh") == "ho chi minh"
    assert core_name("Quận 7") == "7"
    assert core_name("Huyện Nhà Bè") == "nha be"
    assert norm_text("Q.7") == "quan 7"
    assert norm_text("TP.HCM") == "ho chi minh"
    assert norm_text("TPHCM") == "ho chi minh"

    # Mọi cách viết của cùng một quận phải ra cùng một mã
    hcm_q7 = [
        "123 Nguyễn Thị Thập, Phường Tân Phong, Quận 7, Hồ Chí Minh",
        "123 Nguyen Thi Thap, Q.7, TP.HCM",
        "Q7, TPHCM",
        "quan 7, tp hcm",
        "Quận 7, Sài Gòn",
    ]
    codes = set()
    for addr in hcm_q7:
        m = normalize_address(addr)
        assert m.district_code, f"không khớp: {addr}"
        codes.add((m.province_code, m.district_code))
    assert len(codes) == 1, f"cùng một quận nhưng ra nhiều mã: {codes}"

    # Không được nhầm Quận 1 với Quận 10/11/12 — đây là lỗi dễ mắc nhất
    seen = {}
    for n in (1, 10, 11, 12):
        m = normalize_address(f"Quận {n}, TP.HCM")
        assert m.district_name and m.district_name.endswith(str(n)), \
            f"Quận {n} → {m.district_name}"
        seen[n] = m.district_code
    assert len(set(seen.values())) == 4, f"mã trùng nhau: {seen}"

    # Khớp được tới cấp phường
    m = normalize_address("12 Đồng Khởi, Phường Bến Nghé, Quận 1, TP Hồ Chí Minh")
    assert m.ward_code and "Bến Nghé" in (m.ward_name or ""), m
    assert m.score >= 97.0, m

    # Quận có tên chữ, viết thường không dấu
    for addr, expect in [
        ("344 Phạm Văn Đồng, gò vấp, TP.HCM", "Gò Vấp"),
        ("Bình Thạnh, Hồ Chí Minh", "Bình Thạnh"),
        ("Q. Bình Thạnh, TPHCM", "Bình Thạnh"),
        ("Cầu Giấy, Hà Nội", "Cầu Giấy"),
        ("Hải Châu, Đà Nẵng", "Hải Châu"),
    ]:
        m = normalize_address(addr)
        assert m.district_name and expect in m.district_name, f"{addr} → {m.district_name}"

    # Đơn vị đã giải thể: Quận 2 / Quận 9 quy về TP Thủ Đức (sáp nhập 2021).
    # Không có bước này thì 2,36% dataset thật mất mã quận.
    for addr in ("Phường Thảo Điền, Quận 2, Hồ Chí Minh",
                 "Đường 990, Phường Phú Hữu, Quận 9, Hồ Chí Minh"):
        m = normalize_address(addr)
        assert m.district_code == "769", f"{addr} → {m.district_name} ({m.district_code})"
        assert m.geo_source == "dissolved", m.geo_source
    # Quận đang tồn tại KHÔNG được rơi vào bảng giải thể
    assert normalize_address("Quận 7, TP.HCM").geo_source == "old"

    # Suy ra tỉnh từ tên quận duy nhất toàn quốc
    m = normalize_address("Ngũ Hành Sơn")
    assert m.province_name and "Đà Nẵng" in m.province_name, m

    # Nguồn có sẵn trường tách riêng (Chợ Tốt)
    m = normalize_address(None, province_hint="Tp Hồ Chí Minh", district_hint="Quận 7")
    assert m.district_name == "Quận 7", m

    # ── Hệ hành chính MỚI (sau sáp nhập 01/07/2025) ──────────────────
    # Tin rao 2026 không còn cấp quận: "Đường X, Phường Y, Hà Nội".
    # Cầu nối phải quy được về quận cũ để thống nhất với dữ liệu Kaggle 2024.
    if load_bridge():
        new_system = [
            ("Đường Nguyễn Tuân , Phường Thanh Xuân , Hà Nội", "Thanh Xuân"),
            ("Phố Đoàn Thị Điểm , Phường Ô Chợ Dừa , Hà Nội", "Đống Đa"),
            ("Đường Ngọc Trục , Phường Đại Mỗ , Hà Nội", "Nam Từ Liêm"),
            ("Đường Nguyễn Chánh , Phường Yên Hòa , Hà Nội", "Cầu Giấy"),
            ("Đường Nguyễn Đình Tứ , Phường Nghĩa Đô , Hà Nội", "Cầu Giấy"),
            ("Đường Phú Diễn , Phường Phú Diễn , Hà Nội", "Bắc Từ Liêm"),
        ]
        for addr, expect in new_system:
            m = normalize_address(addr)
            assert m.district_name and expect in m.district_name,                 f"{addr} → {m.district_name}, mong {expect}"

        # Địa chỉ hệ CŨ phải đi qua tầng 1, không được rơi vào cầu nối
        m = normalize_address("Quận 7, TP.HCM")
        assert m.geo_source == "old", f"dữ liệu hệ cũ phải khớp trực tiếp, được {m.geo_source}"

        # "Phường Thanh Xuân" từng bị gán nhầm sang Sóc Sơn khi dùng cầu nối
        # ngây thơ theo tên phường cũ — kiểm tra hồi quy cho lỗi đó.
        m = normalize_address("Đường Nguyễn Tuân , Phường Thanh Xuân , Hà Nội")
        assert "Sóc Sơn" not in (m.district_name or ""), "hồi quy: lại gán nhầm Sóc Sơn"
    else:
        print("  (bỏ qua kiểm tra hệ 2025 — chạy scripts/build_admin_bridge_2025.py)")

    # Đầu vào rác không được làm vỡ, cũng không được đoán bừa
    for bad in (None, "", "   ", "abc xyz 123", ",,,"):
        m = normalize_address(bad)
        assert m.district_code is None, f"{bad!r} lại khớp ra {m.district_name}"

    print("udf_address.py — tất cả kiểm tra đạt")


if __name__ == "__main__":
    demo()
