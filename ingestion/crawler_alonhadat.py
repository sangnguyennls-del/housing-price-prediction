"""Thu thập tin rao bất động sản từ alonhadat.com.vn → Kafka.

    docker compose exec ml python ingestion/crawler_alonhadat.py --pages 3 --dry-run
    docker compose exec ml python ingestion/crawler_alonhadat.py --pages 20 --detail

═══════════════════════════════════════════════════════════════════════════
VÌ SAO CHỌN NGUỒN NÀY — phần này phải đưa vào mục đạo đức thu thập dữ liệu
(Chương 3 báo cáo), kèm bảng đối chiếu các nguồn đã khảo sát:

  batdongsan.com.vn  robots.txt trả HTTP 403, chặn cả bot đọc luật → loại
  nhatot.com         robots.txt: "Content-Signal: ai-train=no"
                     → chủ site từ chối cho dùng nội dung huấn luyện AI, loại
  alonhadat.com.vn   robots.txt cho phép /nha-dat/can-ban/*, không có tín hiệu
                     hạn chế AI, và trang dùng schema.org microdata — dữ liệu
                     được chủ site chủ động công bố cho máy đọc → CHỌN

Nguyên tắc tuân thủ áp dụng trong file này:
  1. Kiểm tra robots.txt BẰNG CHƯƠNG TRÌNH (urllib.robotparser) trước mỗi URL,
     không phải chỉ đọc bằng mắt một lần rồi tin.
  2. Tôn trọng Crawl-delay nếu site khai báo; nếu không, mặc định 20 giây
     (đo thực nghiệm: nhịp 5s bị site trả HTTP 429 chặn cả IP).
  3. User-Agent trung thực, nêu rõ mục đích học thuật và cách liên hệ.
  4. KHÔNG thu thập thông tin cá nhân người đăng: tên, số điện thoại, ảnh đại
     diện đều bị bỏ qua. Chỉ lấy thuộc tính bất động sản và giá.
  5. Mỗi trang danh sách cho ~20 tin, nên tải trang danh sách thay vì trang chi
     tiết giúp giảm ~20 lần số request cho cùng lượng dữ liệu.
  6. Lùi theo cấp số nhân khi gặp HTTP 429/503, và giãn vĩnh viễn nhịp gửi
     cho phần còn lại của phiên — site đã báo chậm lại thì phải chậm thật.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.robotparser
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schemas import (  # noqa: E402
    VND_PER_TY,
    blank_record,
    finalize,
    is_usable,
    make_listing_id,
    to_float,
    to_text,
)

BASE = "https://alonhadat.com.vn"
ROBOTS = f"{BASE}/robots.txt"
# Luồng tin mới nhất, phạm vi TOÀN QUỐC. Site không cung cấp bộ lọc tỉnh qua
# URL (đã thử /nha-dat/can-ban/nha-dat/{tinh}.html và biến thể — đều bị bỏ qua),
# nên không lọc ở tầng thu thập. Việc gán tỉnh/quận là của bộ chuẩn hóa địa chỉ
# ở tầng Silver, và với hot path thì luồng toàn quốc mới đúng thứ cần đo.
LIST_PATH = "/nha-dat/can-ban/nha-dat/{page}/ho-chi-minh.html"

# Email liên hệ lấy từ .env (không commit), KHÔNG hardcode: repo là công khai
# còn địa chỉ email thì bị bot thu thập. Nguyên tắc "User-Agent trung thực, nêu
# rõ cách liên hệ" ở mục đạo đức vẫn giữ nguyên — chỉ là giá trị thật nằm trong
# .env chứ không nằm trong mã nguồn.
DEFAULT_UA = os.getenv(
    "CRAWL_USER_AGENT",
    "UIT-IE221-Academic-Crawler/1.0 (do an hoc thuat; dat CRAWL_USER_AGENT trong .env)",
)
# 20 giây/request. Con số này rút ra từ thực nghiệm, không phải đoán: chạy ở
# nhịp 5s cho ~40 trang liên tiếp làm site trả HTTP 429 cho TOÀN BỘ địa chỉ IP
# trong nhiều phút, kể cả trang chủ và kể cả với User-Agent trình duyệt. Thu
# thập chậm mà đều tốt hơn nhiều so với bị chặn giữa buổi demo.
DEFAULT_DELAY = float(os.getenv("CRAWL_DELAY_SECONDS", "20"))
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "redpanda:9092")
DEFAULT_TOPIC = os.getenv("TOPIC_LISTINGS_RAW", "realestate.listings.raw")

# ── Bóc tách HTML ─────────────────────────────────────────────────────────
def _text(html_fragment: str) -> str:
    """Bỏ thẻ, giải mã entity, gộp khoảng trắng."""
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", html_fragment))).strip()


def _first_number(s: str | None) -> float | None:
    """Lấy số đầu tiên trong chuỗi.

    Cần thiết vì site có lỗi hiển thị: "Đường trước nhà" trả về "2mm" thay vì
    "2m". Bắt số đầu tiên là cách chịu lỗi mà không đoán bừa.
    """
    if not s:
        return None
    m = re.search(r"\d+(?:[.,]\d+)?", s)
    return float(m.group(0).replace(",", ".")) if m else None


# Các giá trị site dùng để biểu thị "không có thông tin"
_EMPTY_MARKERS = {"---", "--", "_", "", "n/a", "không", "khong"}


def _clean(value: str | None) -> str | None:
    v = to_text(value)
    return None if v is None or v.strip().lower() in _EMPTY_MARKERS else v


# ── Ngày đăng tin ─────────────────────────────────────────────────────────
# Trường quan trọng nhất mà trang danh sách cho không: kho lưu trữ của site
# trải nhiều tuần, nên MỘT phiên crawl đã dựng được chuỗi thời gian. Nếu chỉ
# có crawled_at thì mọi tin cùng một mốc và bài toán 3 không có dữ liệu.
_DATE_PATTERNS = (
    # microdata chuẩn: <time itemprop='datePosted' datetime='2025-08-14'>
    r"itemprop=['\"]datePosted['\"][^>]*datetime=['\"]([\d]{4}-[\d]{2}-[\d]{2})",
    # microdata chỉ có phần hiển thị: <span itemprop='datePosted'>14/08/2025</span>
    r"itemprop=['\"]datePosted['\"][^>]*>\s*(\d{1,2}/\d{1,2}/\d{4})",
    # dự phòng theo class khi site bỏ microdata
    r"class=['\"][^'\"]*(?:ct_date|post-date|date)[^'\"]*['\"][^>]*>\s*(\d{1,2}/\d{1,2}/\d{4})",
)


def parse_posted_date(block: str) -> str | None:
    """Ngày đăng → chuỗi ISO 'YYYY-MM-DD'. Không đoán khi không tìm thấy."""
    for pat in _DATE_PATTERNS:
        m = re.search(pat, block, re.S)
        if not m:
            continue
        raw = m.group(1)
        if "/" in raw:
            d, mo, y = raw.split("/")
            return f"{y}-{int(mo):02d}-{int(d):02d}"
        return raw
    return None


def parse_list_page(html: str) -> list[dict[str, Any]]:
    """Bóc các tin rao từ một trang danh sách.

    Dựa vào schema.org microdata thay vì class CSS: microdata là hợp đồng dữ
    liệu ổn định, còn class có thể đổi bất cứ lúc nào khi site thay giao diện.
    """
    # Mỗi tin bắt đầu bằng thẻ <a href=... itemprop='url'>
    anchors = list(re.finditer(r"href=['\"]([^'\"]+)['\"][^>]*itemprop=['\"]url['\"]", html))
    records = []

    for i, m in enumerate(anchors):
        start = m.end()
        end = anchors[i + 1].start() if i + 1 < len(anchors) else len(html)
        block = html[start:end]

        # Giá: microdata cho giá trị VND chính xác trong thuộc tính content,
        # đáng tin hơn nhiều so với chuỗi hiển thị "7 tỷ".
        pm = re.search(r"itemprop=['\"]price['\"] content=['\"](\d+)['\"]", block)
        am = re.search(
            r"itemprop=['\"]floorSize['\"].{0,300}?itemprop=['\"]value['\"]>([\d.,]+)</span>",
            block, re.S)
        if not pm or not am:
            continue

        rec = blank_record("alonhadat")
        rec["url"] = urljoin(BASE, m.group(1))
        rec["price"] = round(float(pm.group(1)) / VND_PER_TY, 6)
        rec["area"] = to_float(am.group(1))

        bm = re.search(
            r"numberOfBedrooms.{0,250}?itemprop=['\"]value['\"]>(\d+)</span>", block, re.S)
        rec["bedrooms"] = float(bm.group(1)) if bm else None

        fm = re.search(r"class=['\"]floors['\"]>([^<]*)<", block)
        rec["floors"] = _first_number(fm.group(1)) if fm else None

        sm = re.search(r"class=['\"]street-width['\"]>([^<]*)<", block)
        rec["access_road"] = _first_number(sm.group(1)) if sm else None

        rec["posted_at"] = parse_posted_date(block)

        dm = re.search(r"itemprop=['\"]address['\"][^>]*>(.{0,400}?)</p>", block, re.S)
        rec["address_raw"] = _clean(_text(dm.group(1))) if dm else None

        # ID lấy từ đuôi URL (…-18002176.html) — ổn định qua các lần crawl
        idm = re.search(r"-(\d{6,})\.html", rec["url"] or "")
        rec["listing_id"] = make_listing_id(
            "alonhadat", idm.group(1) if idm else rec["url"])

        records.append(finalize(rec))

    return records


# Nhãn trong bảng thông số trang chi tiết → trường chuẩn
_DETAIL_LABELS = {
    "hướng": "house_direction",
    "đường trước nhà": "access_road",
    "chiều ngang": "frontage",
    "số lầu": "floors",
    "số phòng ngủ": "bedrooms",
    "pháp lý": "legal_status",
    "loại bds": "property_type",
    "nội thất": "furniture_state",
    "số toilet": "bathrooms",
}
_NUMERIC_DETAIL = {"access_road", "frontage", "floors", "bedrooms", "bathrooms"}


def parse_detail_page(html: str) -> dict[str, Any]:
    """Bóc bảng thông số kỹ thuật từ trang chi tiết.

    Trang chi tiết có các trường mà trang danh sách không có: pháp lý, hướng,
    chiều ngang, và loại BDS (mặt tiền / trong hẻm) — yếu tố ảnh hưởng rất mạnh
    tới giá ở Việt Nam.
    """
    out: dict[str, Any] = {}
    pairs = re.findall(
        r"<td[^>]*>\s*([^<>]{2,24}?)\s*:?\s*</td>\s*<td[^>]*>\s*([^<>]{0,60}?)\s*</td>", html)
    for label, value in pairs:
        field = _DETAIL_LABELS.get(unescape(label).strip().lower())
        if not field:
            continue
        cleaned = _clean(unescape(value))
        if cleaned is None:
            continue
        out[field] = _first_number(cleaned) if field in _NUMERIC_DETAIL else cleaned
    return out


# ── Thu thập ──────────────────────────────────────────────────────────────
# Trần cho một lần nghỉ khi bị giới hạn tốc độ. Site đã báo chậm lại thì phải
# chậm thật, nhưng quá ngưỡng này thì nên dừng phiên và chạy lại sau theo lịch
# — ngủ lâu hơn không giúp gì, chỉ giấu mất việc IP đang bị chặn.
MAX_BACKOFF = 180.0


class Fetcher:
    """Tải trang, có kiểm tra robots.txt và giới hạn tốc độ."""

    def __init__(self, user_agent: str, delay: float):
        self.ua = user_agent
        self.delay = delay
        self._last = 0.0

        self.rp = urllib.robotparser.RobotFileParser()
        self.rp.set_url(ROBOTS)
        try:
            self.rp.read()
            crawl_delay = self.rp.crawl_delay(self.ua)
            if crawl_delay:
                # Site khai báo tốc độ riêng thì tôn trọng, và không bao giờ
                # chạy nhanh hơn mức mình tự đặt.
                self.delay = max(self.delay, float(crawl_delay))
                print(f"robots.txt khai báo Crawl-delay={crawl_delay}s → dùng {self.delay}s")
        except Exception as exc:                       # noqa: BLE001
            # Không đọc được robots.txt thì KHÔNG mặc định là được phép.
            raise SystemExit(f"Không đọc được {ROBOTS}: {exc}. Dừng để an toàn.") from exc

    def allowed(self, url: str) -> bool:
        return self.rp.can_fetch(self.ua, url)

    def get(self, url: str, max_retries: int = 4) -> str | None:
        """Tải một URL. Trả None nếu bị cấm, lỗi, hoặc hết lượt thử.

        Xử lý HTTP 429 bằng lùi theo cấp số nhân và TĂNG VĨNH VIỄN khoảng nghỉ
        cho các request sau. Site đã nói "chậm lại" thì phải chậm lại thật, chứ
        không phải chỉ nghỉ một lần rồi lại lao vào như cũ.
        """
        if not self.allowed(url):
            print(f"  robots.txt CẤM: {url} — bỏ qua")
            return None

        import requests

        for attempt in range(max_retries):
            wait = self.delay - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()

            try:
                r = requests.get(url, headers={"User-Agent": self.ua}, timeout=30)
            except Exception as exc:                   # noqa: BLE001
                print(f"  lỗi mạng ({attempt + 1}/{max_retries}): {exc}")
                time.sleep(2 ** attempt)
                continue

            if r.status_code == 200:
                return r.text

            if r.status_code in (429, 503):
                # Retry-After là chỉ dẫn của chủ site, ưu tiên hơn thuật toán
                # lùi của mình.
                retry_after = r.headers.get("Retry-After")
                # Lùi theo cấp số nhân nhưng CÓ TRẦN. Không có trần thì công
                # thức này tự nhân với self.delay đang lớn dần: sau vài lần
                # 429, delay chạm 60s và backoff thành 60 × 2⁵ = 1.920 giây —
                # gần nửa tiếng ngủ cho MỘT trang. Đo thực tế: một phiên 99
                # trang đứng im hơn một giờ mà không tiến được trang nào. Lịch
                # sự với site là đúng; treo vô thời hạn thì không, vì nó chỉ
                # biến job thành hộp đen chứ không giảm tải cho ai.
                backoff = (float(retry_after) if retry_after and retry_after.isdigit()
                           else min(self.delay * (2 ** attempt), MAX_BACKOFF))
                self.delay = min(self.delay * 1.5, 60.0)
                print(f"  HTTP {r.status_code} — nghỉ {backoff:.0f}s, "
                      f"giãn nhịp lên {self.delay:.1f}s ({attempt + 1}/{max_retries})")
                time.sleep(backoff)
                continue

            print(f"  HTTP {r.status_code}: {url}")
            return None

        print(f"  bỏ cuộc sau {max_retries} lần thử: {url}")
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pages", type=int, default=5,
                    help="số trang danh sách (mỗi trang ~20 tin). Nên chạy nhiều "
                         "phiên nhỏ theo lịch thay vì một phiên lớn — xem DEFAULT_DELAY.")
    ap.add_argument("--detail", action="store_true",
                    help="tải thêm trang chi tiết (chậm gấp ~20 lần, nhưng đủ trường)")
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    ap.add_argument("--dry-run", action="store_true", help="in ra thay vì gửi Kafka")
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    args = ap.parse_args()

    fetcher = Fetcher(DEFAULT_UA, args.delay)
    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all", retries=5, linger_ms=50,
        )

    total = sent = 0
    seen: set[str] = set()
    for page in range(1, args.pages + 1):
        url = urljoin(BASE, LIST_PATH.format(page=page))
        print(f"[trang {page}] {url}")
        html = fetcher.get(url)
        if not html:
            continue

        records = parse_list_page(html)
        total += len(records)
        # Trang mới có thể đẩy tin cũ lùi xuống, gây lặp giữa các trang.
        # Lọc ngay tại đây để không tốn request tải lại trang chi tiết.
        records = [r for r in records if r["listing_id"] not in seen]
        seen.update(r["listing_id"] for r in records)
        print(f"  bóc {len(records)} tin mới")

        for rec in records:
            if args.detail and rec.get("url"):
                detail_html = fetcher.get(rec["url"])
                if detail_html:
                    rec.update(parse_detail_page(detail_html))
                    finalize(rec)

            if not is_usable(rec):
                continue
            if producer:
                producer.send(args.topic, key=rec["listing_id"], value=rec)
            else:
                print("   ", {k: v for k, v in rec.items() if v is not None})
            sent += 1

    if producer:
        producer.flush()
        producer.close()

    print(f"\nBóc {total:,} tin · gửi {sent:,} bản ghi hợp lệ"
          + ("" if producer else " (dry-run, không gửi Kafka)"))
    return 0


# ── Kiểm tra ──────────────────────────────────────────────────────────────
# Fixture rút gọn từ HTML thật của alonhadat (đã lược phần không liên quan).
_FIXTURE_LIST = """
<a href='/nha-ban-quan-7-dep-18002176.html' itemprop='url'>Nhà đẹp</a>
<div class='property-details'>
  <div><span class='street-width'>2mm</span><span class='floors'>5 tầng</span>
  <span class='bedroom' itemprop='numberOfBedrooms' itemscope=''>
  <span itemprop='value'>5</span> phòng ngủ</span></div>
  <div><span class='price' itemprop='offers' itemscope=''>
    Giá: <span itemprop='price' content='7000000000'>7 tỷ </span></span>
  <span class='area' itemprop='floorSize' itemscope=''>
    Diện tích: <span itemprop='value'>28</span> <span itemprop='unitText'>m²</span></span></div>
</div>
<time itemprop='datePosted' datetime='2025-08-14'>14/08/2025</time>
<div class='property-address'><p class='new-address' itemprop='address'>
  Đường Nguyễn Thị Thập, Quận 7, TP.HCM</p></div>
<a href='/nha-ha-noi-19011830.html' itemprop='url'>Nhà HN</a>
<div class='property-details'>
  <div><span class='floors'>3 tầng</span></div>
  <div><span itemprop='offers'>Giá: <span itemprop='price' content='9900000000'>9.9 tỷ</span></span>
  <span itemprop='floorSize'>Diện tích: <span itemprop='value'>46</span></span></div>
</div>
<span class='ct_date'>02/09/2025</span>
<div><p itemprop='address'>Cầu Giấy, Hà Nội</p></div>
"""

_FIXTURE_DETAIL = """
<table><tr><td>Mã tin:</td><td>18002176</td></tr>
<tr><td>Hướng:</td><td>_</td></tr>
<tr><td>Đường trước nhà:</td><td>2mm</td></tr>
<tr><td>Loại BDS:</td><td>Nhà trong hẻm</td></tr>
<tr><td>Pháp lý:</td><td>Sổ hồng/ Sổ đỏ</td></tr>
<tr><td>Chiều ngang:</td><td>7m</td></tr>
<tr><td>Số lầu:</td><td>5</td></tr>
<tr><td>Chổ để xe hơi:</td><td>---</td></tr>
<tr><td>Số phòng ngủ:</td><td>5</td></tr></table>
"""


def demo() -> None:
    recs = parse_list_page(_FIXTURE_LIST)
    assert len(recs) == 2, f"mong 2 tin, được {len(recs)}"

    a = recs[0]
    assert a["price"] == 7.0, a["price"]                    # 7e9 VND → 7 tỷ
    assert a["area"] == 28.0
    assert a["bedrooms"] == 5.0
    assert a["floors"] == 5.0
    assert a["access_road"] == 2.0, f"'2mm' phải đọc ra 2.0, được {a['access_road']}"
    assert "Quận 7" in a["address_raw"]
    assert a["price_per_m2"] == 250.0                       # 7 tỷ / 28 m²
    assert a["listing_id"] == make_listing_id("alonhadat", "18002176")
    assert a["crawled_at"] is not None, "dữ liệu crawl phải có mốc thời gian"
    assert a["posted_at"] == "2025-08-14", a["posted_at"]
    assert is_usable(a)

    b = recs[1]
    assert b["price"] == 9.9 and b["area"] == 46.0
    assert b["bedrooms"] is None, "không có phòng ngủ thì phải là None, không đoán"
    # dd/mm/yyyy theo class dự phòng, khác dạng markup với tin đầu
    assert b["posted_at"] == "2025-09-02", b["posted_at"]

    # Không có ngày thì để None — tuyệt đối không lấy ngày crawl thay thế, vì
    # như vậy sẽ dồn cả kho tin vào một mốc và bịa ra chuỗi thời gian giả.
    assert parse_posted_date("<div>không có ngày</div>") is None

    d = parse_detail_page(_FIXTURE_DETAIL)
    assert d["legal_status"] == "Sổ hồng/ Sổ đỏ", d
    assert d["property_type"] == "Nhà trong hẻm", d
    assert d["frontage"] == 7.0, d
    assert d["access_road"] == 2.0, d
    assert d["floors"] == 5.0 and d["bedrooms"] == 5.0, d
    assert "house_direction" not in d, "'_' là ô trống, không được coi là hướng nhà"

    # Trang không có tin nào thì trả rỗng, không nổ
    assert parse_list_page("<html>trang loi</html>") == []

    print("crawler_alonhadat.py — tất cả kiểm tra đạt")


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        demo()
    else:
        raise SystemExit(main())
