"""Kiểm tra và tải các dataset của đồ án.

    python scripts/download_data.py            # kiểm tra những gì đã có
    python scripts/download_data.py --download # tải qua Kaggle API

Kaggle yêu cầu xác thực nên script không tự tải được nếu chưa có credential.
Chạy chế độ kiểm tra để biết còn thiếu gì và làm thế nào để lấy.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

RAW = Path("data/raw")

# (slug Kaggle, token phải CÙNG xuất hiện trong tên file, vai trò, bắt buộc?)
DATASETS = [
    ("nguyentiennhan/vietnam-housing-dataset-2024",
     # KHÔNG đòi token "2024": file tải về từ Kaggle tên là
     # vietnam_housing_dataset.csv, không chứa năm. Đòi quá chặt thì báo
     # "thiếu dataset" trong khi nó đang nằm ngay trong data/raw.
     ("vietnam", "housing"),
     "Tập huấn luyện CHÍNH cho bài toán 1 (dự đoán giá). ~30k tin từ "
     "batdongsan.com.vn, có đủ pháp lý/hướng/mặt tiền.",
     True),
    ("ladcva/vietnam-housing-dataset-hanoi",
     ("hanoi",),
     "Bổ sung dữ liệu Hà Nội, tăng độ phủ địa bàn.",
     False),
    # ĐÃ KIỂM CHỨNG BẰNG CÁCH MỞ FILE, không suy từ tên. Dataset này mang tên
    # "Apartment prices in the city Ho Chi Minh City" nghe như chuỗi giá theo
    # thời gian, nhưng bên trong chỉ có đúng một file "chung cu chotot.csv":
    # 2.015 tin rao, 5 cột (#, title, area, price_VND, location), KHÔNG có cột
    # ngày. Nó là ảnh chụp một thời điểm, không dùng được cho bài toán 3.
    # Giữ lại trong danh sách với đúng token tên file để lần sau không ai
    # tải lại rồi tưởng đã đủ.
    ("hoandan/apartment-prices-in-the-city-ho-chi-minh-city",
     ("chungcu", "chotot"),
     "⚠️  TÊN GÂY HIỂU NHẦM — KHÔNG phải chuỗi thời gian. Bên trong là "
     "chung cu chotot.csv: 2.015 tin rao căn hộ TP.HCM, không có cột ngày. "
     "Không dùng được cho bài toán 3.",
     False),
]

# Chỗ trống thật sự của đồ án: chưa có nguồn chuỗi thời gian nào được kiểm
# chứng. Danh sách ứng viên để thử — CHƯA MỞ RA XEM nên chưa dám khẳng định.
TIMESERIES_CANDIDATES = [
    ("trnduythanhkhttt/housepricinghcm",
     "Mô tả nói có cột Date, giá trung bình/m² theo quận 1-9 TP.HCM. "
     "CHƯA kiểm chứng."),
    ("Chỉ số giá BĐS công bố công khai (globalpropertyguide, batdongsan)",
     "Nhập tay vào data/raw/bds_price_index.csv, có trích dẫn nguồn."),
    ("Tự tích lũy từ crawler",
     "posted_at của alonhadat — chạy nhiều phiên nhỏ theo lịch trong vài tuần."),
]


def kaggle_ready() -> tuple[bool, str]:
    """Kiểm tra credential Kaggle."""
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return True, "biến môi trường KAGGLE_USERNAME/KAGGLE_KEY"
    for p in (Path.home() / ".kaggle" / "kaggle.json",
              Path(os.getenv("KAGGLE_CONFIG_DIR", "")) / "kaggle.json"):
        if p.is_file():
            return True, str(p)
    return False, ""


def _norm(name: str) -> str:
    return name.replace("_", "").replace("-", "").replace(" ", "").lower()


def found_files(tokens: tuple[str, ...]) -> list[Path]:
    """File trong data/raw khớp TẤT CẢ token đặc trưng của dataset.

    Đòi hỏi khớp mọi token, không phải một token: khớp lỏng làm
    sample_vietnam_housing.csv bị nhận nhầm thành cả hai dataset Kaggle, và báo
    "đã có dữ liệu" trong khi thực tế chưa tải gì.
    """
    if not RAW.exists():
        return []
    out = []
    for p in RAW.rglob("*"):
        if not p.is_file() or p.suffix.lower() not in {".csv", ".xlsx", ".zip"}:
            continue
        if p.stem.lower().startswith("sample"):
            continue                      # dữ liệu giả lập, không phải Kaggle
        stem = _norm(p.stem)
        if all(t in stem for t in tokens):
            out.append(p)
    return out


def list_raw_files() -> list[Path]:
    if not RAW.exists():
        return []
    return sorted(p for p in RAW.rglob("*")
                  if p.is_file() and p.suffix.lower() in {".csv", ".xlsx"})


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="tải qua Kaggle API")
    args = ap.parse_args()

    RAW.mkdir(parents=True, exist_ok=True)
    ready, source = kaggle_ready()

    print("═" * 74)
    print("DATASET CỦA ĐỒ ÁN")
    print("═" * 74)

    missing = []
    for slug, tokens, role, required in DATASETS:
        hits = found_files(tokens)
        mark = "✓ CÓ  " if hits else ("✗ THIẾU" if required else "○ tuỳ chọn")
        print(f"\n{mark}  {slug}")
        print(f"        {role}")
        if hits:
            for h in hits:
                print(f"        → {h}  ({h.stat().st_size / 1024:,.0f} KB)")
        else:
            missing.append((slug, tokens, required))

    others = [p for p in list_raw_files()
              if not p.stem.lower().startswith("sample")
              and not any(p in found_files(tk) for _, tk, _, _ in DATASETS)]
    if others:
        print("")
        print("○ chưa nhận diện  — file trong data/raw không khớp dataset nào:")
        for p in others:
            print(f"        → {p}  ({p.stat().st_size / 1024:,.0f} KB)")
        print("        Nếu đây là dataset đã tải, đổi tên cho chứa token tương ứng,")
        print("        hoặc dùng thẳng: replay_producer.py --csv <đường dẫn> --survey-only")

    # Dữ liệu giả lập để kiểm thử pipeline
    sample = RAW / "sample_vietnam_housing.csv"
    print(f"\n{'✓ CÓ  ' if sample.exists() else '○ chưa'}  (dữ liệu giả lập kiểm thử)")
    print("        Sinh bằng: python scripts/make_sample_data.py")
    print("        ⚠️  Chỉ để kiểm thử pipeline — KHÔNG dùng cho kết quả báo cáo.")

    print("\n" + "─" * 74)
    print("BÀI TOÁN 3 — chưa có nguồn chuỗi thời gian nào được kiểm chứng.")
    print("Ứng viên để thử:")
    for slug, note in TIMESERIES_CANDIDATES:
        print(f"  · {slug}")
        print(f"    {note}")
    print("Kiểm tra nhanh một file bất kỳ có dùng được không:")
    print("  docker compose exec ml python scripts/build_price_history.py")

    if not missing:
        print("\n" + "═" * 74)
        print("Đủ dữ liệu. Bước tiếp theo:")
        print("  docker compose exec ml python ingestion/replay_producer.py \\")
        print("      --csv data/raw/<file>.csv --survey-only")
        print("  (khảo sát tên cột thật trước khi huấn luyện)")
        return 0

    print("\n" + "═" * 74)
    print(f"CÒN THIẾU {len(missing)} dataset")
    print("═" * 74)

    if not ready:
        print("""
Chưa có credential Kaggle. Cách lấy:

  1. Đăng nhập kaggle.com → Settings → API → "Create New Token"
  2. Lưu file kaggle.json tải về vào:  %USERPROFILE%\\.kaggle\\kaggle.json
  3. Chạy lại:  python scripts/download_data.py --download

Hoặc tải thủ công — mở từng link dưới đây, bấm Download, giải nén vào data/raw/:
""")
        for slug, _, req in missing:
            print(f"  {'[BẮT BUỘC]' if req else '[tuỳ chọn]'} "
                  f"https://www.kaggle.com/datasets/{slug}")
        return 1

    print(f"\nCredential Kaggle: {source}")
    if not args.download:
        print("Chạy lại với --download để tải.")
        return 1

    for slug, _, _ in missing:
        print(f"\nĐang tải {slug} ...")
        r = subprocess.run(
            [sys.executable, "-m", "kaggle", "datasets", "download",
             "-d", slug, "-p", str(RAW), "--unzip"],
            capture_output=True, text=True)
        if r.returncode == 0:
            print("  xong")
        else:
            print(f"  thất bại: {(r.stderr or r.stdout).strip()[:300]}")
            print(f"  tải thủ công: https://www.kaggle.com/datasets/{slug}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
