"""Dựng file Word từ report/BaoCaoCuoiKy.md.

    pip install python-docx
    python scripts/build_report.py
    python scripts/build_report.py --figures      # chèn luôn biểu đồ ở Chương 5

Vì sao chuyển đổi bằng script thay vì soạn thẳng trong Word: nội dung báo cáo
chứa hàng chục con số lấy từ kết quả chạy thật. Mỗi lần huấn luyện lại là số
đổi. Giữ nguồn ở Markdown thì sửa một chỗ, dựng lại file Word bằng một lệnh —
soạn tay thì sớm muộn báo cáo và hệ thống sẽ nói hai chuyện khác nhau.

Bộ chuyển đổi này CỐ Ý tối giản: chỉ hỗ trợ đúng những cấu trúc mà báo cáo dùng
(tiêu đề, đoạn văn, bảng, danh sách, khối mã, ngắt trang, in đậm/nghiêng/mã
inline). Không phải trình dựng Markdown tổng quát — viết một cái như vậy là
công việc của thư viện khác, không phải của đồ án này.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

SRC = Path("report/BaoCaoCuoiKy.md")
OUT = Path("report/BaoCaoCuoiKy.docx")
FIGDIR = Path("report/figures")

# Biểu đồ chèn vào cuối Chương 5, theo đúng bảng danh mục trong báo cáo.
FIGURES = [
    ("model_comparison.png", "Hình 5.9. So sánh các mô hình dự đoán giá"),
    ("feature_importance.png", "Hình 5.10. Tầm quan trọng của đặc trưng"),
    ("cluster_selection.png", "Hình 5.11. Chọn số cụm — Elbow và Silhouette"),
    ("cluster_scatter.png", "Hình 5.12. Phân cụm khu vực trên không gian PCA"),
    ("anomaly_detection.png", "Hình 5.13. Phân bố điểm bất thường"),
    ("learning_curve.png", "Hình 5.14. Đường cong học — thêm dữ liệu còn giúp được bao nhiêu"),
    ("forecast.png", "Hình 5.15. Chuỗi giá và dự báo"),
]

INLINE = re.compile(r"(\*\*[^*]+\*\*|`[^`]+`|\*[^*]+\*)")


def add_runs(par, text: str) -> None:
    """Đổ text có **đậm**, *nghiêng*, `mã` vào một paragraph."""
    for part in INLINE.split(text):
        if not part:
            continue
        if part.startswith("**") and part.endswith("**"):
            par.add_run(part[2:-2]).bold = True
        elif part.startswith("`") and part.endswith("`"):
            r = par.add_run(part[1:-1])
            r.font.name = "Consolas"
            r.font.size = _pt(10)
        elif part.startswith("*") and part.endswith("*") and len(part) > 2:
            par.add_run(part[1:-1]).italic = True
        else:
            par.add_run(part)


def _pt(n):
    from docx.shared import Pt
    return Pt(n)


def split_row(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def is_separator(line: str) -> bool:
    return bool(re.fullmatch(r"\|[\s:|-]+\|", line.strip()))


def build(md: str, with_figures: bool):
    from docx import Document
    from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK
    from docx.shared import Cm, Pt

    doc = Document()

    style = doc.styles["Normal"]
    style.font.name = "Times New Roman"
    style.font.size = Pt(13)                 # cỡ chữ chuẩn của báo cáo UIT
    style.paragraph_format.space_after = Pt(6)
    style.paragraph_format.line_spacing = 1.3

    for s in doc.sections:
        s.left_margin, s.right_margin = Cm(3.0), Cm(2.0)
        s.top_margin, s.bottom_margin = Cm(2.0), Cm(2.0)

    lines = md.splitlines()
    i, in_code = 0, False
    code_buf: list[str] = []

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # ── Khối mã ───────────────────────────────────────────────
        if stripped.startswith("```"):
            if in_code:
                p = doc.add_paragraph()
                p.paragraph_format.left_indent = Cm(0.6)
                p.paragraph_format.space_after = Pt(10)
                r = p.add_run("\n".join(code_buf))
                r.font.name = "Consolas"
                r.font.size = Pt(9)
                code_buf, in_code = [], False
            else:
                in_code = True
            i += 1
            continue
        if in_code:
            code_buf.append(line)
            i += 1
            continue

        # ── Ngắt trang ────────────────────────────────────────────
        if stripped == "<!-- PAGEBREAK -->":
            doc.add_paragraph().add_run().add_break(WD_BREAK.PAGE)
            i += 1
            continue

        if not stripped or stripped == "---":
            i += 1
            continue

        # ── Bảng ──────────────────────────────────────────────────
        if stripped.startswith("|") and i + 1 < len(lines) and is_separator(lines[i + 1]):
            header = split_row(stripped)
            i += 2
            rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                rows.append(split_row(lines[i]))
                i += 1
            t = doc.add_table(rows=1, cols=len(header))
            t.style = "Table Grid"
            for c, txt in zip(t.rows[0].cells, header):
                c.paragraphs[0].clear()
                add_runs(c.paragraphs[0], txt)
                for r in c.paragraphs[0].runs:
                    r.bold = True
                    r.font.size = Pt(11)
            for row in rows:
                cells = t.add_row().cells
                # Dòng thiếu/thừa ô so với header: cắt theo số cột của bảng,
                # thà mất một ô còn hơn nổ giữa chừng và mất cả báo cáo.
                for c, txt in zip(cells, row + [""] * len(header)):
                    c.paragraphs[0].clear()
                    add_runs(c.paragraphs[0], txt)
                    for r in c.paragraphs[0].runs:
                        r.font.size = Pt(11)
            doc.add_paragraph()
            continue

        # ── Tiêu đề ───────────────────────────────────────────────
        m = re.match(r"^(#{1,4})\s+(.*)", stripped)
        if m:
            level, text = len(m.group(1)), m.group(2)
            h = doc.add_heading(level=min(level, 4))
            add_runs(h, text)
            for r in h.runs:
                r.font.name = "Times New Roman"
                r.font.color.rgb = None
                r.font.size = Pt({1: 16, 2: 14, 3: 13, 4: 13}[min(level, 4)])
            if level == 1:
                h.alignment = WD_ALIGN_PARAGRAPH.CENTER
            i += 1
            continue

        # ── Danh sách ─────────────────────────────────────────────
        m = re.match(r"^[-*]\s+(.*)", stripped)
        if m:
            add_runs(doc.add_paragraph(style="List Bullet"), m.group(1))
            i += 1
            continue
        m = re.match(r"^\d+\.\s+(.*)", stripped)
        if m:
            add_runs(doc.add_paragraph(style="List Number"), m.group(1))
            i += 1
            continue

        # ── Trích dẫn ─────────────────────────────────────────────
        if stripped.startswith(">"):
            p = doc.add_paragraph()
            p.paragraph_format.left_indent = Cm(1.0)
            add_runs(p, stripped.lstrip("> "))
            i += 1
            continue

        # ── Đoạn văn ──────────────────────────────────────────────
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        add_runs(p, stripped)
        i += 1

    if with_figures:
        doc.add_paragraph().add_run().add_break(WD_BREAK.PAGE)
        h = doc.add_heading(level=1)
        add_runs(h, "PHỤ LỤC — BIỂU ĐỒ KẾT QUẢ")
        h.alignment = WD_ALIGN_PARAGRAPH.CENTER
        n = 0
        for fname, caption in FIGURES:
            path = FIGDIR / fname
            if not path.exists():
                print(f"  bỏ qua (chưa có): {path}")
                continue
            doc.add_picture(str(path), width=Cm(15.5))
            doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
            cap = doc.add_paragraph()
            cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
            r = cap.add_run(caption)
            r.italic = True
            r.font.size = Pt(11)
            n += 1
        print(f"  chèn {n}/{len(FIGURES)} biểu đồ")

    return doc


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default=str(SRC))
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--figures", action="store_true", help="chèn biểu đồ vào phụ lục")
    args = ap.parse_args()

    src = Path(args.src)
    if not src.exists():
        print(f"Không tìm thấy {src}")
        return 1
    try:
        import docx  # noqa: F401
    except ImportError:
        print("Thiếu thư viện: pip install python-docx")
        return 1

    md = src.read_text(encoding="utf-8")
    print(f"Nguồn: {src}  ({len(md.splitlines()):,} dòng)")
    doc = build(md, args.figures)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    doc.save(out)
    print(f"  → {out}  ({out.stat().st_size / 1024:,.0f} KB)")
    print("\nCòn phải làm bằng tay trong Word (script không tự làm được):")
    print("  1. Chèn mục lục tự động: References → Table of Contents")
    print("  2. Đánh số trang: Insert → Page Number")
    print("  3. Chèn ảnh chụp màn hình theo bảng danh mục ở mục 5.7")
    print("  4. Điền tên GVHD và thành viên nhóm ở trang bìa")
    print("  5. Xuất PDF: File → Save as → PDF")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
