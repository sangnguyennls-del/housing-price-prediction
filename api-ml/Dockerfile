# Tầng suy luận mô hình. Build context là thư mục gốc dự án vì service này
# dùng lại ml/features.py và spark/udf_address.py — KHÔNG nhân bản logic đặc
# trưng sang một bản sao riêng, đó là cách chắc chắn nhất để train và serving
# trôi lệch nhau.
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY api-ml/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ml/          /app/ml/
COPY spark/       /app/spark/
COPY api-ml/main.py /app/main.py
COPY data/geo/    /app/data/geo/
COPY report/district_encoding.json /app/report/district_encoding.json

# udf_address.py suy đường dẫn bảng tra từ __file__ (../data/geo). Trong ảnh
# này cây thư mục khác, nên chỉ tường minh bằng biến môi trường.
ENV ADMIN_UNITS_PATH=/app/data/geo/admin_units.json \
    WARD2025_BRIDGE_PATH=/app/data/geo/ward2025_to_old.json \
    PYTHONUNBUFFERED=1

EXPOSE 8000
HEALTHCHECK --interval=20s --timeout=5s --start-period=40s --retries=5 \
    CMD curl -sf http://localhost:8000/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
