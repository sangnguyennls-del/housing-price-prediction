# Container huấn luyện ML. Python 3.11 vì host là 3.14 (chưa có wheel cho
# lightgbm/catboost/prophet).
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY docker/requirements-ml.txt /tmp/requirements-ml.txt
RUN pip install --no-cache-dir -r /tmp/requirements-ml.txt

# torch bản CPU: index riêng của PyTorch, wheel ~200MB thay vì ~2.5GB bản CUDA
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.5.1

# Client Kafka để chạy producer/crawler từ trong container (host là Python 3.14,
# chưa có wheel). Tách thành layer riêng ở cuối: sửa dòng này không phải build
# lại toàn bộ layer requirements phía trên.
RUN pip install --no-cache-dir kafka-python-ng==2.2.3 requests==2.32.3

CMD ["bash"]
