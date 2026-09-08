# Build context là thư mục gốc dự án: dashboard cần data/geo/*.geojson, mà file
# đó do scripts/fetch_geojson.py sinh ra ở ngoài thư mục dashboard/. Copy vào
# public/ lúc build thay vì bắt gateway phục vụ ~1MB file tĩnh không đổi.
FROM node:20-alpine AS build
WORKDIR /src

COPY dashboard/package.json ./
RUN npm install --no-audit --no-fund

COPY dashboard/ ./
COPY data/geo/provinces.geojson data/geo/districts.geojson ./public/geo/
RUN npm run build

# Runtime chỉ có nginx + file tĩnh — không mang theo Node lẫn công cụ build.
FROM nginx:1.27-alpine
COPY --from=build /src/dist /usr/share/nginx/html
COPY dashboard/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 5173
# Dùng 127.0.0.1 chứ KHÔNG phải localhost: trong container, localhost phân giải
# ra ::1 trước, mà nginx ở đây chỉ lắng nghe IPv4 → healthcheck báo "connection
# refused" trong khi dịch vụ hoàn toàn bình thường.
HEALTHCHECK --interval=20s --timeout=4s --start-period=10s --retries=5 \
    CMD wget -qO- http://127.0.0.1:5173/ >/dev/null || exit 1
