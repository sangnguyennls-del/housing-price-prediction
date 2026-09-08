# Build hai giai đoạn: giai đoạn 1 có Maven + JDK (~700MB), giai đoạn 2 chỉ giữ
# JRE + file jar (~250MB). Ảnh cuối không chứa mã nguồn lẫn công cụ build.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src

# Copy pom trước và tải phụ thuộc thành một layer riêng: sửa code Java sau này
# không phải tải lại toàn bộ Maven repo.
COPY pom.xml .
RUN mvn -q -B dependency:go-offline

COPY src ./src
RUN mvn -q -B clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/*.jar app.jar

EXPOSE 8090
HEALTHCHECK --interval=20s --timeout=5s --start-period=60s --retries=5 \
    CMD curl -sf http://localhost:8090/actuator/health || exit 1

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-jar", "app.jar"]
