# Spark 3.5.9 + jar Kafka + driver PostgreSQL + thư viện Python cho ETL
FROM apache/spark:3.5.9-java17-python3

USER root

ARG SPARK_VERSION=3.5.9
ARG SCALA_VERSION=2.12
ARG MAVEN=https://repo1.maven.org/maven2

# Bake sẵn jar thay vì dùng --packages lúc chạy: spark-submit sẽ không phải
# gọi Maven mỗi lần khởi động, và demo không phụ thuộc vào mạng.
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# Driver JDBC để Spark ghi thẳng vào serving layer PostgreSQL
ADD ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar

COPY docker/requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

USER spark
