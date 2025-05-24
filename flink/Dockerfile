FROM flink:1.20.1-scala_2.12-java11

USER root
WORKDIR /app

COPY requirements.txt .
COPY flink_job.py .

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk python3 python3-pip wget && \
    pip3 install -r requirements.txt

# Kafka connector
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.4.0-1.20/flink-connector-kafka-3.4.0-1.20.jar -P /opt/flink/lib

# Kafka clients
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar -P /opt/flink/lib

# JSON format
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.20.1/flink-json-1.20.1.jar -P /opt/flink/lib

# JDBC connector
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar -P /opt/flink/lib

# PostgreSQL driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P /opt/flink/lib

USER flink
