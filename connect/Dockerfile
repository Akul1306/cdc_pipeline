FROM confluentinc/cp-kafka-connect:7.7.0 AS plugins

USER root

RUN mkdir -p /tmp/plugins

RUN confluent-hub install --no-prompt \
    --component-dir /tmp/plugins \
    confluentinc/kafka-connect-s3:12.1.14

# Stage 2: Debezium Connect with additional plugins and monitoring
FROM quay.io/debezium/connect:3.0

USER root

RUN mkdir -p \
    /kafka/connect/debezium-connector-oracle \
    /kafka/connect/confluentinc-kafka-connect-s3 \
    /kafka/etc

# Oracle JDBC driver
ADD --chmod=644 \
    https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.5.0.24.07/ojdbc11-23.5.0.24.07.jar \
    /kafka/connect/debezium-connector-oracle/

# Prometheus JMX Java agent
ADD --chmod=644 \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
    /kafka/etc/jmx_prometheus_javaagent.jar

COPY --chmod=644 jmx-config.yml /kafka/etc/jmx-config.yml

# Copy the S3 Sink plugin from the first stage
COPY --from=plugins \
    /tmp/plugins/confluentinc-kafka-connect-s3/ \
    /kafka/connect/confluentinc-kafka-connect-s3/

# Ensure Kafka Connect can read AND write the plugin folder (the startup script adds symlinks there)
RUN chown -R kafka:kafka /kafka/connect/confluentinc-kafka-connect-s3 \
    && chmod -R u+rwX,a+rX /kafka/connect/confluentinc-kafka-connect-s3 \
    && chmod 755 /kafka/etc

USER kafka
