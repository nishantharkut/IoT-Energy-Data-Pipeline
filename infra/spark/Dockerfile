FROM apache/spark:3.5.1-python3@sha256:4494ef2d6cb94e343402d67a0151ab39ce9b1a13810b82e5cbe55530c895aaf7 AS spark-distribution

FROM python:3.11.9-slim-bullseye@sha256:8983cb8c63f49ecf9313146b0ea09ab7e8ae19fe30712bd08f0d26c45db1fcb3

USER root

COPY --from=spark-distribution /opt/spark /opt/spark
COPY --from=spark-distribution /opt/java /opt/java

RUN apt-get update && \
    apt-get install -y --no-install-recommends bash ca-certificates curl procps tini && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/spark/sbin:/opt/java/openjdk/bin:$PATH

ARG MAVEN_CENTRAL=https://repo.maven.apache.org/maven2
RUN set -eux; \
    curl -fsSLo /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      "$MAVEN_CENTRAL/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar"; \
    echo "cb96af6c95c0beea714f73e5bc297f962bdd82fe492d8df94239c23e6a52bcf6  /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar" | sha256sum -c -; \
    curl -fsSLo /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      "$MAVEN_CENTRAL/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar"; \
    echo "b753422d64544703919abded8cbbc2ed76098da74e6b194d3407233eefc822a0  /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" | sha256sum -c -; \
    curl -fsSLo /opt/spark/jars/kafka-clients-3.4.1.jar \
      "$MAVEN_CENTRAL/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"; \
    echo "9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6  /opt/spark/jars/kafka-clients-3.4.1.jar" | sha256sum -c -; \
    curl -fsSLo /opt/spark/jars/commons-pool2-2.11.1.jar \
      "$MAVEN_CENTRAL/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"; \
    echo "ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83  /opt/spark/jars/commons-pool2-2.11.1.jar" | sha256sum -c -; \
    curl -fsSLo /opt/spark/jars/jsr305-3.0.0.jar \
      "$MAVEN_CENTRAL/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar"; \
    echo "bec0b24dcb23f9670172724826584802b80ae6cbdaba03bdebdef9327b962f6a  /opt/spark/jars/jsr305-3.0.0.jar" | sha256sum -c -

WORKDIR /opt/pipeline
COPY pyproject.toml README.md ./
COPY src ./src
COPY jobs ./jobs
COPY queries ./queries
COPY dashboard ./dashboard
COPY scripts/__init__.py scripts/scan_independence.py ./scripts/

RUN python3 -m pip install --no-cache-dir ".[dataset,kafka]"

ENV PYTHONPATH=/opt/pipeline/src:/opt/pipeline
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/bin/bash", "-lc", "trap : TERM INT; sleep infinity & wait"]
