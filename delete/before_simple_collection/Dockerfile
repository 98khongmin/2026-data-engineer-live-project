# Docker Official Image maintained by the Apache Spark developers.
FROM spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root
ARG POSTGRES_JDBC_VERSION=42.7.14
# Download at build time: no Maven resolution/network access needed at job time.
RUN python3 -c "import urllib.request; urllib.request.urlretrieve('https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar', '/opt/spark/jars/postgresql.jar')" \
    && chmod 644 /opt/spark/jars/postgresql.jar \
    && mkdir -p /opt/spark/app /opt/spark/data /tmp/spark-local \
    && chown -R spark:spark /tmp/spark-local

# Include code and configuration so a future Airflow DockerOperator can use
# the image directly. Compose read-only mounts override these during development.
COPY jobs/ /opt/spark/app/
COPY scripts/ /opt/spark/scripts/
COPY config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
WORKDIR /opt/spark/work-dir
