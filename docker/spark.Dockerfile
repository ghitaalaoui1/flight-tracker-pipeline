FROM apache/spark:3.5.0-python3

USER root
WORKDIR /app

COPY docker/download_jars.sh .
RUN bash download_jars.sh

COPY src/ ./src/

ENV PYTHONUNBUFFERED=1

CMD ["/opt/spark/bin/spark-submit", \
     "--jars", "/opt/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/jars/kafka-clients-3.5.0.jar,/opt/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/jars/commons-pool2-2.11.1.jar", \
     "/app/src/streaming/spark_job.py"]