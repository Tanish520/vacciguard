FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl default-jre-headless \
    && mkdir -p /opt/java \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java/openjdk \
    && mkdir -p /opt/spark/jars-extra \
    && curl -fsSL -o /opt/spark/jars-extra/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    && curl -fsSL -o /opt/spark/jars-extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    && curl -fsSL -o /opt/spark/jars-extra/kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSL -o /opt/spark/jars-extra/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && curl -fsSL -o /opt/spark/jars-extra/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o /opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java/openjdk
ENV LOCAL_SPARK_JARS=/opt/spark/jars-extra/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars-extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars-extra/kafka-clients-3.4.1.jar,/opt/spark/jars-extra/commons-pool2-2.11.1.jar,/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY job.py .

CMD ["python", "job.py"]
