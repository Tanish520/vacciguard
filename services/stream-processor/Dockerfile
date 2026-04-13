FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install Java (retry-safe)
RUN apt-get update --fix-missing \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && mkdir -p /opt/java \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java/openjdk \
    && mkdir -p /opt/spark/jars-extra \
    && rm -rf /var/lib/apt/lists/*

# Copy JARs (offline, no internet needed)
COPY jars/*.jar /opt/spark/jars-extra/

ENV JAVA_HOME=/opt/java/openjdk
ENV LOCAL_SPARK_JARS=/opt/spark/jars-extra/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars-extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars-extra/kafka-clients-3.4.1.jar,/opt/spark/jars-extra/commons-pool2-2.11.1.jar,/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY job.py .

CMD ["python", "job.py"]