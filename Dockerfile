# Dockerfile - VacciGuard PyFlink runtime image
#
# This image stays self-contained for cloud deployment:
# - all Python dependencies are baked into the image
# - the Kinesis connector JAR is baked into the image
# - no host source-code mount is required in production
#
# Local Apple Silicon compatibility is handled in docker-compose.yml by
# pinning the runtime platform to linux/amd64 during local builds.

FROM eclipse-temurin:17-jdk-jammy

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY requirements.txt ./
RUN pip3 install -r requirements.txt

RUN mkdir -p /app/lib
COPY lib/flink-sql-connector-kinesis-4.3.0-1.18.jar /app/lib/
COPY . .

CMD ["python", "flink/pipeline.py"]
